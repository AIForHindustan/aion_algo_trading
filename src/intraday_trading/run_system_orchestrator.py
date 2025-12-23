#!/usr/bin/env python3
"""
System Orchestrator
Manages the lifecycle of all trading components.
"""
import sys
import time
import logging
import threading
import subprocess
import os
import signal
from pathlib import Path

# ✅ Use centralized Redis client for M4-optimized pooling
from shared_core.redis_clients.redis_client import get_redis_client

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/orchestrator.log")
    ]
)
logger = logging.getLogger("SystemOrchestrator")

class SystemOrchestrator:
    def __init__(self):
        self.processes = {}
        self.running = False
        # ✅ DB 0: For system/config operations
        self.redis = get_redis_client(process_name="system_orchestrator", db=0)
        # ✅ DB 1: For stream verification (streams are in DB 1)
        self.redis_db1 = get_redis_client(process_name="system_orchestrator", db=1)
        # Handle signals
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("🛑 Received termination signal")
        self.stop()
        sys.exit(0)

    def start_component(self, name, module_path, function_name=None, restart_policy="always", **kwargs):
        """Start a component as a subprocess"""
        logger.info(f"🚀 Launching {name} ({module_path})...")
        
        try:
            # Construct command: python -m module_path
            cmd = [sys.executable, "-m", module_path]
            
            # Create logs for this component
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            stdout_log = open(log_dir / f"{name}.log", "a")
            stderr_log = open(log_dir / f"{name}.err", "a")
            
            # Setup environment with PYTHONPATH
            env = os.environ.copy()
            cwd = os.getcwd()
            src_path = os.path.join(cwd, "src")
            
            # Add src to PYTHONPATH
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}"
            else:
                env["PYTHONPATH"] = src_path
                
            process = subprocess.Popen(
                cmd,
                stdout=stdout_log,
                stderr=stderr_log,
                cwd=cwd,
                env=env
            )
            
            self.processes[name] = {
                "process": process,
                "module_path": module_path,
                "start_time": time.time(),
                "restart_policy": restart_policy
            }
            logger.info(f"✅ {name} started with PID {process.pid}")
            
        except Exception as e:
            logger.error(f"❌ Failed to start {name}: {e}")

    def monitor_health(self):
        """Monitor subprocess health"""
        while self.running:
            for name, info in list(self.processes.items()):
                proc = info["process"]
                ret = proc.poll()
                
                if ret is not None:
                    logger.warning(f"⚠️ Component {name} matches dead (Exit: {ret})")
                    if info["restart_policy"] == "always":
                        logger.info(f"🔄 Restarting {name}...")
                        self.start_component(name, info["module_path"], restart_policy="always")
                    else:
                        logger.info(f"ℹ️ {name} finished (no restart)")
                        del self.processes[name]
            
            time.sleep(5)

    def start(self):
        """Start all components in the correct order"""
        logger.info("🚀 STARTING COMPLETE TRADING PIPELINE")
        logger.info("⏳ Starting components in sequence...")
        
        # 1. START DATA PIPELINE FIRST - CRITICAL!
        logger.info("🔧 Starting Data Pipeline Supervisor...")
        self.start_component(
            name="data_pipeline_supervisor",
            module_path="shared_core.redis_clients.supervise_pipeline_workers",
            function_name="main"
        )
        
        # Wait longer for pipeline to start
        logger.info("⏳ Waiting 5 seconds for data pipeline to initialize...")
        time.sleep(5)
        
        # 2. Verify pipeline is working
        self._verify_data_pipeline()
        
        # 3. Start Scanner Main
        logger.info("🔍 Starting Scanner Main...")
        self.start_component(
            name="scanner_main",
            module_path="intraday_trading.intraday_scanner.scanner_main",
            function_name="main"
        )
        
        # 4. Start Robot Alert Consumer
        logger.info("🤖 Starting Robot Alert Consumer...")
        self.start_component(
            name="robot_alert_consumer",
            module_path="aion_trading_dashboard.backend.robot_alert_consumer",
            function_name="main"
        )
        
        self.running = True
        logger.info("✅ ALL COMPONENTS STARTED")
        
        # Immediately check status
        self._check_data_flow_now()
        
        # Start health monitor thread
        monitor_thread = threading.Thread(target=self.monitor_health, daemon=True)
        monitor_thread.start()
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop all components"""
        self.running = False
        logger.info("🛑 Stopping All Components...")
        for name, info in self.processes.items():
            logger.info(f"  Killing {name}...")
            info["process"].terminate()
            try:
                info["process"].wait(timeout=3)
            except subprocess.TimeoutExpired:
                info["process"].kill()

    def _verify_data_pipeline(self):
        """Verify data pipeline is actually working"""
        try:
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            
            # Check if enriched stream is being populated
            enriched_stream = DatabaseAwareKeyBuilder.live_enriched_stream()
            enriched_len = self.redis_db1.xlen(enriched_stream)
            
            # Check if pipeline workers are running
            raw_stream = "ticks:intraday:processed"
            groups = self.redis_db1.xinfo_groups(raw_stream)
            
            has_pipeline_group = any(g['name'] == 'data_pipeline_group' for g in groups)
            
            logger.info("🔍 DATA PIPELINE VERIFICATION:")
            logger.info(f"  Raw stream messages: {self.redis_db1.xlen(raw_stream)}")
            logger.info(f"  Enriched stream messages: {enriched_len}")
            logger.info(f"  Pipeline group exists: {has_pipeline_group}")
            
            if enriched_len == 0:
                logger.warning("⚠️  ENRICHED STREAM IS EMPTY - Data pipeline may not be running!")
                logger.warning("   Check logs/data_pipeline_supervisor.log for errors")
                
        except Exception as e:
            logger.error(f"❌ Pipeline verification failed: {e}")

    def _check_data_flow_now(self):
        """Immediate data flow check"""
        def check_flow():
            time.sleep(5)  # Wait 5 seconds
            try:
                initial_raw = self.redis_db1.xlen("ticks:intraday:processed")
                initial_enriched = self.redis_db1.xlen("ticks:intraday:enriched")
                
                time.sleep(5)  # Wait another 5 seconds
                
                final_raw = self.redis_db1.xlen("ticks:intraday:processed")
                final_enriched = self.redis_db1.xlen("ticks:intraday:enriched")
                
                logger.info("📊 IMMEDIATE FLOW CHECK (10 seconds):")
                logger.info(f"  Raw stream: {initial_raw} → {final_raw} (Δ: {final_raw - initial_raw})")
                logger.info(f"  Enriched stream: {initial_enriched} → {final_enriched} (Δ: {final_enriched - initial_enriched})")
                
                if final_enriched == initial_enriched and final_raw > initial_raw:
                     # Only complain if raw IS moving but enriched IS NOT
                    logger.error("❌ DATA PIPELINE NOT PROCESSING - Enriched stream not growing!")
                    logger.error("   → Check supervise_pipeline_workers.py")
                    
            except Exception as e:
                logger.error(f"❌ Flow check error: {e}")
        
        # Run in background thread
        flow_thread = threading.Thread(target=check_flow, daemon=True)
        flow_thread.start()

if __name__ == "__main__":
    if Path(os.getcwd()).name == "intraday_trading":
        # If running from inside intraday_trading, switch to root
        os.chdir("../..")
        sys.path.insert(0, os.getcwd())
    elif Path(os.getcwd()).name == "src":
         # Running from src
         sys.path.insert(0, os.getcwd())
    
    orch = SystemOrchestrator()
    orch.start()