# src/patterns/singleton_factory.py
import os
import threading
import time
import traceback
import weakref
from typing import Dict, Any

class PatternDetectorSingletonFactory:
    """
    Nuclear fix: ONE single PatternDetector for entire application
    No more "every module creates its own"
    """
    
    _instance = None
    _lock = threading.RLock()
    _refs: Dict[str, Any] = {}
    
    @classmethod
    def get_instance(
        cls,
        *args,
        force_new: bool = False,
        caller: str = "unknown",
        **kwargs,
    ):
        """Get the ONE AND ONLY PatternDetector instance"""
        
        with cls._lock:
            # Debug logging
            print(f"🔍 [FACTORY] {caller} requested PatternDetector "
                  f"(force_new={force_new}, existing={cls._instance is not None})")
            
            if force_new:
                # Only for tests/debug
                from patterns.pattern_detector import PatternDetector
                new_instance = PatternDetector(*args, **kwargs)
                print(f"⚠️  [FACTORY] Creating NEW instance for {caller} (forced)")
                return new_instance
            
            if cls._instance is None:
                # First time - create it
                from patterns.pattern_detector import PatternDetector
                cls._instance = PatternDetector(*args, **kwargs)
                cls._log_creation_event(caller)
                
                # Register cleanup
                weakref.finalize(cls, cls._cleanup)
                
                print(f"🎯 [FACTORY] Created SINGLETON instance "
                      f"(PID: {os.getpid()}, Caller: {caller})")
            
            # Track who's using it
            cls._refs[caller] = time.time()
            
            return cls._instance
    
    @classmethod
    def _cleanup(cls):
        """Cleanup singleton"""
        print(f"🧹 [FACTORY] Cleaning up singleton instance")
        cls._instance = None
        cls._refs.clear()
        cls._creation_log.clear()
    
    @classmethod
    def get_refs(cls) -> Dict[str, float]:
        """Get all callers using the singleton"""
        return cls._refs.copy()
    
    @classmethod 
    def reset(cls):
        """Reset singleton (for tests only!)"""
        with cls._lock:
            cls._instance = None
            cls._refs.clear()
            cls._creation_log.clear()
            print("🔄 [FACTORY] Singleton RESET")

    _creation_log: list[Dict[str, Any]] = []

    @classmethod
    def _log_creation_event(cls, caller: str) -> None:
        stack = traceback.format_stack(limit=12)
        entry = {
            "timestamp": time.time(),
            "pid": os.getpid(),
            "caller": caller,
            "stack": [line.strip() for line in stack[:-1]],
        }
        cls._creation_log.append(entry)
        if len(cls._creation_log) > 1:
            print("🚨 [FACTORY] PatternDetector initialized multiple times!")
            for idx, event in enumerate(cls._creation_log, 1):
                print(f"  Creation #{idx} (pid={event['pid']} caller={event['caller']} ts={event['timestamp']})")
                for frame in event["stack"]:
                    print(f"    {frame}")
