# shared_core

Installable bundle of modules that are shared between the intraday scanner,
dashboard backend, and any auxiliary services.  Today it contains:

- `redis_clients`: Redis client factories, key standards, unified storage
- `volume_files`: Volume manager, correct volume calculators, dynamic baselines

Install locally with:

```bash
cd shared_core
pip install -e .
```

Then import modules via `from shared_core.redis_clients...` or
`from volume_files.volume_manager import VolumeManager`.  The compatibility
shims at the repository root ensure legacy imports continue to work while the
actual implementation lives inside this package.
