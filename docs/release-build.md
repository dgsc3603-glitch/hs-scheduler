# Windows Release Build

This project can be distributed as either a Python source checkout or a PyInstaller Windows package.

## Build

From the repository root, double-click:

```text
build_release.bat
```

Or run:

```powershell
python -m pip install -r requirements.txt pyinstaller
python -m PyInstaller --clean --noconfirm hs_scheduler.spec
```

The packaged app is created under:

```text
dist/HS Scheduler/
```

The build script also creates:

```text
dist/HS-Scheduler-Windows.zip
```

## Included Files

The PyInstaller spec includes:

- `scheduler_data.sample.json`
- `scheduler_secrets.sample.json`
- `config/distributed_runtime.sample.json`
- `config/distributed_runtime.main_pc.sample.json`
- `config/distributed_runtime.sub_laptop.sample.json`
- `README.md`
- `SECURITY.md`
- `LICENSE`

## Do Not Include

Before creating a GitHub Release ZIP, confirm that the package does not contain local runtime data:

- `scheduler_data.json`
- `scheduler_secrets.json`
- `scheduler_engine.db`
- `logs/`
- `task_logs/`
- `config/distributed_runtime.json`

The source repository ignores these files, but release packages should still be checked manually.
