# HS Scheduler

HS Scheduler is a Windows desktop automation scheduler for running Python task pipelines, monitoring execution logs, and optionally coordinating Main/Sub failover through a shared Cloudflare D1 control plane.

## Features

- Project-based Python task scheduling
- Step-based task execution with selectable tasks
- Local engine process with HTTP control API
- Execution summary and detail log viewer
- Telegram notification support
- Optional Main/Sub distributed execution lease
- Cloudflare D1-backed run coordination

## Requirements

- Windows 10 or later
- Python 3.11+
- Tkinter, included with the standard Windows Python installer

Install Python dependencies:

```powershell
pip install -r requirements.txt
```

Or double-click:

```text
install_requirements.bat
```

For first-time Windows setup and launch, double-click:

```text
setup_and_run.bat
```

## Quick Start

1. Copy the sample scheduler data:

```powershell
Copy-Item scheduler_data.sample.json scheduler_data.json
```

2. Optional: copy the sample Telegram secrets file:

```powershell
Copy-Item scheduler_secrets.sample.json scheduler_secrets.json
```

3. Start the desktop app:

```powershell
python .\hs_scheduler.py
```

For a GUI-only launch without a console window:

```powershell
pythonw .\hs_scheduler.py
```

On Windows, you can also double-click:

```text
start_hs_scheduler.bat
```

For troubleshooting with console output:

```text
start_hs_scheduler_console.bat
```

To build a Windows release package:

```text
build_release.bat
```

See `docs/release-build.md` for release packaging notes.

## Configuration

Runtime files are intentionally ignored by git:

- `scheduler_data.json`
- `scheduler_secrets.json`
- `scheduler_engine.db`
- `logs/`
- `task_logs/`
- `config/distributed_runtime.json`

Use the checked-in sample files as templates:

- `scheduler_data.sample.json`
- `scheduler_secrets.sample.json`
- `config/distributed_runtime.sample.json`
- `config/distributed_runtime.main_pc.sample.json`
- `config/distributed_runtime.sub_laptop.sample.json`

## Main/Sub Failover

Main/Sub mode is optional. When enabled, each PC points to the same Cloudflare D1 database. The lease owner is allowed to run scheduled jobs; the standby node waits until it owns the lease.

Open `Main/Sub Settings` in the app to configure:

- Node ID
- Main PC or Sub PC role
- Cloudflare D1 account/database/token
- local artifact/archive paths

Do not commit real Cloudflare tokens or local runtime files.

## Tests

```powershell
python button_smoke_test.py
python engine_resilience_test.py
python stage2_smoke_test.py
```

## Public Repository Safety

Before publishing, confirm these files are not staged:

- `scheduler_secrets.json`
- `scheduler_data.json`
- `scheduler_engine.db*`
- `logs/`
- `task_logs/`
- `backup/`
- `_*.png`

The repository includes `.gitignore` rules for these paths.

## License

MIT
