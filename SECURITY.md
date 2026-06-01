# Security Policy

## Reporting

Please open a GitHub issue for non-sensitive security concerns. For sensitive reports, contact the maintainer privately before sharing exploit details publicly.

## Secrets

HS Scheduler stores local secrets and runtime state outside git-tracked files.

Never commit:

- Telegram bot tokens
- Telegram chat IDs
- Cloudflare API tokens
- Cloudflare account/database identifiers from a private deployment
- local scheduler data
- execution logs
- SQLite runtime databases

Use:

- `scheduler_secrets.sample.json`
- `config/distributed_runtime.sample.json`

as templates only.

## Local Execution

Task scripts configured in `scheduler_data.json` run with the user's local permissions. Only add scripts that you trust.
