# PipeGuard - Phase 2

Phase 2 implements:

- Accepting a GitHub repository URL from the dashboard
- Cloning that repository on the backend
- Detecting whether a cloned repository has a `Dockerfile`
- Reporting repositories without `Dockerfile` as unsupported
- Building Docker images when `Dockerfile` exists
- Running `npm audit --json` and parsing dependency vulnerability summary
- Running Trivy image scan (`trivy image --format json`) and parsing severity summary
- Streaming live backend logs to the dashboard
- Displaying project history in the dashboard

## Prerequisites

- Node.js 20+
- PostgreSQL 14+
- `git` installed on the server runtime
- `docker` installed and daemon running (for image builds)
- `trivy` installed and available on PATH (for image scan)

## Environment variables

Set one of these database configurations:

- `DATABASE_URL=postgresql://user@localhost:5432/pipeguard`

or:

- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`

Optional:

- `PORT` (default `3000`)
- `CLONE_BASE_DIR` (default `/tmp/pipeguard/repos`)

## Run locally

```bash
npm install
npm start
```

Open `http://localhost:3000`.

On startup, the server creates/updates the `projects` table if needed.
