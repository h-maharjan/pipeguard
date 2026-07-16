# PipeGuard - Phase 1

Phase 1 implements:

- Accepting a GitHub repository URL from the dashboard
- Cloning that repository on the backend
- Saving project data and clone status in PostgreSQL
- Displaying project history in the dashboard

## Prerequisites

- Node.js 20+
- PostgreSQL 14+
- `git` installed on the server runtime

## Environment variables

Set one of these database configurations:

- `DATABASE_URL=******localhost:5432/pipeguard`

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

On startup, the server creates the `projects` table if it does not already exist.
