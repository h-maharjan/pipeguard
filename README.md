# PipeGuard - Phase 3

Phase 3 implements:

- Accepting a GitHub repository URL from the dashboard
- Running the existing Phase 2 clone/build/security scan pipeline
- Triggering a Jenkins job from the backend (`buildWithParameters`)
- Polling Jenkins build status and fetching console logs
- Persisting build history in PostgreSQL (`build_runs` table)
- Generating a polished HTML report for each build run
- Converting that report to PDF with Puppeteer
- Allowing users to revisit previous HTML reports and download PDFs

## Prerequisites

- Node.js 20+
- PostgreSQL 14+
- `git` installed on the server runtime
- `docker` installed and daemon running (for image builds)
- `trivy` installed and available on PATH (for image scan)
- Jenkins server reachable from backend runtime

## Environment variables

Set one of these database configurations:

- `DATABASE_URL=postgresql://user@localhost:5432/pipeguard`

or:

- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`

Required for Jenkins integration:

- `JENKINS_URL` (example: `https://jenkins.example.com`)
- `JENKINS_JOB` (job name)
- `JENKINS_USER`
- `JENKINS_API_TOKEN`

Optional for Jenkins job tokenized triggers:

- `JENKINS_BUILD_TOKEN`

Optional runtime settings:

- `PORT` (default `3000`)
- `CLONE_BASE_DIR` (default `/tmp/pipeguard/repos`)
- `REPORTS_DIR` (default `/tmp/pipeguard/reports`)

## Run locally

```bash
npm install
npm start
```

Open `http://localhost:3000`.

On startup, the server creates/updates the `projects` and `build_runs` tables if needed.
