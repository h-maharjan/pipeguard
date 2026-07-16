const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const { execFile } = require("child_process");
const { promisify } = require("util");
const { Pool } = require("pg");

const execFileAsync = promisify(execFile);
const app = express();

const PORT = Number(process.env.PORT || 3000);
const CLONE_BASE_DIR = process.env.CLONE_BASE_DIR || "/tmp/pipeguard/repos";
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  host: process.env.PGHOST,
  port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
});

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function parseRepositoryInput(input) {
  const raw = String(input || "").trim();
  const githubUrlMatch = raw.match(
    /^https:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?\/?$/
  );
  if (githubUrlMatch) {
    return { owner: githubUrlMatch[1], repo: githubUrlMatch[2] };
  }

  const ownerRepoMatch = raw.match(/^([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)$/);
  if (ownerRepoMatch) {
    return { owner: ownerRepoMatch[1], repo: ownerRepoMatch[2] };
  }

  return null;
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS projects (
      id BIGSERIAL PRIMARY KEY,
      repo_url TEXT NOT NULL,
      owner TEXT NOT NULL,
      repo TEXT NOT NULL,
      clone_path TEXT,
      clone_status TEXT NOT NULL,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

async function cloneRepository(owner, repo, targetDir) {
  await fs.mkdir(path.dirname(targetDir), { recursive: true });
  await fs.rm(targetDir, { recursive: true, force: true });
  const cloneUrl = `https://github.com/${owner}/${repo}.git`;
  await execFileAsync("git", ["clone", "--depth", "1", cloneUrl, targetDir], {
    timeout: 120000,
  });
}

app.get("/api/projects", async (_req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT id, repo_url, owner, repo, clone_path, clone_status, error_message, created_at
      FROM projects
      ORDER BY created_at DESC
      LIMIT 50
      `
    );
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: "Could not load project history." });
  }
});

app.post("/api/projects", async (req, res) => {
  const parsed = parseRepositoryInput(req.body?.repositoryUrl);
  if (!parsed) {
    return res.status(400).json({
      error:
        'Invalid repository URL. Use "owner/repo" or "https://github.com/owner/repo".',
    });
  }

  const repoUrl = `https://github.com/${parsed.owner}/${parsed.repo}`;
  const clonePath = path.join(
    CLONE_BASE_DIR,
    `${parsed.owner}-${parsed.repo}-${Date.now()}`
  );

  const insertResult = await pool.query(
    `
      INSERT INTO projects (repo_url, owner, repo, clone_path, clone_status)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING id, repo_url, owner, repo, clone_path, clone_status, error_message, created_at
    `,
    [repoUrl, parsed.owner, parsed.repo, clonePath, "cloning"]
  );

  const project = insertResult.rows[0];

  try {
    await cloneRepository(parsed.owner, parsed.repo, clonePath);
    const updateResult = await pool.query(
      `
        UPDATE projects
        SET clone_status = 'success', error_message = NULL
        WHERE id = $1
        RETURNING id, repo_url, owner, repo, clone_path, clone_status, error_message, created_at
      `,
      [project.id]
    );

    return res.status(201).json(updateResult.rows[0]);
  } catch (error) {
    const errorMessage = String(error?.stderr || error?.message || "Clone failed")
      .trim()
      .slice(0, 500);
    const updateResult = await pool.query(
      `
        UPDATE projects
        SET clone_status = 'failed', error_message = $2
        WHERE id = $1
        RETURNING id, repo_url, owner, repo, clone_path, clone_status, error_message, created_at
      `,
      [project.id, errorMessage]
    );

    return res.status(500).json(updateResult.rows[0]);
  }
});

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok" });
});

async function start() {
  await ensureSchema();
  app.listen(PORT, () => {
    console.log(`PipeGuard Phase 1 server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error("Failed to start server:", error.message);
  process.exit(1);
});
