const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const { spawn } = require("child_process");
const { Pool } = require("pg");

const app = express();

const PORT = Number(process.env.PORT || 3000);
const CLONE_BASE_DIR = process.env.CLONE_BASE_DIR || "/tmp/pipeguard/repos";
const MAX_LOG_LINES = 500;
const runs = new Map();

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

function sanitizeTagPart(value) {
  return String(value).toLowerCase().replace(/[^a-z0-9_.-]/g, "-");
}

function appendLog(projectId, line) {
  const run = runs.get(projectId) || { logs: [] };
  run.logs.push(`[${new Date().toISOString()}] ${line}`);
  if (run.logs.length > MAX_LOG_LINES) {
    run.logs = run.logs.slice(-MAX_LOG_LINES);
  }
  runs.set(projectId, run);
}

function setRunStatus(projectId, status) {
  const run = runs.get(projectId) || { logs: [] };
  run.status = status;
  runs.set(projectId, run);
}

function streamLines(chunkBuffer, emitLine) {
  return (chunk) => {
    chunkBuffer.value += chunk.toString();
    const lines = chunkBuffer.value.split(/\r?\n/);
    chunkBuffer.value = lines.pop() || "";
    for (const line of lines) {
      if (line.trim()) {
        emitLine(line);
      }
    }
  };
}

function runCommand(command, args, options = {}) {
  const { cwd, onLine, allowNonZeroExit = false } = options;
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    const outBuf = { value: "" };
    const errBuf = { value: "" };

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      streamLines(outBuf, (line) => onLine && onLine(line))(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      streamLines(errBuf, (line) => onLine && onLine(line))(chunk);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (outBuf.value.trim() && onLine) onLine(outBuf.value.trim());
      if (errBuf.value.trim() && onLine) onLine(errBuf.value.trim());

      if (code !== 0 && !allowNonZeroExit) {
        const error = new Error(`Command failed with exit code ${code}: ${command}`);
        error.code = code;
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
        return;
      }

      resolve({ code, stdout, stderr });
    });
  });
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
      phase2_status TEXT NOT NULL DEFAULT 'queued',
      dockerfile_present BOOLEAN,
      build_summary JSONB,
      audit_summary JSONB,
      trivy_summary JSONB,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(
    "ALTER TABLE projects ADD COLUMN IF NOT EXISTS phase2_status TEXT NOT NULL DEFAULT 'queued'"
  );
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS dockerfile_present BOOLEAN");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS build_summary JSONB");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS audit_summary JSONB");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS trivy_summary JSONB");
  await pool.query(
    "ALTER TABLE projects ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
  );
}

async function updateProject(projectId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;
  const updates = keys.map((key, index) => `${key} = $${index + 2}`);
  const values = keys.map((key) => fields[key]);
  await pool.query(
    `
      UPDATE projects
      SET ${updates.join(", ")}, updated_at = NOW()
      WHERE id = $1
    `,
    [projectId, ...values]
  );
}

function parseNpmAuditSummary(rawOutput) {
  const parsed = JSON.parse(rawOutput);
  const vulnerabilitiesBySeverity = parsed?.metadata?.vulnerabilities || {};
  const totalVulnerabilities = Object.values(vulnerabilitiesBySeverity).reduce(
    (total, count) => total + Number(count || 0),
    0
  );
  const topIssues = Object.entries(parsed?.vulnerabilities || {})
    .slice(0, 10)
    .map(([name, detail]) => ({
      name,
      severity: detail?.severity || "unknown",
      fixAvailable: Boolean(detail?.fixAvailable),
      via:
        Array.isArray(detail?.via) && detail.via.length
          ? detail.via
              .map((entry) =>
                typeof entry === "string"
                  ? entry
                  : entry?.source || entry?.title || entry?.name || "advisory"
              )
              .slice(0, 3)
          : [],
    }));

  return {
    status: "success",
    totalDependencies: parsed?.metadata?.dependencies?.total ?? null,
    totalVulnerabilities,
    bySeverity: {
      critical: vulnerabilitiesBySeverity.critical || 0,
      high: vulnerabilitiesBySeverity.high || 0,
      moderate: vulnerabilitiesBySeverity.moderate || 0,
      low: vulnerabilitiesBySeverity.low || 0,
      info: vulnerabilitiesBySeverity.info || 0,
    },
    topIssues,
  };
}

function parseTrivySummary(rawOutput) {
  const parsed = JSON.parse(rawOutput);
  const severityTotals = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0, UNKNOWN: 0 };
  const topFindings = [];

  for (const result of parsed?.Results || []) {
    for (const vuln of result?.Vulnerabilities || []) {
      const severity = String(vuln.Severity || "UNKNOWN").toUpperCase();
      if (severityTotals[severity] === undefined) severityTotals.UNKNOWN += 1;
      else severityTotals[severity] += 1;

      if (topFindings.length < 10) {
        topFindings.push({
          id: vuln.VulnerabilityID,
          package: vuln.PkgName,
          severity,
          installedVersion: vuln.InstalledVersion || null,
          fixedVersion: vuln.FixedVersion || null,
          title: vuln.Title || vuln.Description || "",
        });
      }
    }
  }

  const totalVulnerabilities = Object.values(severityTotals).reduce((a, b) => a + b, 0);
  return {
    status: "success",
    totalVulnerabilities,
    bySeverity: severityTotals,
    topFindings,
  };
}

async function processProject(project) {
  const projectId = project.id;
  const clonePath = project.clone_path;
  const imageTag = `pipeguard-${sanitizeTagPart(project.owner)}-${sanitizeTagPart(project.repo)}-${projectId}`;

  setRunStatus(projectId, "running");
  appendLog(projectId, `Starting phase 2 pipeline for ${project.owner}/${project.repo}`);
  await updateProject(projectId, {
    clone_status: "cloning",
    phase2_status: "running",
    error_message: null,
  });

  try {
    await fs.mkdir(path.dirname(clonePath), { recursive: true });
    await fs.rm(clonePath, { recursive: true, force: true });
    appendLog(projectId, "Cloning repository...");
    await runCommand(
      "git",
      ["clone", "--depth", "1", `https://github.com/${project.owner}/${project.repo}.git`, clonePath],
      { onLine: (line) => appendLog(projectId, line) }
    );
    appendLog(projectId, "Repository cloned successfully.");
    await updateProject(projectId, { clone_status: "success" });

    const dockerfilePath = path.join(clonePath, "Dockerfile");
    let hasDockerfile = true;
    try {
      await fs.access(dockerfilePath);
    } catch {
      hasDockerfile = false;
    }

    if (!hasDockerfile) {
      appendLog(projectId, "Dockerfile not found. Repository is unsupported for phase 2.");
      await updateProject(projectId, {
        dockerfile_present: false,
        phase2_status: "unsupported",
        build_summary: {
          status: "unsupported",
          reason: "Dockerfile not found",
        },
        audit_summary: {
          status: "unsupported",
          reason: "Phase 2 stopped because Dockerfile is missing",
        },
        trivy_summary: {
          status: "unsupported",
          reason: "Phase 2 stopped because Dockerfile is missing",
        },
        error_message: "Dockerfile not found; unsupported repository for phase 2.",
      });
      setRunStatus(projectId, "unsupported");
      return;
    }

    await updateProject(projectId, { dockerfile_present: true });
    appendLog(projectId, `Dockerfile detected. Building Docker image (${imageTag})...`);

    await runCommand("docker", ["build", "-t", imageTag, "."], {
      cwd: clonePath,
      onLine: (line) => appendLog(projectId, `[docker] ${line}`),
    });
    appendLog(projectId, "Docker build completed.");
    await updateProject(projectId, {
      build_summary: {
        status: "success",
        imageTag,
      },
    });

    let auditSummary = null;
    try {
      await fs.access(path.join(clonePath, "package.json"));
      appendLog(projectId, "Running npm audit...");
      const auditCommand = await runCommand("npm", ["audit", "--json"], {
        cwd: clonePath,
        onLine: (line) => appendLog(projectId, `[npm audit] ${line}`),
        allowNonZeroExit: true,
      });
      auditSummary = parseNpmAuditSummary(auditCommand.stdout);
      appendLog(
        projectId,
        `npm audit complete. Total vulnerabilities: ${auditSummary.totalVulnerabilities}`
      );
    } catch (error) {
      auditSummary = {
        status: "unsupported",
        reason: "npm audit could not run (package.json missing or audit output unavailable)",
      };
      appendLog(projectId, `npm audit skipped: ${auditSummary.reason}`);
    }
    await updateProject(projectId, { audit_summary: auditSummary });

    let trivySummary = null;
    try {
      appendLog(projectId, "Running Trivy image scan...");
      const trivyCommand = await runCommand(
        "trivy",
        ["image", "--format", "json", imageTag],
        {
          onLine: (line) => appendLog(projectId, `[trivy] ${line}`),
          allowNonZeroExit: true,
        }
      );
      trivySummary = parseTrivySummary(trivyCommand.stdout);
      appendLog(
        projectId,
        `Trivy scan complete. Total image vulnerabilities: ${trivySummary.totalVulnerabilities}`
      );
    } catch (error) {
      trivySummary = {
        status: "failed",
        reason: String(error?.message || "Trivy scan failed").slice(0, 300),
      };
      appendLog(projectId, `Trivy scan failed: ${trivySummary.reason}`);
    }
    await updateProject(projectId, { trivy_summary: trivySummary });

    await updateProject(projectId, {
      phase2_status: "completed",
      error_message: null,
    });
    appendLog(projectId, "Phase 2 pipeline completed.");
    setRunStatus(projectId, "completed");
  } catch (error) {
    const errorMessage = String(error?.stderr || error?.message || "Pipeline failed")
      .trim()
      .slice(0, 500);
    appendLog(projectId, `Pipeline failed: ${errorMessage}`);
    setRunStatus(projectId, "failed");
    await updateProject(projectId, {
      clone_status: "failed",
      phase2_status: "failed",
      error_message: errorMessage,
    });
  }
}

app.get("/api/projects", async (_req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        id, repo_url, owner, repo, clone_path, clone_status, phase2_status,
        dockerfile_present, build_summary, audit_summary, trivy_summary,
        error_message, created_at, updated_at
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

app.get("/api/projects/:id/logs", async (req, res) => {
  const projectId = Number(req.params.id);
  if (!Number.isInteger(projectId)) {
    return res.status(400).json({ error: "Invalid project id." });
  }

  try {
    const projectResult = await pool.query(
      `
      SELECT
        id, owner, repo, clone_status, phase2_status, dockerfile_present,
        build_summary, audit_summary, trivy_summary, error_message, created_at, updated_at
      FROM projects
      WHERE id = $1
      `,
      [projectId]
    );

    if (!projectResult.rows.length) {
      return res.status(404).json({ error: "Project not found." });
    }

    const run = runs.get(projectId) || { logs: [], status: "unknown" };
    return res.json({
      project: projectResult.rows[0],
      runtimeStatus: run.status || "unknown",
      logs: run.logs || [],
    });
  } catch (error) {
    return res.status(500).json({ error: "Could not load project logs." });
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

  try {
    const insertResult = await pool.query(
      `
      INSERT INTO projects (
        repo_url, owner, repo, clone_path, clone_status, phase2_status, error_message
      )
      VALUES ($1, $2, $3, $4, $5, $6, NULL)
      RETURNING
        id, repo_url, owner, repo, clone_path, clone_status, phase2_status,
        dockerfile_present, build_summary, audit_summary, trivy_summary,
        error_message, created_at, updated_at
      `,
      [repoUrl, parsed.owner, parsed.repo, clonePath, "queued", "queued"]
    );

    const project = insertResult.rows[0];
    runs.set(project.id, { logs: [], status: "queued" });
    appendLog(project.id, `Project queued: ${project.owner}/${project.repo}`);

    processProject(project).catch((error) => {
      const failure = String(error?.message || "Background processing failed").slice(0, 300);
      appendLog(project.id, failure);
      setRunStatus(project.id, "failed");
    });

    return res.status(202).json(project);
  } catch (error) {
    return res.status(500).json({ error: "Could not enqueue project scan." });
  }
});

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok" });
});

async function start() {
  await ensureSchema();
  app.listen(PORT, () => {
    console.log(`PipeGuard Phase 2 server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error("Failed to start server:", error.message);
  process.exit(1);
});
