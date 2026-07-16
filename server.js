const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const { spawn } = require("child_process");
const { Pool } = require("pg");
const puppeteer = require("puppeteer");

const app = express();

const PORT = Number(process.env.PORT || 3000);
const CLONE_BASE_DIR = process.env.CLONE_BASE_DIR || "/tmp/pipeguard/repos";
const REPORTS_DIR = process.env.REPORTS_DIR || "/tmp/pipeguard/reports";
const MAX_LOG_LINES = 500;
const MAX_BUILD_LOG_LENGTH = 120000;

const JENKINS_URL = (process.env.JENKINS_URL || "").replace(/\/$/, "");
const JENKINS_JOB = process.env.JENKINS_JOB || "";
const JENKINS_USER = process.env.JENKINS_USER || "";
const JENKINS_API_TOKEN = process.env.JENKINS_API_TOKEN || "";
const JENKINS_BUILD_TOKEN = process.env.JENKINS_BUILD_TOKEN || "";

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
      if (line.trim()) emitLine(line);
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

    child.on("error", (error) => reject(error));

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

function jenkinsConfigured() {
  return Boolean(JENKINS_URL && JENKINS_JOB && JENKINS_USER && JENKINS_API_TOKEN);
}

async function jenkinsRequest(url, options = {}) {
  const headers = {
    Authorization: `Basic ${Buffer.from(`${JENKINS_USER}:${JENKINS_API_TOKEN}`).toString("base64")}`,
    ...(options.headers || {}),
  };

  const response = await fetch(url, { ...options, headers });
  return response;
}

async function getJenkinsCrumb() {
  if (!jenkinsConfigured()) return null;

  try {
    const response = await jenkinsRequest(`${JENKINS_URL}/crumbIssuer/api/json`);
    if (!response.ok) return null;
    const payload = await response.json();
    if (!payload?.crumbRequestField || !payload?.crumb) return null;
    return payload;
  } catch {
    return null;
  }
}

async function triggerJenkinsJob(project, buildRunId) {
  const query = new URLSearchParams({
    owner: project.owner,
    repo: project.repo,
    projectId: String(project.id),
    buildRunId: String(buildRunId),
  });
  if (JENKINS_BUILD_TOKEN) query.set("token", JENKINS_BUILD_TOKEN);

  const url = `${JENKINS_URL}/job/${encodeURIComponent(JENKINS_JOB)}/buildWithParameters?${query.toString()}`;
  const crumb = await getJenkinsCrumb();
  const headers = crumb ? { [crumb.crumbRequestField]: crumb.crumb } : {};

  const response = await jenkinsRequest(url, { method: "POST", headers });
  if (!response.ok && response.status !== 201) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error(`Jenkins trigger failed (${response.status}): ${detail || "unknown error"}`);
  }

  const queueUrl = response.headers.get("location");
  if (!queueUrl) {
    throw new Error("Jenkins did not return queue location for triggered build.");
  }
  return queueUrl.endsWith("/") ? queueUrl : `${queueUrl}/`;
}

async function waitForBuildNumber(queueUrl, timeoutMs = 120000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const response = await jenkinsRequest(`${queueUrl}api/json`);
    if (!response.ok) {
      const detail = (await response.text()).slice(0, 500);
      throw new Error(`Failed to read Jenkins queue item: ${detail}`);
    }

    const payload = await response.json();
    if (payload?.cancelled) throw new Error("Jenkins queue item was cancelled.");
    if (payload?.executable?.number) return payload.executable.number;

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  throw new Error("Timed out while waiting for Jenkins build to start.");
}

async function readBuildInfo(buildNumber) {
  const response = await jenkinsRequest(
    `${JENKINS_URL}/job/${encodeURIComponent(JENKINS_JOB)}/${buildNumber}/api/json`
  );
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error(`Failed to read Jenkins build status: ${detail}`);
  }
  return response.json();
}

async function readBuildLogs(buildNumber) {
  const response = await jenkinsRequest(
    `${JENKINS_URL}/job/${encodeURIComponent(JENKINS_JOB)}/${buildNumber}/consoleText`
  );
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error(`Failed to read Jenkins logs: ${detail}`);
  }
  return response.text();
}

function trimBuildLogs(raw) {
  const text = String(raw || "");
  if (text.length <= MAX_BUILD_LOG_LENGTH) return text;
  return text.slice(text.length - MAX_BUILD_LOG_LENGTH);
}

function formatDate(dateLike) {
  if (!dateLike) return "-";
  return new Date(dateLike).toISOString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatSeverityRows(summary, labels) {
  return labels
    .map((label) => {
      const value = summary?.[label] ?? summary?.[label.toLowerCase()] ?? 0;
      return `<tr><td>${escapeHtml(label)}</td><td>${escapeHtml(value)}</td></tr>`;
    })
    .join("");
}

function generateHtmlReport(project, buildRun) {
  const audit = project.audit_summary || {};
  const trivy = project.trivy_summary || {};
  const buildStatus = buildRun.result || buildRun.status || "unknown";
  const logs = String(buildRun.logs || "No logs available");

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>PipeGuard Report - ${escapeHtml(project.owner)}/${escapeHtml(project.repo)}</title>
    <style>
      body { font-family: Inter, Arial, sans-serif; margin: 24px; color: #10203a; }
      h1, h2 { margin-bottom: 8px; }
      .meta { margin-bottom: 18px; padding: 12px; background: #f3f6ff; border-radius: 8px; }
      .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #e8eefc; margin-right: 6px; }
      .grid { display: grid; grid-template-columns: repeat(2, minmax(220px, 1fr)); gap: 12px; margin-top: 10px; }
      .card { border: 1px solid #d8e1f5; border-radius: 8px; padding: 12px; }
      table { width: 100%; border-collapse: collapse; margin-top: 8px; }
      td, th { border: 1px solid #dfe6f8; padding: 6px 8px; text-align: left; font-size: 13px; }
      pre { white-space: pre-wrap; background: #0a1128; color: #d8e4ff; padding: 12px; border-radius: 8px; font-size: 11px; }
    </style>
  </head>
  <body>
    <h1>PipeGuard CI/CD Security Report</h1>
    <div class="meta">
      <div><b>Repository:</b> ${escapeHtml(project.owner)}/${escapeHtml(project.repo)}</div>
      <div><b>Project ID:</b> ${escapeHtml(project.id)} | <b>Build Run:</b> ${escapeHtml(buildRun.id)}</div>
      <div><b>Created:</b> ${escapeHtml(formatDate(buildRun.created_at))}</div>
      <div><b>Jenkins Build:</b> #${escapeHtml(buildRun.jenkins_build_number || "-")} (${escapeHtml(buildStatus)})</div>
      <div style="margin-top:8px">
        <span class="pill">Clone: ${escapeHtml(project.clone_status)}</span>
        <span class="pill">Phase 2: ${escapeHtml(project.phase2_status)}</span>
        <span class="pill">Phase 3: ${escapeHtml(project.phase3_status)}</span>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>npm audit summary</h2>
        <div><b>Total vulnerabilities:</b> ${escapeHtml(audit.totalVulnerabilities ?? "-")}</div>
        <table>
          <tr><th>Severity</th><th>Count</th></tr>
          ${formatSeverityRows(audit.bySeverity || {}, ["critical", "high", "moderate", "low", "info"])}
        </table>
      </div>
      <div class="card">
        <h2>Trivy summary</h2>
        <div><b>Total vulnerabilities:</b> ${escapeHtml(trivy.totalVulnerabilities ?? "-")}</div>
        <table>
          <tr><th>Severity</th><th>Count</th></tr>
          ${formatSeverityRows(trivy.bySeverity || {}, ["CRITICAL", "HIGH", "MEDIUM", "LOW", "UNKNOWN"])}
        </table>
      </div>
    </div>

    <h2 style="margin-top:18px">Jenkins Console Log</h2>
    <pre>${escapeHtml(logs)}</pre>
  </body>
</html>`;
}

async function generatePdfFromHtml(html, outputPath) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });
    await page.pdf({
      path: outputPath,
      format: "A4",
      printBackground: true,
      margin: { top: "20px", right: "20px", bottom: "20px", left: "20px" },
    });
  } finally {
    await browser.close();
  }
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
      phase3_status TEXT NOT NULL DEFAULT 'queued',
      dockerfile_present BOOLEAN,
      build_summary JSONB,
      audit_summary JSONB,
      trivy_summary JSONB,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS build_runs (
      id BIGSERIAL PRIMARY KEY,
      project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      jenkins_queue_url TEXT,
      jenkins_build_number INTEGER,
      status TEXT NOT NULL DEFAULT 'queued',
      result TEXT,
      logs TEXT,
      report_html TEXT,
      report_pdf_path TEXT,
      report_generated_at TIMESTAMPTZ,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(
    "ALTER TABLE projects ADD COLUMN IF NOT EXISTS phase2_status TEXT NOT NULL DEFAULT 'queued'"
  );
  await pool.query(
    "ALTER TABLE projects ADD COLUMN IF NOT EXISTS phase3_status TEXT NOT NULL DEFAULT 'queued'"
  );
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS dockerfile_present BOOLEAN");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS build_summary JSONB");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS audit_summary JSONB");
  await pool.query("ALTER TABLE projects ADD COLUMN IF NOT EXISTS trivy_summary JSONB");
  await pool.query(
    "ALTER TABLE projects ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
  );

  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS jenkins_queue_url TEXT");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS jenkins_build_number INTEGER");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'queued'");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS result TEXT");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS logs TEXT");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS report_html TEXT");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS report_pdf_path TEXT");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS report_generated_at TIMESTAMPTZ");
  await pool.query("ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS error_message TEXT");
  await pool.query(
    "ALTER TABLE build_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
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

async function updateBuildRun(buildRunId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;

  const updates = keys.map((key, index) => `${key} = $${index + 2}`);
  const values = keys.map((key) => fields[key]);
  await pool.query(
    `
      UPDATE build_runs
      SET ${updates.join(", ")}, updated_at = NOW()
      WHERE id = $1
    `,
    [buildRunId, ...values]
  );
}

async function createBuildRun(projectId) {
  const result = await pool.query(
    `
      INSERT INTO build_runs (project_id, status)
      VALUES ($1, 'queued')
      RETURNING id, project_id, status, created_at
    `,
    [projectId]
  );
  return result.rows[0];
}

async function getProjectById(projectId) {
  const result = await pool.query(
    `
      SELECT
        id, repo_url, owner, repo, clone_path, clone_status, phase2_status, phase3_status,
        dockerfile_present, build_summary, audit_summary, trivy_summary,
        error_message, created_at, updated_at
      FROM projects
      WHERE id = $1
    `,
    [projectId]
  );
  return result.rows[0] || null;
}

async function runPhase3(projectId) {
  let buildRun = null;

  try {
    const project = await getProjectById(projectId);
    if (!project) return;

    if (project.phase2_status !== "completed") {
      appendLog(projectId, "Phase 3 skipped because phase 2 did not complete.");
      await updateProject(projectId, { phase3_status: "skipped" });
      return;
    }

    appendLog(projectId, "Starting Phase 3: Jenkins CI/CD, logs, and report generation.");
    await updateProject(projectId, { phase3_status: "running" });

    buildRun = await createBuildRun(projectId);

    if (!jenkinsConfigured()) {
      throw new Error(
        "Jenkins is not configured. Set JENKINS_URL, JENKINS_JOB, JENKINS_USER, and JENKINS_API_TOKEN."
      );
    }

    const queueUrl = await triggerJenkinsJob(project, buildRun.id);
    appendLog(projectId, `Jenkins job queued: ${queueUrl}`);
    await updateBuildRun(buildRun.id, { jenkins_queue_url: queueUrl, status: "queued" });

    const buildNumber = await waitForBuildNumber(queueUrl);
    appendLog(projectId, `Jenkins build started: #${buildNumber}`);
    await updateBuildRun(buildRun.id, {
      jenkins_build_number: buildNumber,
      status: "running",
    });

    let buildInfo = await readBuildInfo(buildNumber);
    while (buildInfo?.building) {
      appendLog(projectId, `Jenkins #${buildNumber} status: building...`);
      await new Promise((resolve) => setTimeout(resolve, 2500));
      buildInfo = await readBuildInfo(buildNumber);
    }

    const logs = trimBuildLogs(await readBuildLogs(buildNumber));
    const result = buildInfo?.result || "UNKNOWN";
    const status = result === "SUCCESS" ? "completed" : "failed";

    appendLog(projectId, `Jenkins #${buildNumber} completed with result: ${result}`);

    const latestProject = await getProjectById(projectId);
    const reportHtml = generateHtmlReport(latestProject || project, {
      ...buildRun,
      jenkins_build_number: buildNumber,
      logs,
      result,
      status,
      created_at: buildRun.created_at,
    });

    const pdfPath = path.join(REPORTS_DIR, `project-${projectId}-run-${buildRun.id}.pdf`);
    await generatePdfFromHtml(reportHtml, pdfPath);

    await updateBuildRun(buildRun.id, {
      status,
      result,
      logs,
      report_html: reportHtml,
      report_pdf_path: pdfPath,
      report_generated_at: new Date(),
      error_message: null,
    });

    await updateProject(projectId, {
      phase3_status: status,
      error_message: status === "failed" ? `Jenkins build failed with result: ${result}` : null,
    });

    appendLog(projectId, "Phase 3 completed. HTML/PDF report generated.");
  } catch (error) {
    const message = String(error?.message || "Phase 3 failed").slice(0, 500);
    appendLog(projectId, `Phase 3 failed: ${message}`);
    await updateProject(projectId, {
      phase3_status: "failed",
      error_message: message,
    });
    if (buildRun?.id) {
      await updateBuildRun(buildRun.id, {
        status: "failed",
        result: "FAILED",
        error_message: message,
      });
    }
  }
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
    phase3_status: "queued",
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
        phase3_status: "skipped",
        build_summary: { status: "unsupported", reason: "Dockerfile not found" },
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
    } catch {
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
      const trivyCommand = await runCommand("trivy", ["image", "--format", "json", imageTag], {
        onLine: (line) => appendLog(projectId, `[trivy] ${line}`),
        allowNonZeroExit: true,
      });
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
    await runPhase3(projectId);

    const updatedProject = await getProjectById(projectId);
    if (updatedProject?.phase3_status === "completed") setRunStatus(projectId, "completed");
    else if (updatedProject?.phase3_status === "failed") setRunStatus(projectId, "failed");
    else setRunStatus(projectId, "running");
  } catch (error) {
    const errorMessage = String(error?.stderr || error?.message || "Pipeline failed")
      .trim()
      .slice(0, 500);
    appendLog(projectId, `Pipeline failed: ${errorMessage}`);
    setRunStatus(projectId, "failed");
    await updateProject(projectId, {
      clone_status: "failed",
      phase2_status: "failed",
      phase3_status: "failed",
      error_message: errorMessage,
    });
  }
}

app.get("/api/projects", async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        p.id, p.repo_url, p.owner, p.repo, p.clone_path, p.clone_status, p.phase2_status, p.phase3_status,
        p.dockerfile_present, p.build_summary, p.audit_summary, p.trivy_summary,
        p.error_message, p.created_at, p.updated_at,
        br.id AS latest_build_run_id,
        br.jenkins_build_number AS latest_jenkins_build_number,
        br.status AS latest_build_status,
        br.result AS latest_build_result,
        br.report_generated_at AS latest_report_generated_at
      FROM projects p
      LEFT JOIN LATERAL (
        SELECT id, jenkins_build_number, status, result, report_generated_at
        FROM build_runs
        WHERE project_id = p.id
        ORDER BY created_at DESC
        LIMIT 1
      ) br ON true
      ORDER BY p.created_at DESC
      LIMIT 50
    `);
    res.json(result.rows);
  } catch {
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
        id, owner, repo, clone_status, phase2_status, phase3_status, dockerfile_present,
        build_summary, audit_summary, trivy_summary, error_message, created_at, updated_at
      FROM projects
      WHERE id = $1
      `,
      [projectId]
    );

    if (!projectResult.rows.length) {
      return res.status(404).json({ error: "Project not found." });
    }

    const buildResult = await pool.query(
      `
      SELECT
        id, status, result, jenkins_build_number,
        report_generated_at, error_message, created_at
      FROM build_runs
      WHERE project_id = $1
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [projectId]
    );

    const run = runs.get(projectId) || { logs: [], status: "unknown" };
    return res.json({
      project: projectResult.rows[0],
      latestBuild: buildResult.rows[0] || null,
      runtimeStatus: run.status || "unknown",
      logs: run.logs || [],
    });
  } catch {
    return res.status(500).json({ error: "Could not load project logs." });
  }
});

app.get("/api/projects/:id/builds", async (req, res) => {
  const projectId = Number(req.params.id);
  if (!Number.isInteger(projectId)) {
    return res.status(400).json({ error: "Invalid project id." });
  }

  try {
    const result = await pool.query(
      `
      SELECT
        id, project_id, jenkins_queue_url, jenkins_build_number,
        status, result, report_generated_at, error_message, created_at, updated_at
      FROM build_runs
      WHERE project_id = $1
      ORDER BY created_at DESC
      LIMIT 50
      `,
      [projectId]
    );

    return res.json(result.rows);
  } catch {
    return res.status(500).json({ error: "Could not load build history." });
  }
});

app.get("/api/build-runs/:runId/logs", async (req, res) => {
  const runId = Number(req.params.runId);
  if (!Number.isInteger(runId)) return res.status(400).json({ error: "Invalid build run id." });

  try {
    const result = await pool.query(
      `
      SELECT id, project_id, jenkins_build_number, status, result, logs, error_message, created_at
      FROM build_runs
      WHERE id = $1
      `,
      [runId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "Build run not found." });
    }

    return res.json(result.rows[0]);
  } catch {
    return res.status(500).json({ error: "Could not load build logs." });
  }
});

app.get("/api/build-runs/:runId/report/html", async (req, res) => {
  const runId = Number(req.params.runId);
  if (!Number.isInteger(runId)) return res.status(400).send("Invalid build run id.");

  try {
    const result = await pool.query("SELECT report_html FROM build_runs WHERE id = $1", [runId]);
    if (!result.rows.length || !result.rows[0].report_html) {
      return res.status(404).send("HTML report not found.");
    }

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.send(result.rows[0].report_html);
  } catch {
    return res.status(500).send("Could not load HTML report.");
  }
});

app.get("/api/build-runs/:runId/report/pdf", async (req, res) => {
  const runId = Number(req.params.runId);
  if (!Number.isInteger(runId)) return res.status(400).send("Invalid build run id.");

  try {
    const result = await pool.query(
      "SELECT project_id, report_pdf_path FROM build_runs WHERE id = $1",
      [runId]
    );
    if (!result.rows.length || !result.rows[0].report_pdf_path) {
      return res.status(404).send("PDF report not found.");
    }

    const filePath = result.rows[0].report_pdf_path;
    await fs.access(filePath);

    return res.download(filePath, `pipeguard-report-project-${result.rows[0].project_id}-run-${runId}.pdf`);
  } catch {
    return res.status(500).send("Could not load PDF report.");
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
  const clonePath = path.join(CLONE_BASE_DIR, `${parsed.owner}-${parsed.repo}-${Date.now()}`);

  try {
    const insertResult = await pool.query(
      `
      INSERT INTO projects (
        repo_url, owner, repo, clone_path, clone_status, phase2_status, phase3_status, error_message
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
      RETURNING
        id, repo_url, owner, repo, clone_path, clone_status, phase2_status, phase3_status,
        dockerfile_present, build_summary, audit_summary, trivy_summary,
        error_message, created_at, updated_at
      `,
      [repoUrl, parsed.owner, parsed.repo, clonePath, "queued", "queued", "queued"]
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
  } catch {
    return res.status(500).json({ error: "Could not enqueue project scan." });
  }
});

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok" });
});

async function start() {
  await ensureSchema();
  app.listen(PORT, () => {
    console.log(`PipeGuard Phase 3 server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error("Failed to start server:", error.message);
  process.exit(1);
});
