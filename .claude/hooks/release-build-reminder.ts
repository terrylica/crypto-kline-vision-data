#!/usr/bin/env bun
// Issue #75: Prevent forgetting PyPI publish after semantic-release
/**
 * PostToolUse hook: Release Build Reminder (Generic/Agnostic)
 *
 * Auto-detects if the current repository has a PyPI release pipeline,
 * then reminds Claude to run the full build workflow when a new version
 * is released but not yet published to PyPI.
 *
 * PyPI Pipeline Detection (checks for ANY of these):
 * - pyproject.toml with [build-system] or [project]
 * - setup.py exists
 * - maturin in pyproject.toml (Rust+Python)
 * - mise.toml with release:pypi or publish task
 *
 * Triggers on patterns like:
 * - "Published release X.Y.Z"
 * - "Created tag vX.Y.Z"
 * - "The next release version is X.Y.Z"
 *
 * Does NOT trigger if:
 * - Repository has no PyPI pipeline
 * - Already running a full build/publish command
 * - Output contains "Published to PyPI" (already published)
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";

// ============================================================================
// TYPES
// ============================================================================

interface PostToolUseInput {
  tool_name: string;
  tool_input: {
    command?: string;
    [key: string]: unknown;
  };
  tool_response?: string;
  cwd?: string;
}

interface HookResult {
  exitCode: number;
  stdout?: string;
}

interface PyPIConfig {
  hasPyPI: boolean;
  buildSystem: "maturin" | "setuptools" | "hatchling" | "flit" | "poetry" | "unknown";
  publishCommand: string;
}

// ============================================================================
// PYPI DETECTION
// ============================================================================

function detectPyPIPipeline(projectDir: string): PyPIConfig {
  const noConfig: PyPIConfig = {
    hasPyPI: false,
    buildSystem: "unknown",
    publishCommand: "",
  };

  // Check pyproject.toml
  const pyprojectPath = join(projectDir, "pyproject.toml");
  if (existsSync(pyprojectPath)) {
    try {
      const content = readFileSync(pyprojectPath, "utf-8");

      // Check for maturin (Rust+Python)
      if (content.includes("maturin") || content.includes('build-backend = "maturin"')) {
        return {
          hasPyPI: true,
          buildSystem: "maturin",
          publishCommand: detectPublishCommand(projectDir, "maturin"),
        };
      }

      // Check for other build systems
      if (content.includes("[build-system]") || content.includes("[project]")) {
        let buildSystem: PyPIConfig["buildSystem"] = "unknown";

        if (content.includes("setuptools")) buildSystem = "setuptools";
        else if (content.includes("hatchling")) buildSystem = "hatchling";
        else if (content.includes("flit")) buildSystem = "flit";
        else if (content.includes("poetry")) buildSystem = "poetry";

        return {
          hasPyPI: true,
          buildSystem,
          publishCommand: detectPublishCommand(projectDir, buildSystem),
        };
      }
    } catch {
      // Ignore read errors
    }
  }

  // Check setup.py (legacy)
  const setupPyPath = join(projectDir, "setup.py");
  if (existsSync(setupPyPath)) {
    return {
      hasPyPI: true,
      buildSystem: "setuptools",
      publishCommand: detectPublishCommand(projectDir, "setuptools"),
    };
  }

  return noConfig;
}

function detectPublishCommand(projectDir: string, buildSystem: string): string {
  // Check mise.toml for custom publish tasks (including hub-and-spoke pattern)
  const miseConfigPaths = [
    join(projectDir, ".mise.toml"),
    join(projectDir, "mise.toml"),
  ];

  // Also check .mise/tasks/ directory for hub-and-spoke mise configs
  const miseTasksDir = join(projectDir, ".mise", "tasks");
  if (existsSync(miseTasksDir)) {
    try {
      const files = Bun.spawnSync(["ls", miseTasksDir]).stdout.toString().trim().split("\n");
      for (const file of files) {
        if (file.endsWith(".toml")) {
          miseConfigPaths.push(join(miseTasksDir, file));
        }
      }
    } catch {
      // Ignore errors listing directory
    }
  }

  for (const path of miseConfigPaths) {
    if (existsSync(path)) {
      try {
        const content = readFileSync(path, "utf-8");

        // Look for release:full or publish tasks
        // Check for both inline [tasks.X] and separate ["release:full"] formats
        if (
          content.includes("release:full") ||
          content.includes('"release:full"') ||
          content.includes("'release:full'")
        ) {
          return "mise run release:full";
        }
        if (
          content.includes("release:pypi") ||
          content.includes('"release:pypi"') ||
          content.includes("'release:pypi'")
        ) {
          // Prefer release:full if it exists, otherwise use release:pypi
          continue;
        }
        if (content.includes("[tasks.publish]") || content.includes('["publish"]')) {
          return "mise run publish";
        }
      } catch {
        // Ignore read errors
      }
    }
  }

  // Second pass: check for release:pypi if release:full not found
  for (const path of miseConfigPaths) {
    if (existsSync(path)) {
      try {
        const content = readFileSync(path, "utf-8");
        if (
          content.includes("release:pypi") ||
          content.includes('"release:pypi"') ||
          content.includes("'release:pypi'")
        ) {
          return "mise run release:pypi";
        }
      } catch {
        // Ignore read errors
      }
    }
  }

  // Default commands based on build system
  switch (buildSystem) {
    case "maturin":
      return "maturin build --release && twine upload dist/*";
    case "poetry":
      return "poetry publish --build";
    case "flit":
      return "flit publish";
    case "hatchling":
      return "hatch build && twine upload dist/*";
    default:
      return "python -m build && twine upload dist/*";
  }
}

// ============================================================================
// PATTERNS
// ============================================================================

// Patterns indicating a new release was created
const RELEASE_PATTERNS = [
  /Published release (\d+\.\d+\.\d+)/i,
  /Created tag v(\d+\.\d+\.\d+)/i,
  /The next release version is (\d+\.\d+\.\d+)/i,
  /✔\s+Published release (\d+\.\d+\.\d+)/,
];

// Patterns indicating full build already happened
const ALREADY_PUBLISHED_PATTERNS = [
  /Published to PyPI/i,
  /Complete! Published/i,
  /Uploading.*to.*pypi/i,
  /twine upload/i,
  /Successfully uploaded/i,
];

// Commands that indicate we're already doing full build
const FULL_BUILD_COMMAND_PATTERNS = [
  /mise run (release:full|release:build|release:pypi|publish)/,
  /maturin (build|publish)/,
  /twine upload/,
  /poetry publish/,
  /flit publish/,
  /hatch (build|publish)/,
  /python -m build/,
];

// ============================================================================
// OUTPUT FORMATTERS
// ============================================================================

function createReminderOutput(version: string, config: PyPIConfig): string {
  const reason = `[RELEASE-BUILD-REMINDER] Version ${version} was tagged but NOT published to PyPI.

Build system detected: ${config.buildSystem}

Run the full release workflow to build and publish:

    ${config.publishCommand}

This ensures all platform wheels are built and uploaded to PyPI.`;

  return JSON.stringify({ decision: "block", reason }, null, 2);
}

// ============================================================================
// MAIN LOGIC
// ============================================================================

async function runHook(): Promise<HookResult> {
  const stdin = await Bun.stdin.text();
  if (!stdin.trim()) {
    return { exitCode: 0 };
  }

  let input: PostToolUseInput;
  try {
    input = JSON.parse(stdin);
  } catch {
    return { exitCode: 0 };
  }

  // Only process Bash tool
  if (input.tool_name !== "Bash") {
    return { exitCode: 0 };
  }

  const command = input.tool_input?.command || "";
  const response = input.tool_response || "";

  // Get project directory from environment or cwd
  const projectDir = process.env.CLAUDE_PROJECT_DIR || input.cwd || process.cwd();

  // Check if this repo has a PyPI pipeline
  const pypiConfig = detectPyPIPipeline(projectDir);
  if (!pypiConfig.hasPyPI) {
    return { exitCode: 0 }; // Not a PyPI project, skip
  }

  // Skip if we're already running a full build command
  for (const pattern of FULL_BUILD_COMMAND_PATTERNS) {
    if (pattern.test(command)) {
      return { exitCode: 0 };
    }
  }

  // Skip if output indicates already published
  for (const pattern of ALREADY_PUBLISHED_PATTERNS) {
    if (pattern.test(response)) {
      return { exitCode: 0 };
    }
  }

  // Check for release patterns
  for (const pattern of RELEASE_PATTERNS) {
    const match = response.match(pattern);
    if (match) {
      const version = match[1];
      return {
        exitCode: 0,
        stdout: createReminderOutput(version, pypiConfig),
      };
    }
  }

  return { exitCode: 0 };
}

// ============================================================================
// ENTRY POINT
// ============================================================================

async function main(): Promise<never> {
  let result: HookResult;

  try {
    result = await runHook();
  } catch (err: unknown) {
    // On error, allow through to avoid blocking
    console.error("[RELEASE-BUILD-REMINDER] Unexpected error:", err);
    return process.exit(0);
  }

  if (result.stdout) console.log(result.stdout);
  return process.exit(result.exitCode);
}

void main();
