#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Validate Claude Code infrastructure setup.

Verifies that all Claude Code extensions are properly configured:
- Agents have required frontmatter
- Commands have appropriate settings
- Skills have $ARGUMENTS placeholder
- Rules exist and are linked
"""

from pathlib import Path


def check_file_has_frontmatter(file_path: Path, required_fields: list[str]) -> tuple[bool, list[str]]:
    """Check if a markdown file has required YAML frontmatter fields."""
    content = file_path.read_text()
    issues = []

    if not content.startswith("---"):
        issues.append(f"Missing YAML frontmatter in {file_path.name}")
        return False, issues

    # Find end of frontmatter
    end_idx = content.find("---", 3)
    if end_idx == -1:
        issues.append(f"Malformed frontmatter in {file_path.name}")
        return False, issues

    frontmatter = content[3:end_idx]

    for field in required_fields:
        if f"{field}:" not in frontmatter:
            issues.append(f"Missing '{field}' in {file_path.name}")

    return len(issues) == 0, issues


def validate_agents(claude_dir: Path) -> tuple[int, int, list[str]]:
    """Validate agent configurations."""
    agents_dir = claude_dir / "agents"
    if not agents_dir.exists():
        return 0, 0, ["agents/ directory not found"]

    issues = []
    total = 0
    passed = 0

    for agent_file in agents_dir.glob("*.md"):
        total += 1
        ok, file_issues = check_file_has_frontmatter(
            agent_file, ["name", "description", "tools"]
        )
        if ok:
            passed += 1
        else:
            issues.extend(file_issues)

    return passed, total, issues


def validate_commands(claude_dir: Path) -> tuple[int, int, list[str]]:
    """Validate command configurations."""
    commands_dir = claude_dir / "commands"
    if not commands_dir.exists():
        return 0, 0, ["commands/ directory not found"]

    issues = []
    total = 0
    passed = 0

    # Commands with side effects that need disable-model-invocation
    side_effect_commands = {"quick-test.md", "fetch-data.md", "debug-fcp.md"}

    for cmd_file in commands_dir.glob("*.md"):
        total += 1
        # Check required fields including argument-hint and allowed-tools
        ok, file_issues = check_file_has_frontmatter(
            cmd_file, ["name", "description", "argument-hint", "allowed-tools"]
        )

        if cmd_file.name in side_effect_commands:
            content = cmd_file.read_text()
            if "disable-model-invocation: true" not in content:
                file_issues.append(
                    f"{cmd_file.name} has side effects but missing disable-model-invocation"
                )
                ok = False

        if ok:
            passed += 1
        else:
            issues.extend(file_issues)

    return passed, total, issues


def validate_skills(docs_dir: Path) -> tuple[int, int, list[str]]:
    """Validate skill configurations."""
    skills_dir = docs_dir / "skills"
    if not skills_dir.exists():
        return 0, 0, ["docs/skills/ directory not found"]

    issues = []
    total = 0
    passed = 0

    for skill_dir in skills_dir.iterdir():
        if not skill_dir.is_dir():
            continue

        skill_file = skill_dir / "SKILL.md"
        if not skill_file.exists():
            issues.append(f"Missing SKILL.md in {skill_dir.name}/")
            total += 1
            continue

        total += 1
        ok, file_issues = check_file_has_frontmatter(
            skill_file, ["name", "description", "user-invocable"]
        )

        # Check for $ARGUMENTS placeholder
        content = skill_file.read_text()
        if "$ARGUMENTS" not in content:
            file_issues.append(f"{skill_dir.name}/SKILL.md missing $ARGUMENTS placeholder")
            ok = False

        if ok:
            passed += 1
        else:
            issues.extend(file_issues)

    return passed, total, issues


def validate_rules(claude_dir: Path) -> tuple[int, int, list[str]]:
    """Validate rules exist and have proper frontmatter."""
    rules_dir = claude_dir / "rules"
    if not rules_dir.exists():
        return 0, 0, ["rules/ directory not found"]

    expected_rules = [
        "binance-api.md",
        "caching-patterns.md",
        "dataframe-operations.md",
        "error-handling.md",
        "fcp-protocol.md",
        "symbol-formats.md",
        "timestamp-handling.md",
    ]

    # Rules that should have ADR references (FCP-related)
    rules_with_adr = {
        "binance-api.md",
        "caching-patterns.md",
        "fcp-protocol.md",
        "symbol-formats.md",
    }

    issues = []
    passed = 0
    total = len(expected_rules)

    for rule in expected_rules:
        rule_path = rules_dir / rule
        if rule_path.exists():
            # Check for paths: field
            ok, file_issues = check_file_has_frontmatter(rule_path, ["paths"])
            if not ok:
                issues.extend(file_issues)
                continue

            # Check for adr: field in FCP-related rules
            if rule in rules_with_adr:
                content = rule_path.read_text()
                if "adr:" not in content:
                    issues.append(f"{rule} missing adr: field")
                    continue

            passed += 1
        else:
            issues.append(f"Missing rule: {rule}")

    return passed, total, issues


def validate_claude_md(project_root: Path) -> tuple[bool, list[str]]:
    """Validate CLAUDE.md exists and is under 300 lines."""
    claude_md = project_root / "CLAUDE.md"
    issues = []

    if not claude_md.exists():
        return False, ["CLAUDE.md not found in project root"]

    line_count = len(claude_md.read_text().splitlines())
    if line_count > 300:
        issues.append(f"CLAUDE.md is {line_count} lines (should be <300)")
        return False, issues

    return True, []


def main() -> int:
    """Run all infrastructure validations."""
    # Find project root (look for CLAUDE.md AND .claude/ directory)
    current = Path(__file__).resolve()
    project_root = None

    for parent in current.parents:
        # Project root must have both CLAUDE.md and .claude/ directory
        if (parent / "CLAUDE.md").exists() and (parent / ".claude").is_dir():
            project_root = parent
            break

    if project_root is None:
        print("ERROR: Could not find project root (no CLAUDE.md with .claude/ found)")
        return 1

    claude_dir = project_root / ".claude"
    docs_dir = project_root / "docs"

    print("=" * 60)
    print("Claude Code Infrastructure Validation")
    print("=" * 60)
    print(f"Project: {project_root.name}")
    print()

    all_issues = []
    total_passed = 0
    total_checks = 0

    # Validate CLAUDE.md
    ok, issues = validate_claude_md(project_root)
    total_checks += 1
    if ok:
        total_passed += 1
        print("✓ CLAUDE.md: Valid")
    else:
        all_issues.extend(issues)
        print("✗ CLAUDE.md: Issues found")

    # Validate agents
    passed, total, issues = validate_agents(claude_dir)
    total_passed += passed
    total_checks += total
    if passed == total:
        print(f"✓ Agents: {passed}/{total} valid")
    else:
        all_issues.extend(issues)
        print(f"✗ Agents: {passed}/{total} valid")

    # Validate commands
    passed, total, issues = validate_commands(claude_dir)
    total_passed += passed
    total_checks += total
    if passed == total:
        print(f"✓ Commands: {passed}/{total} valid")
    else:
        all_issues.extend(issues)
        print(f"✗ Commands: {passed}/{total} valid")

    # Validate skills
    passed, total, issues = validate_skills(docs_dir)
    total_passed += passed
    total_checks += total
    if passed == total:
        print(f"✓ Skills: {passed}/{total} valid")
    else:
        all_issues.extend(issues)
        print(f"✗ Skills: {passed}/{total} valid")

    # Validate rules
    passed, total, issues = validate_rules(claude_dir)
    total_passed += passed
    total_checks += total
    if passed == total:
        print(f"✓ Rules: {passed}/{total} valid")
    else:
        all_issues.extend(issues)
        print(f"✗ Rules: {passed}/{total} valid")

    print()
    print("-" * 60)
    print(f"Total: {total_passed}/{total_checks} checks passed")

    if all_issues:
        print()
        print("Issues found:")
        for issue in all_issues:
            print(f"  - {issue}")
        return 1

    print()
    print("All infrastructure checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
