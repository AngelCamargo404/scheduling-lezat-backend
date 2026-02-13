import re
from pathlib import Path
from typing import Mapping

ENV_LINE_PATTERN = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")


def read_env_file_values(env_file_path: Path) -> dict[str, str]:
    if not env_file_path.exists():
        return {}

    values: dict[str, str] = {}
    for line in env_file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = ENV_LINE_PATTERN.match(line)
        if not match:
            continue
        env_key = match.group(1)
        env_value = match.group(2).strip()
        values[env_key] = _strip_wrapping_quotes(env_value)
    return values


def update_env_file_values(*, env_file_path: Path, updates: Mapping[str, str]) -> None:
    if not updates:
        return

    current_lines = _read_lines(env_file_path)
    pending_updates = dict(updates)
    updated_lines: list[str] = []

    for line in current_lines:
        stripped = line.lstrip()
        if stripped.startswith("#"):
            updated_lines.append(line)
            continue

        match = ENV_LINE_PATTERN.match(line)
        if not match:
            updated_lines.append(line)
            continue

        env_key = match.group(1)
        if env_key not in pending_updates:
            updated_lines.append(line)
            continue

        updated_lines.append(f"{env_key}={pending_updates.pop(env_key)}")

    for env_key, env_value in pending_updates.items():
        updated_lines.append(f"{env_key}={env_value}")

    output = "\n".join(updated_lines).rstrip("\n") + "\n"
    env_file_path.write_text(output, encoding="utf-8")


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) < 2:
        return value
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    return value


def _read_lines(env_file_path: Path) -> list[str]:
    if not env_file_path.exists():
        return []
    return env_file_path.read_text(encoding="utf-8").splitlines()
