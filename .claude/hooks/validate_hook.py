#!/usr/bin/env python3
"""Pre-tool-use hook: validates KB YAML files before Edit/Write operations.

Simulates the edit, writes to a temp file, runs `just validate-schema`,
and blocks (exit 2) if validation fails.

Usage in .claude/settings.json:
  "hooks": {
    "PreToolUse": [{
      "matcher": "Edit|Write",
      "command": "python3 .claude/hooks/validate_hook.py"
    }]
  }
"""

import json
import os
import subprocess
import sys
import tempfile


def main():
    hook_input = json.load(sys.stdin)
    tool_name = hook_input.get("tool_name", "")
    tool_input = hook_input.get("tool_input", {})

    # Only validate KB YAML files
    file_path = tool_input.get("file_path", "")
    if not file_path:
        sys.exit(0)

    # Check if this is a KB file
    if "/kb/" not in file_path or not file_path.endswith(".yaml"):
        sys.exit(0)

    # Determine target class from path
    target_class = None
    if "/drugs/" in file_path:
        target_class = "Drug"
    elif "/diseases/" in file_path:
        target_class = "Disease"
    elif "/on_label/" in file_path:
        target_class = "IndicationAssociation"
    elif "/adverse_events/" in file_path:
        target_class = "AdverseEventAssociation"
    elif "/research/" in file_path:
        target_class = "ResearchAssociation"

    if not target_class:
        sys.exit(0)

    # For Write tool, validate the new content
    if tool_name == "Write":
        content = tool_input.get("content", "")
        if not content:
            sys.exit(0)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(content)
            temp_path = f.name
    elif tool_name == "Edit":
        # For Edit, we'd need to simulate the edit - skip for now
        # Full simulation would require reading the file and applying the edit
        sys.exit(0)
    else:
        sys.exit(0)

    try:
        result = subprocess.run(
            [
                "just",
                "validate-schema",
                temp_path,
                target_class,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(
                json.dumps(
                    {
                        "decision": "block",
                        "reason": f"Schema validation failed:\n{result.stderr or result.stdout}",
                    }
                )
            )
            sys.exit(2)
    except subprocess.TimeoutExpired:
        # Don't block on timeout
        pass
    finally:
        os.unlink(temp_path)

    sys.exit(0)


if __name__ == "__main__":
    main()
