import os
import subprocess
import logging
from typing import Optional

logger = logging.getLogger("AgentLandline.IssueReporter")

class IssueReporter:
    """
    Helper utility for reporting issues, bugs, or feature requests directly to GitHub using gh CLI.
    """

    @staticmethod
    def format_issue_body(
        body: str = "",
        label: str = "bug",
        agent_model: str = "",
        session_id: str = "unknown",
        agent_name: str = "unknown",
        log_file: str = ""
    ) -> str:
        """Formats GitHub issue body with metadata header."""
        clean_log = log_file.strip() if log_file else ""
        if clean_log:
            log_file_str = clean_log if ("logs/" in clean_log or "logs\\" in clean_log) else f"logs/{clean_log}"
            log_line = f"`{log_file_str}` (created in `logs/` folder)"
        else:
            log_line = "Created in `logs/` folder (`logs/mcp_*.log`)"

        model_str = f" (`{agent_model.strip()}`)" if (agent_model and agent_model.strip()) else ""

        header_parts = [
            f"> **Reported by Agent:** {agent_name}{model_str}",
            f"> **Session ID:** `{session_id}`",
            f"> **MCP Log File:** {log_line}",
            "---",
            ""
        ]

        content = body.strip() if (body and body.strip()) else "No issue content provided."
        return "\n".join(header_parts) + content

    @classmethod
    def ensure_label_exists(cls, label_name: str, cwd: Optional[str] = None) -> bool:
        """Checks if a label exists in the GitHub repository, creating it via gh label create if missing."""
        if not label_name or not label_name.strip():
            return False

        clean_label = label_name.strip()
        try:
            list_cmd = ["gh", "label", "list"]
            res = subprocess.run(list_cmd, capture_output=True, text=True, check=True, cwd=cwd)

            existing_labels = []
            for line in res.stdout.splitlines():
                parts = line.split('\t')
                if parts:
                    existing_labels.append(parts[0].strip().lower())

            if clean_label.lower() in existing_labels:
                return True

            description = f"Issues reported by {clean_label} AI Agent"
            create_cmd = [
                "gh", "label", "create", clean_label,
                "--description", description,
                "--color", "5319e7"
            ]
            subprocess.run(create_cmd, capture_output=True, text=True, check=True, cwd=cwd)
            logger.info(f"Created new GitHub label: {clean_label}")
            return True
        except Exception as e:
            logger.warning(f"Label check/create failed for {clean_label!r}: {e}")
            return False

    @classmethod
    def create_issue(
        cls,
        title: str,
        body: str = "",
        label: str = "bug",
        agent_model: str = "",
        session_id: str = "unknown",
        agent_name: str = "unknown",
        log_file: str = "",
        cwd: Optional[str] = None
    ) -> str:
        """
        Creates a GitHub issue via the gh CLI tool with auto-created AI agent label.
        
        Returns:
            The URL of the created issue or an error message.
        """
        issue_body = cls.format_issue_body(
            body=body,
            label=label,
            agent_model=agent_model,
            session_id=session_id,
            agent_name=agent_name,
            log_file=log_file
        )

        labels_to_apply = []
        if label and label.strip():
            labels_to_apply.append(label.strip())

        if agent_name and agent_name.strip():
            agent_label = agent_name.strip()
            if agent_label.lower() != "unknown":
                cls.ensure_label_exists(agent_label, cwd=cwd)
                if agent_label.lower() not in [l.lower() for l in labels_to_apply]:
                    labels_to_apply.append(agent_label)

        cmd = [
            "gh", "issue", "create",
            "--title", title,
            "--body", issue_body
        ]

        for lbl in labels_to_apply:
            cmd.extend(["--label", lbl])

        logger.info(f"Creating GitHub issue: title={title!r}, labels={labels_to_apply!r}, agent={agent_name!r}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                cwd=cwd
            )
            issue_url = result.stdout.strip()
            logger.info(f"Successfully created GitHub issue: {issue_url}")
            return f"Successfully created GitHub issue: {issue_url}"
        except FileNotFoundError:
            err_msg = "Error: 'gh' CLI tool is not installed or not found in PATH."
            logger.error(err_msg)
            return err_msg
        except subprocess.CalledProcessError as e:
            err_msg = f"Error creating GitHub issue via gh CLI (exit code {e.returncode}):\n{e.stderr.strip()}"
            logger.error(err_msg)
            return err_msg
