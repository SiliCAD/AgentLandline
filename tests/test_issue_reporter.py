"""
Unit tests for IssueReporter in src/issue_reporter.py.
Tests smart label matching, normalization, auto-creation with random colors,
and resilient issue creation.
"""

import sys
import os
import re
import subprocess
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from issue_reporter import IssueReporter


def test_normalize_label_text():
    assert IssueReporter.normalize_label_text("new_lable") == "new lable"
    assert IssueReporter.normalize_label_text("new-label") == "new label"
    assert IssueReporter.normalize_label_text("good-first-issue") == "good first issue"
    assert IssueReporter.normalize_label_text("help_wanted") == "help wanted"
    assert IssueReporter.normalize_label_text("  extra   spaces  and__underscores  ") == "extra spaces and underscores"


def test_clean_alphanumeric():
    assert IssueReporter.clean_alphanumeric("new_lable") == "newlable"
    assert IssueReporter.clean_alphanumeric("good-first-issue") == "goodfirstissue"
    assert IssueReporter.clean_alphanumeric("feat/ui.v1") == "featuiv1"


def test_generate_random_color():
    color = IssueReporter.generate_random_color()
    assert len(color) == 6
    assert re.match(r"^[0-9a-f]{6}$", color)
    val = int(color, 16)
    assert 0 <= val <= 0xFFFFFF


def test_find_matching_label():
    existing = [
        "bug",
        "enhancement",
        "documentation",
        "good first issue",
        "help-wanted",
        "new lable"
    ]

    # Exact match
    assert IssueReporter.find_matching_label("bug", existing) == "bug"

    # Case-insensitive match
    assert IssueReporter.find_matching_label("BUG", existing) == "bug"
    assert IssueReporter.find_matching_label("Documentation", existing) == "documentation"

    # Delimiter / typography variation: underscore to space
    assert IssueReporter.find_matching_label("new_lable", existing) == "new lable"

    # Delimiter / typography variation: space to hyphen
    assert IssueReporter.find_matching_label("help wanted", existing) == "help-wanted"
    assert IssueReporter.find_matching_label("help_wanted", existing) == "help-wanted"

    # Delimiter / typography variation: hyphen to space
    assert IssueReporter.find_matching_label("good-first-issue", existing) == "good first issue"
    assert IssueReporter.find_matching_label("good_first_issue", existing) == "good first issue"

    # Fuzzy match for common typos
    assert IssueReporter.find_matching_label("enhansement", existing) == "enhancement"
    assert IssueReporter.find_matching_label("documentaion", existing) == "documentation"

    # None for unrelated label
    assert IssueReporter.find_matching_label("unrelated_feature_xyz", existing) is None
    assert IssueReporter.find_matching_label("", existing) is None


def test_find_matching_label_vice_versa():
    # Test vice versa: GH has underscore, agent gives space or hyphens
    existing = ["new_lable", "feature_request"]
    assert IssueReporter.find_matching_label("new lable", existing) == "new_lable"
    assert IssueReporter.find_matching_label("new-lable", existing) == "new_lable"
    assert IssueReporter.find_matching_label("feature request", existing) == "feature_request"
    assert IssueReporter.find_matching_label("feature-request", existing) == "feature_request"


def test_resolve_or_create_label_existing():
    existing = ["bug", "new lable"]
    with patch("subprocess.run") as mock_run:
        resolved = IssueReporter.resolve_or_create_label("new_lable", existing)
        assert resolved == "new lable"
        # gh label create should not have been called
        mock_run.assert_not_called()


def test_resolve_or_create_label_creates_new():
    existing = ["bug", "enhancement"]
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        resolved = IssueReporter.resolve_or_create_label("custom-category", existing)
        assert resolved == "custom-category"
        assert "custom-category" in existing

        # Verify gh label create was called
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd[:4] == ["gh", "label", "create", "custom-category"]
        assert "--color" in cmd
        color_idx = cmd.index("--color") + 1
        assert len(cmd[color_idx]) == 6


def test_create_issue_with_smart_label_matching():
    existing_json = '[{"name": "bug"}, {"name": "new lable"}, {"name": "Antigravity"}]'

    def mock_subp_run(cmd, *args, **kwargs):
        if "label" in cmd and "list" in cmd:
            return MagicMock(returncode=0, stdout=existing_json)
        if "issue" in cmd and "create" in cmd:
            return MagicMock(returncode=0, stdout="https://github.com/org/repo/issues/42\n")
        return MagicMock(returncode=0, stdout="")

    with patch("subprocess.run", side_effect=mock_subp_run) as mock_run:
        res = IssueReporter.create_issue(
            title="Problem detected",
            body="Details here",
            label="new_lable",
            agent_name="antigravity"
        )

        assert "https://github.com/org/repo/issues/42" in res

        # Find the issue create call
        issue_create_call = None
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            if cmd[:3] == ["gh", "issue", "create"]:
                issue_create_call = cmd
                break

        assert issue_create_call is not None
        # Verify the smart resolved labels were used: "new lable" and "Antigravity"
        assert "--label" in issue_create_call
        labels_in_cmd = [
            issue_create_call[i + 1]
            for i, arg in enumerate(issue_create_call)
            if arg == "--label"
        ]
        assert "new lable" in labels_in_cmd
        assert "Antigravity" in labels_in_cmd


def test_create_issue_fallback_on_label_error():
    existing_json = '[{"name": "bug"}]'

    def mock_subp_run(cmd, *args, **kwargs):
        if "label" in cmd and "list" in cmd:
            return MagicMock(returncode=0, stdout=existing_json)
        if "label" in cmd and "create" in cmd:
            return MagicMock(returncode=0, stdout="")
        if "issue" in cmd and "create" in cmd:
            if "--label" in cmd:
                # Simulate GitHub CLI rejecting a label
                raise subprocess.CalledProcessError(
                    returncode=1,
                    cmd=cmd,
                    stderr="GraphQL: label not found (createIssue)"
                )
            else:
                # Retry without labels succeeds
                return MagicMock(returncode=0, stdout="https://github.com/org/repo/issues/43\n")
        return MagicMock(returncode=0, stdout="")

    with patch("subprocess.run", side_effect=mock_subp_run):
        res = IssueReporter.create_issue(
            title="Issue with failing label",
            body="Details here",
            label="broken-label",
            agent_name="test-agent"
        )
        assert "https://github.com/org/repo/issues/43" in res
        assert "labels omitted due to GitHub error" in res
