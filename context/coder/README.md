# AgentLandline Coder Context & Engineering Specification

Welcome to the **AgentLandline** developer and AI agent context specification. This documentation establishes engineering standards, operational invariants, and execution workflows for both human engineers and AI coding agents contributing to this repository.

---

## 🏗 Repository Architecture & Component Map

`AgentLandline` is a high-performance Model Context Protocol (MCP) server and Python orchestration engine providing a reliable, programmatic bridge to headless **Antigravity CLI (`agy`)** agent sessions.

```
AgentLandline/
├── src/
│   ├── server.py             # FastMCP server entry point & tool dispatch
│   ├── agent_manager.py      # High-level session orchestration, DB fork, & transcript parsing
│   ├── agy_pipeline.py       # Subprocess bridge launching agy (stream-json bidirectional pipes)
│   └── issue_reporter.py     # GitHub Issue reporter with smart label normalization
├── tests/
│   ├── test_agent_manager.py # Unit tests for conversation database and transcript parsing
│   ├── test_agy_pipeline.py  # Integration test for multi-turn streaming communication
│   └── test_issue_reporter.py# Unit tests for fuzzy label matching and issue generation
├── scripts/
│   └── setup_agent_workspace.sh # Provisioning script for isolated agent workspaces
├── context/
│   └── coder/                # Developer and AI agent operational specifications
└── .agents/
    ├── mcp_config.json       # MCP client server registration
    └── skills/               # Autonomous agent routing skills
```

---

## ⚡ Dual Task Trigger Modes

AI agents operating in `AgentLandline` are designed to execute seamlessly across two primary operational modes:

### Mode A: Direct User Tasking (Human-Prompted)
- The human developer directly assigns feature requests, bugfixes, refactoring, or architectural improvements via chat/prompt.
- **Workflow**: Understand requirements, verify existing tests, implement changes incrementally, prove correctness with test output, and confirm satisfaction directly with the human user before finalizing changes or opening a PR.

### Mode B: Cross-Agent Collaboration (Autonomous Invocations)
- Another AI agent (e.g. Chip Designer Agent, Meta-Harness, or downstream orchestrator) invokes `AgentLandline` or requests a bugfix, schema extension, or interface adjustment.
- **Workflow**: Parse the caller's requirements, inspect any provided session metadata, iterate until the calling agent confirms satisfaction, inquire about the peer agent's active `Session ID`, and link both session IDs in the PR metadata banner.

---

## 🛡 Agent Operational Invariants

Every AI agent working in this repository MUST strictly abide by the following non-negotiable rules:

1. **AGENT_IDENTITY_BRANCHING**:
   - Unless explicitly instructed by the user to work on `main`, all changes should be authored on a feature branch named `<agent_name>/issue-<issue_number>-<description>` or `<agent_name>/<feature-name>` (e.g., `antigravity/issue-12-streaming-reconnect`).
2. **AGENT_IDENTITY_COMMITS**:
   - Commits authored by an AI agent must explicitly declare custom agent author metadata:
     ```bash
     git -c user.name="<AgentName>" -c user.email="<agent>@ai.local" commit -m "<type>(<scope>): <concise message>"
     ```
3. **PULL_REQUEST_EXPLANATION & COHESIVE SCOPING**:
   - When creating a Pull Request via `gh pr create`, the description MUST include:
     - Prominent Metadata Header Banner (`Resolved by Agent`, `agent_model`, `session_id`).
     - **Motivation & Problem Statement**: Clear rationale.
     - **How Are You Solving It**: Technical breakdown of changes.
     - **Verification**: Exact test results and reproduction proof.
     - **Other Useful Information**: Architectural caveats and design notes.
   - **Avoid Artificially Low-Scoped PRs**: Do not fragment related changes into disjoint micro-PRs. Group core logic updates, test coverage, and documentation into one cohesive, reviewable PR.
4. **STRICT_NO_AUTOMERGE_POLICY**:
   - AI agents MUST NEVER autonomously merge PRs or push directly to remote `main` unless the human user explicitly commands it. Stop execution immediately after `gh pr create` and request human review.
5. **SATISFACTION & SESSION ID INQUIRY GATE**:
   - **User Mode**: Present test and verification evidence to the human user and confirm completion.
   - **Agent Mode**: Coordinate with the calling/collaborating agent, iterate on changes until it explicitly confirms full satisfaction, and ask for its active `Session ID` (`agy --conversation=<id>`) to record in the PR header banner.
6. **NESTED_PRS_FOR_CONTINUOUS_DEVELOPMENT**:
   - When a PR is open and awaiting human review, subsequent tasks should not be blocked.
   - Branch off the unmerged feature branch: `git checkout -b <agent>/<next-feature> <agent>/<parent-feature>`.
   - Open a nested PR targeting the parent branch: `gh pr create --base <agent>/<parent-feature>`.
   - Retarget to `main` once the parent PR is merged: `gh pr edit <pr> --base main`.
7. **SUBPROCESS_AND_STREAM_SAFETY**:
   - `AgyPipeline` manages live OS subprocesses (`agy`) via standard input/output pipes.
   - Always ensure reader threads terminate cleanly, stream pipes are flushed, and child processes are closed via `pipeline.close()` in `finally:` blocks to prevent orphaned zombie processes.

---

## 🤖 MANDATORY_AGENT_PRE_TASK_HOOKS (Machine Directives)

> **STRICT AGENT INSTRUCTION**: Before generating code, proposing terminal commands, or executing git operations, the AI Agent MUST read the target context specification mapped to the current task intent.

| Task Intent / Action Trigger | Target Context Specification | Required Pre-Execution Context Inspection |
| :--- | :--- | :--- |
| **Issue Resolution, Branching & PRs** | [`context/coder/issue_resolution_workflow.md`](issue_resolution_workflow.md) | Dual-mode task intake, branching conventions, atomic commits, satisfaction gate, session ID inquiry, nested PRs, and PR formatting. |
| **FastMCP Server & Tool Definitions** | [`context/coder/server_and_mcp.md`](server_and_mcp.md) | FastMCP tool signatures (`initialize_agent`, `send_prompt`, `agent_status`, `exit_agent`, `report_issue`), client agent detection, and error contracts. |
| **Agy Engine & Stream-JSON Protocol** | [`context/coder/agy_pipeline_engine.md`](agy_pipeline_engine.md) | Subprocess bridge mechanics, `stream-json` parsing, token usage extraction, tool call tracking, timeout handling, and session teardown. |
| **Running Test Suites & Diagnostics** | [`context/coder/testing_and_maintenance.md`](testing_and_maintenance.md) | Pytest commands, test architecture, unit vs integration testing, and Python 3.10+ / 3.13 considerations. |

---

## 📚 Coder Context Index

- [`context/coder/issue_resolution_workflow.md`](issue_resolution_workflow.md): Standard operating procedure for issue resolution, cross-agent collaboration, nested PRs, and human review gates.
- [`context/coder/server_and_mcp.md`](server_and_mcp.md): FastMCP server architecture, tool parameter specifications, and client agent metadata handling.
- [`context/coder/agy_pipeline_engine.md`](agy_pipeline_engine.md): Technical deep-dive into `AgyPipeline`, bidirectional JSON streaming, and subprocess lifecycle management.
- [`context/coder/testing_and_maintenance.md`](testing_and_maintenance.md): Complete testing guide, running unit test suites with `pytest`, and environment compatibility.
