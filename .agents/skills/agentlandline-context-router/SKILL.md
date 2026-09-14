---
name: agentlandline-context-router
description: Autonomous task-routing and context-loading directive for AI agents in AgentLandline. Directs the agent to read context/coder/README.md before proposing terminal commands, writing code, or opening PRs, supporting both direct user prompts and cross-agent invocations.
---

# SYSTEM_DIRECTIVE: MANDATORY PRE-TASK CONTEXT ROUTING

> **STRICT AGENT EXECUTION INSTRUCTION**: Before generating code, proposing terminal commands, or making git modifications in this repository, you MUST read the target context specification mapped to your current task intent:
> `context/coder/README.md`

---

## 1. Task Intake Classification

```text
TASK INTENT CLASSIFICATION                  MANDATORY SPECIFICATION TO READ
──────────────────────────────────────────  ──────────────────────────────────────────
[DIRECT USER TASKING]                   ──> context/coder/README.md
• Human user assigns feature/bug/refactor   ↳ then context/coder/issue_resolution_workflow.md
• Test additions or performance tuning
• Documentation or schema adjustments

[CROSS-AGENT COLLABORATION]             ──> context/coder/README.md
• Invocation from collaborating agent       ↳ then context/coder/issue_resolution_workflow.md
• Automated bug report or session handoff   ↳ and context/coder/server_and_mcp.md
• Subprocess or streaming pipe fixes        ↳ and context/coder/agy_pipeline_engine.md
```

---

## 2. Core Operational Invariants Enforced

1. **`BRANCH_NAMING`**: Author changes on a dedicated feature branch (`<agent_name>/issue-<issue_number>-<description>` or `<agent_name>/<feature-name>`), unless explicitly instructed by the user to commit to `main`.
2. **`AGENT_COMMITS`**: Author commits with custom agent metadata:
   ```bash
   git -c user.name="<AgentName>" -c user.email="<agent>@ai.local" commit -m "<type>(<scope>): <message>"
   ```
3. **`COHESIVE_PR_SCOPING`**: Do not create artificially low-scoped micro-PRs. Group core code fixes, accompanying tests, and developer context updates into one cohesive PR.
4. **`NO_AUTOMERGE`**: NEVER merge PRs or push to remote `main` autonomously. Halt execution after `gh pr create` and request human code review.
5. **`SATISFACTION_AND_SESSION_ID_GATE`**:
   - **User Tasks**: Verify satisfaction directly with the human user before finalizing.
   - **Agent Tasks**: Coordinate with the calling agent, verify satisfaction, and inquire about its active `Session ID` (`agy --conversation=<peer_id>`) for inclusion in the PR metadata banner.
6. **`NESTED_PRS_FOR_CONTINUOUS_DEV`**: When working on tasks dependent on an unmerged PR, branch off the parent feature branch and open a nested PR targeting `--base <parent_branch>`.
7. **`SUBPROCESS_SAFETY`**: Ensure all `AgyPipeline` subprocesses and background reader threads terminate cleanly without leaving orphaned zombie processes.
