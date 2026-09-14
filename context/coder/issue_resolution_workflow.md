# Issue Resolution & Contribution Workflow

This document defines the standard operating procedure (SOP) for developing features, resolving GitHub issues, and fixing bugs in **AgentLandline**.

It is designed to govern both:
- **Direct User-Initiated Tasks**: Direct prompts from human developers.
- **Autonomous Cross-Agent Delegations**: Automated task handoffs from collaborating agents or orchestrators.

---

## 1. Dual-Track Task Intake

### Track A: Direct User Requests (Human-Prompted)
1. **Understand Intent**: Clarify ambiguous requirements before modifying files.
2. **Locate Relevant Components**: Determine whether the request targets FastMCP tools (`server.py`), session management (`agy_manager.py`), streaming subprocess bridge (`agy_pipeline.py`), or GitHub integration (`issue_reporter.py`).
3. **Check Baseline**: Run the unit test suite (`pytest tests/test_issue_reporter.py tests/test_agy_manager.py`) to ensure existing tests are green before starting work.

### Track B: Autonomous Cross-Agent Collaboration
1. **Extract Collaborator Context**: Inspect the caller's request, error tracebacks, and any passed session IDs or conversation tokens.
2. **Isolate Scope**: Ensure changes address the peer agent's specific blocker (e.g. streaming EOF, process timeout, custom conversation fork, label mismatch) without breaking general MCP tool contracts.
3. **Continuous Coordination**: Keep track of the peer agent's context and requirements throughout development.

---

## 2. Dedicated Feature Branching

Unless the human user explicitly instructs you to commit directly to `main`:

1. **Standalone Feature / Fix**:
   Create a dedicated branch based on `main`:
   ```bash
   git checkout -b <agent_name>/issue-<issue_number>-<description>
   # Example:
   git checkout -b antigravity/issue-24-stream-timeout-recovery
   ```

2. **Stacked on an Unmerged Feature Branch (Nested PR)**:
   If building upon prior work that is currently in an open PR awaiting review:
   ```bash
   git checkout -b <agent_name>/<child-feature> <agent_name>/<parent-feature-branch>
   ```

---

## 3. Incremental Atomic Commits

Commit changes incrementally as you make them rather than in one giant unreviewable diff. Every commit authored by an AI agent must explicitly declare agent author identity:

```bash
git -c user.name="<AgentName>" -c user.email="<agent>@ai.local" commit -m "<type>(<scope>): <concise summary>"
```

### Commit Types:
- `feat`: New capability or tool addition.
- `fix`: Bugfix, stream parsing correction, or process recovery fix.
- `refactor`: Structural improvement without altering external tool contracts.
- `test`: New or updated unit/integration tests.
- `docs`: Operational context or documentation updates.

---

## 4. Verification & Testing

Every code change must be validated against the repository's test suites before proposing a PR:

1. **Run Unit Tests**:
   ```bash
   pytest tests/test_issue_reporter.py tests/test_agy_manager.py
   ```
2. **Validate Integration Logic (if modifying `agy_pipeline.py`)**:
   Verify that subprocess spawning and JSON streaming remain clean and that timeouts or exceptions do not leave orphaned processes:
   ```bash
   pytest tests/test_agy_pipeline.py
   ```
3. **Verify MCP Tool Signatures**:
   Ensure `server.py` runs and registers tools without import errors or schema validation exceptions:
   ```bash
   PYTHONPATH=src python3 -c "import server; print('Server loaded successfully')"
   ```

---

## 5. Collaborator / User Satisfaction & Session ID Inquiry Gate

Before opening a Pull Request, you must complete the satisfaction gate:

### For Direct Human User Tasks:
- Present the verified solution, test evidence, and diff summary to the human user.
- Confirm that the human user is satisfied with the implementation before opening a PR or finalizing the branch.

### For Autonomous Cross-Agent Tasks:
- Share test proofs and status with the collaborating agent.
- Confirm the collaborating agent's full satisfaction with the fix or enhancement.
- **Inquire Active Session ID**: Explicitly ask the collaborating agent for its active `Session ID` (`agy --conversation=<peer_session_id>`) so it can be cited in the PR header.

---

## 6. Cohesive PR Scoping Policy

- **No Artificially Low-Scoped PRs**: Do not split closely related fixes, test suites, and documentation updates across separate micro-PRs.
- **Holistic Delivery**: A single PR should bundle:
  1. Core Python logic modifications (`src/`).
  2. Accompanying unit/integration tests (`tests/`).
  3. Associated developer context and documentation (`context/coder/`).

---

## 7. Nested PRs (Stacked Branches) for Continuous Development

Because AI agents adhere to a strict `NO_AUTOMERGE` policy, open PRs will sit awaiting human review. To prevent stalled development while waiting for human merge approval:

### 1. Branch Off the Active Parent Feature Branch
```bash
git checkout -b <agent_name>/<next-feature> <agent_name>/<parent-feature>
```

### 2. Implement and Commit On Top of Parent Commits
Implement changes incrementally on top of the parent branch commits.

### 3. Open a Nested PR Targeting the Parent Branch
Use `--base` when opening the PR so the review diff only includes the new feature's commits:
```bash
gh pr create \
  --base <agent_name>/<parent-feature> \
  --title "<type>(<scope>): <feature description>" \
  --body "> [!NOTE]
> **Nested PR (Stacked on top of #<parent_pr_number>)**
...
"
```

### 4. PR Stack Navigation Convention
Include a stack navigation map in the nested PR description:
```markdown
### 🥞 PR Stack Navigation
- ➔ **#<this_pr_number> (This PR)**: `<title>` (Base: `<parent-branch>`)
  - ↳ [#<parent_pr_number>](https://github.com/SiliCAD/AgentLandline/pull/<parent_pr_number>): `<parent title>` (Base: `main`)
```

### 5. Sync & Rebase Runbook
If the parent branch receives revisions during code review, rebase the child branch:
```bash
git checkout <child_branch>
git fetch origin
git rebase <parent_branch>
git push --force-with-lease origin <child_branch>
```

### 6. Retargeting upon Parent Merge
Once the human maintainer merges the parent PR into `main`, retarget the child PR to `main`:
```bash
gh pr edit <child_pr_number> --base main
```

---

## 8. Pull Request Formatting & Human Review Gate

### Pull Request Description Template
Every PR must follow this standard structure:

```markdown
> **Resolved by Agent:** <AgentName> (`<model_name>`)
> **Coder Session ID:** `agy --conversation=<coder_session_id>`
> **Collaborator/Peer Session ID:** `agy --conversation=<peer_session_id>` *(if cross-agent)*
---

## Motivation & Problem Statement
[Clear explanation of the problem, bug report, or feature request being solved.]

## How Are You Solving It (Technical Solution)
[Detailed technical breakdown of the code, classes, and logic modified.]

## Verification & Proof
[Exact test commands executed and results, e.g. pytest outputs proving green passes.]

## Other Useful Information
[Operational notes, caveats, edge-case observations, or follow-up suggestions.]
```

### Strict Human Review Gate (NO_AUTOMERGE)
- **Do NOT auto-merge**: Agents must NEVER call `gh pr merge` or push directly to `origin/main` without explicit user permission.
- Stop execution immediately after `gh pr create` and request human review.

---

## 📋 Step-by-Step Coder Execution Checklist

1. [ ] **Inspect Task/Issue**: Parse user prompt or collaborator instructions.
2. [ ] **Verify Baseline**: Run existing tests (`pytest tests/test_issue_reporter.py tests/test_agy_manager.py`).
3. [ ] **Create Dedicated Branch**: Run `git checkout -b <agent>/<task-desc>` (or branch off parent if stacked).
4. [ ] **Implement Changes Incrementally**: Commit atomically with agent identity (`git commit -m "..."`).
5. [ ] **Run Test Suites**: Ensure 100% test passing across modified components.
6. [ ] **Satisfaction & Session ID Gate**:
   - User mode: Confirm satisfaction with human user.
   - Agent mode: Confirm satisfaction with peer agent and obtain active session ID.
7. [ ] **Push Feature Branch**: Push to remote feature branch (`git push origin <branch>`). *(Only when authorized by user)*.
8. [ ] **Open Pull Request**: Run `gh pr create` with metadata header, Motivation, Solution, Verification, and Other Info.
9. [ ] **Halt & Request Human Review**: Stop execution and inform human reviewer. Never auto-merge.
