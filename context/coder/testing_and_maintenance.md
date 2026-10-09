# Testing & Maintenance Specification

This document details the testing architecture, test suites, execution commands, and debugging techniques for **AgentLandline**.

---

## 1. Testing Framework & Philosophy

AgentLandline uses **`pytest`** as its primary test runner. The test suite is divided into fast, isolated **Unit Tests** and end-to-end **Integration Tests**.

```
tests/
├── test_issue_reporter.py  # Unit: Label normalization, fuzzy matching, and mock gh calls
├── test_agent_manager.py   # Unit: SQLite conversation cloning, transcript parsing
└── test_agy_pipeline.py    # Integration: Live multi-turn agy session bridge & memory
```

---

## 2. Running Test Suites

### Run All Unit Tests (Fast, Offline)
```bash
pytest tests/test_issue_reporter.py tests/test_agent_manager.py
```

### Run Specific Test Suites
1. **GitHub Issue Reporter & Smart Labeling**:
   ```bash
   pytest tests/test_issue_reporter.py -v
   ```
   *Validates label string normalization (`"new_lable"` -> `"new lable"`), alphanumeric stripping, typo tolerance, mock `gh issue create` invocations, and auto-label creation.*

2. **AgentManager & Transcript Reader**:
   ```bash
   pytest tests/test_agent_manager.py -v
   ```
   *Validates SQLite conversation DB forking (`fork_conversation`), trajectory cloning, and reading JSONL transcripts.*

3. **Live Integration Tests**:
   ```bash
   pytest tests/test_agy_pipeline.py -s
   ```
   *Spawns a real `agy` session, sends multi-turn prompts, verifies conversational memory ("What is my name?"), and checks tool execution event streams. (Requires `agy` in `PATH`).*

---

## 3. Best Practices for Authoring Tests

1. **Isolation & Mocking**:
   - Unit tests must never depend on network calls or live GitHub credentials. Mock external CLI tools (`gh`) using `unittest.mock.patch("subprocess.run")`.
2. **Subprocess Cleanup**:
   - Any test spawning child processes must wrap execution in `try ... finally:` blocks calling `pipeline.close()` to guarantee no zombie processes remain after test runs.
3. **Python Version Compatibility**:
   - AgentLandline supports **Python 3.10+** (including Python 3.13). Avoid features or deprecations specific to a single minor version.

---

## 4. Common Diagnostics & Troubleshooting

| Symptom | Probable Cause | Resolution |
| :--- | :--- | :--- |
| `FileNotFoundError: agy` | `agy` CLI is not installed or not in `PATH` | Ensure `agy` is installed and reachable via `which agy`. |
| `BrokenPipeError` in reader | Child process terminated abruptly | Inspect `self._raw_output_buffer` or stderr logs to diagnose process crash. |
| `sqlite3.OperationalError` | Database locked during fork | Ensure previous agent session process is fully exited before cloning DB. |
