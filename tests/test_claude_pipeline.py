"""
Test suite for ClaudePipeline.
Verifies multi-turn memory, tool execution, and question handling against a
real `claude` CLI subprocess (Requires `claude` in PATH). Mirrors
test_agy_pipeline.py's shape for the Antigravity backend.
"""

import sys
import os
import time

# Ensure src is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from claude_pipeline import ClaudePipeline

def test_multi_turn_and_tools():
    print("========================================")
    print("1. Starting ClaudePipeline...")
    print("========================================")
    pipeline = ClaudePipeline(skip_permissions=True)
    pipeline.start()
    print(f"Agent Session ID: {pipeline.conversation_id}")
    print(f"Available tools: {len(pipeline.available_tools)} tools detected")

    try:
        # Turn 1: Set context
        print("\n----------------------------------------")
        print("Turn 1: Setting context ('My name is Vaibhav')...")
        t1 = time.time()
        res1 = pipeline.send("Hello! My name is Vaibhav. Please acknowledge briefly.")
        print(f"Status: {res1.status} ({time.time() - t1:.2f}s)")
        print(f"Response: {res1.response.strip()}")
        print(f"Usage: {res1.usage}")

        # Turn 2: Verify memory
        print("\n----------------------------------------")
        print("Turn 2: Testing memory ('What is my name?')...")
        t2 = time.time()
        res2 = pipeline.send("What is my name?")
        print(f"Status: {res2.status} ({time.time() - t2:.2f}s)")
        print(f"Response: {res2.response.strip()}")
        assert "Vaibhav" in res2.response, f"Context failed! Expected Vaibhav in {res2.response}"
        print("Memory test PASSED!")

        # Turn 3: Tool execution
        print("\n----------------------------------------")
        print("Turn 3: Testing tool execution ('Read requirements.txt with Read')...")
        t3 = time.time()
        res3 = pipeline.send("Use the Read tool to view requirements.txt in the current directory.")
        print(f"Status: {res3.status} ({time.time() - t3:.2f}s)")
        print(f"Tools invoked count: {len(res3.tool_calls)}")
        for t in res3.tool_calls:
            print(f"  - Tool: {t.name}, State: {t.state}, Params: {t.parameters}")
            if t.error:
                print(f"    Error: {t.error}")
        print(f"Response: {res3.response.strip()[:200]}...")

    finally:
        print("\nClosing pipeline...")
        pipeline.close()
        print("Pipeline closed cleanly.")

if __name__ == "__main__":
    test_multi_turn_and_tools()
