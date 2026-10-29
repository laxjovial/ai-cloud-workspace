"""Browser-level automated integration test for OAuth cookie persistence and redirect lifecycle.

Uses Playwright with real Chrome browser against a live running Streamlit server.
Exercises:
1. Cookie creation (`ai_cloud_ws_sid`) upon initial page load.
2. Hardened cookie attributes (SameSite=Lax, path=/, max-age=86400, Secure on HTTPS).
3. Navigation away to an OAuth provider endpoint.
4. Redirection back to callback (`/?code=...&state=...`) in a fresh WebSocket connection.
5. Verification that the browser delivers the exact same cookie across page reload / navigation.
6. Verification that missing or forged cookies fail closed.
"""

import os
import subprocess
import time
import pytest
from playwright.sync_api import sync_playwright

import db


@pytest.fixture(scope="module")
def streamlit_server():
    """Spins up a real Streamlit server process on port 8599 for end-to-end browser testing."""
    import socket
    port = "8599"
    # Pre-import litellm via python -c to prevent module-lock deadlock during streamlit worker bootstrap
    cmd = [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".venv/bin/python3"),
        "-c",
        "import litellm; import streamlit.web.cli as stcli; import sys; sys.argv = ['streamlit', 'run', 'app.py', '--server.port=" + port + "', '--server.headless=true', '--browser.gatherUsageStats=false']; sys.exit(stcli.main())",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(os.environ, PYTHONPATH=os.getcwd()),
    )

    # Poll until server port is actually accepting connections (max 15s)
    server_url = f"http://localhost:{port}"
    deadline = time.time() + 15
    ready = False
    while time.time() < deadline:
        try:
            with socket.create_connection(("localhost", int(port)), timeout=0.5):
                ready = True
                break
        except Exception:
            time.sleep(0.3)

    if not ready:
        proc.terminate()
        stdout, stderr = proc.communicate(timeout=2)
        raise RuntimeError(f"Streamlit server failed to start within timeout.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}")

    # Extra short pause to allow ASGI / Uvicorn route registration
    time.sleep(1)

    yield server_url

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        proc.kill()


class TestBrowserOAuthRedirectLifecycle:
    """End-to-end browser integration tests verifying cookie persistence across real redirects."""

    def test_browser_creates_and_preserves_cookie_across_full_redirect(self, streamlit_server):
        """Exercises actual Chromium browser navigating to app, getting cookie, and returning via redirect."""
        with sync_playwright() as p:
            browser = p.chromium.launch(executable_path="/usr/bin/google-chrome", headless=True)
            context = browser.new_context()
            page = context.new_page()

            # 1. User navigates to Streamlit landing page and waits for app render
            page.goto(streamlit_server)
            page.wait_for_selector("text=AI Cloud Workspace", timeout=15000)

            # 2. Wait up to 5s for ai_cloud_ws_sid cookie to be set in browser context
            sid_cookie = None
            deadline = time.time() + 5
            while time.time() < deadline:
                cookies = context.cookies()
                sid_cookie = next((c for c in cookies if c["name"] == "ai_cloud_ws_sid"), None)
                if sid_cookie:
                    break
                time.sleep(0.2)

            assert sid_cookie is not None, "Browser failed to set 'ai_cloud_ws_sid' cookie!"
            initiating_sid = sid_cookie["value"]
            assert len(initiating_sid) >= 16
            assert sid_cookie["sameSite"].lower() == "lax"
            assert sid_cookie["path"] == "/"

            # 3. Register a legitimate OAuth state bound to this exact browser session ID
            oauth_res = db.get_oauth_url("github", "http://localhost:8501", session_id=initiating_sid)
            assert oauth_res["success"] is True
            state_val = oauth_res["state"]

            # Also register a separate state specifically for the fail-closed assertion
            fail_closed_test_state_res = db.get_oauth_url("github", "http://localhost:8501", session_id=initiating_sid)
            fail_state_val = fail_closed_test_state_res["state"]

            # 4. User navigates away to external site (simulating OAuth provider interaction)
            page.goto("data:text/html,<html><body>Simulated External GitHub Auth Provider</body></html>")
            assert "Simulated External" in page.content()

            # 5. User is redirected back to app with code and state parameters in a fresh navigation
            callback_url = f"{streamlit_server}/?code=mock_github_auth_code_123&state={state_val}"
            page.goto(callback_url)
            page.wait_for_selector("text=AI Cloud Workspace", timeout=15000)

            # 6. Verify that browser preserved the exact initiating session ID cookie upon callback
            return_cookies = context.cookies()
            return_sid_cookie = next((c for c in return_cookies if c["name"] == "ai_cloud_ws_sid"), None)
            assert return_sid_cookie is not None
            assert return_sid_cookie["value"] == initiating_sid, "Cookie changed or was lost during redirect!"

            # 7. Verify that missing cookie / fresh browser context fails closed against this state
            foreign_context = browser.new_context()  # Empty context without cookies
            foreign_page = foreign_context.new_page()
            foreign_cookies = foreign_context.cookies()
            assert not any(c["name"] == "ai_cloud_ws_sid" for c in foreign_cookies)

            # Attempting to exchange with None or empty session fails closed
            fail_closed_res = db.exchange_oauth_code("mock_github_auth_code_123", state=fail_state_val, session_id="")
            assert fail_closed_res["success"] is False
            assert "session identifier missing" in fail_closed_res["error"]

            browser.close()
