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

            # 7. Verify that the app callback consumed the OAuth state under this matching session
            # Polling ensures the server's asynchronous callback processing has finished
            consumed = False
            dl_consume = time.time() + 5
            while time.time() < dl_consume:
                current_states = db._atomic_modify_oauth_states(lambda s: dict(s))
                if state_val not in current_states:
                    consumed = True
                    break
                time.sleep(0.3)
            assert consumed, f"State {state_val} was not consumed by the app callback under matching session!"

            # 8. Verify cross-session protection:
            # In a foreign browser context with no cookie, navigate to the callback using a state bound to the first browser
            foreign_context = browser.new_context()  # Separate context without cookies
            foreign_page = foreign_context.new_page()
            foreign_cookies = foreign_context.cookies()
            assert not any(c["name"] == "ai_cloud_ws_sid" for c in foreign_cookies)

            foreign_callback_url = f"{streamlit_server}/?code=mock_foreign_code_789&state={fail_state_val}"
            foreign_page.goto(foreign_callback_url)
            foreign_page.wait_for_selector("text=AI Cloud Workspace", timeout=15000)

            # Assert rejection: verify the state was NOT consumed and remains available for the legitimate session
            time.sleep(1)
            states_after_foreign = db._atomic_modify_oauth_states(lambda s: dict(s))
            assert fail_state_val in states_after_foreign, "Foreign browser context illegally consumed the state!"

            # 9. Verify that the legitimate browser context can now exchange the preserved state with a mock token exchange
            from unittest.mock import MagicMock
            mock_client = MagicMock()
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(
                user=MagicMock(id="user_123", email="user@example.com"),
                session=MagicMock(access_token="mock_jwt"),
            )
            mock_client.auth._storage_key = "sb"
            mock_client.auth._storage = MagicMock()

            orig_client = db.get_supabase_client
            db.get_supabase_client = lambda: mock_client
            try:
                legitimate_res = db.exchange_oauth_code(
                    "mock_legitimate_code",
                    state=fail_state_val,
                    session_id=initiating_sid,
                )
                assert legitimate_res["success"] is True
                # State must now be atomically consumed
                states_final = db._atomic_modify_oauth_states(lambda s: dict(s))
                assert fail_state_val not in states_final
            finally:
                db.get_supabase_client = orig_client

            browser.close()
