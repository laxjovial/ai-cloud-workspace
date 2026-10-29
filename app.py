"""Main Streamlit Application for AI Cloud Workspace."""

import importlib
import os
from datetime import datetime
import streamlit as st
import agent
import db
from sandbox import WorkspaceSandbox

# Ensure db module is fresh
importlib.reload(db)

# Configure page
st.set_page_config(
    page_title="AI Cloud Workspace",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom High-Quality Styling (Big buttons, readable inputs, sleek dark theme)
st.markdown(
    """
<style>
    /* Big, prominent submit and primary action buttons */
    .stButton > button, .stFormSubmitButton > button {
        font-size: 16px !important;
        font-weight: 700 !important;
        padding: 12px 24px !important;
        border-radius: 8px !important;
        width: 100% !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="primary"], .stFormSubmitButton > button[kind="primary"] {
        background-color: #238636 !important;
        color: #ffffff !important;
        border: 1px solid #2ea043 !important;
    }
    .stButton > button[kind="primary"]:hover, .stFormSubmitButton > button[kind="primary"]:hover {
        background-color: #2ea043 !important;
        border-color: #3fb950 !important;
        box-shadow: 0 4px 12px rgba(46, 160, 67, 0.3) !important;
    }
    
    /* Sleek OAuth Buttons */
    .oauth-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        background-color: #21262d;
        color: #f0f6fc !important;
        padding: 13px 18px;
        border-radius: 8px;
        text-decoration: none !important;
        font-weight: 600;
        font-size: 15px;
        border: 1px solid #30363d;
        margin-bottom: 10px;
        text-align: center;
        width: 100%;
        box-sizing: border-box;
        transition: all 0.2s ease;
    }
    .oauth-btn:hover {
        background-color: #30363d;
        border-color: #58a6ff;
        color: #ffffff !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    
    /* Profile & Status Cards */
    .profile-card {
        background: linear-gradient(135deg, #161b22, #0d1117);
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 14px;
        margin-bottom: 16px;
    }
    .status-badge-green {
        color: #3fb950;
        font-weight: 600;
        font-size: 12px;
    }
    .status-badge-red {
        color: #f85149;
        font-weight: 600;
        font-size: 12px;
    }
    
    /* Terminal output */
    .terminal-box {
        background-color: #0d1117;
        color: #58a6ff;
        font-family: 'Courier New', Courier, monospace;
        padding: 14px;
        border-radius: 8px;
        font-size: 13px;
        white-space: pre-wrap;
        max-height: 380px;
        overflow-y: auto;
        border: 1px solid #30363d;
    }
    .badge {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: bold;
        background-color: #238636;
    /* Disabled button cursor: Hand with Cancel/Forbidden symbol 🚫 */
    button:disabled, .stButton button:disabled, .stFormSubmitButton button:disabled {
        cursor: not-allowed !important;
        opacity: 0.5 !important;
        pointer-events: auto !important;
        background-color: #21262d !important;
        border: 1px solid #30363d !important;
        color: #8b949e !important;
        box-shadow: none !important;
        transform: none !important;
    }
    button:disabled:hover, .stButton button:disabled:hover, .stFormSubmitButton button:disabled:hover {
        cursor: not-allowed !important;
        background-color: #21262d !important;
        border-color: #f85149 !important; /* Subtle cancel red indicator on hover */
        box-shadow: 0 0 8px rgba(248, 81, 73, 0.25) !important;
    }

    /* Rolling wheel spinner animation for loading/busy states */
    @keyframes rolling-wheel-spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    .stApp[data-test-script-state="running"] button,
    .stApp[data-test-script-state="running"] .stButton > button {
        cursor: wait !important;
    }
    .rolling-wheel {
        display: inline-block;
        width: 14px;
        height: 14px;
        border: 2px solid rgba(255, 255, 255, 0.25);
        border-radius: 50%;
        border-top-color: #58a6ff;
        animation: rolling-wheel-spin 0.75s linear infinite;
        vertical-align: middle;
        margin-right: 6px;
    }

    /* Category Badges for GitHub Accounts */
    .cat-badge-personal { background-color: #1f6feb; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
    .cat-badge-work { background-color: #8957e5; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
    .cat-badge-opensource { background-color: #238636; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
    .cat-badge-org { background-color: #d29922; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
    .cat-badge-custom { background-color: #388bfd; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
</style>
""",
    unsafe_allow_html=True,
)

# Initialize Session State
if "user" not in st.session_state:
    st.session_state.user = None
if "current_session_id" not in st.session_state:
    st.session_state.current_session_id = None
if "active_repo" not in st.session_state:
    st.session_state.active_repo = None
if "auth_tab" not in st.session_state:
    st.session_state.auth_tab = "Log In"
if "login_prefill" not in st.session_state:
    st.session_state.login_prefill = ""
if "signup_prefill" not in st.session_state:
    st.session_state.signup_prefill = ""
if "auth_flash" not in st.session_state:
    st.session_state.auth_flash = None
if "active_gh_account_id" not in st.session_state:
    st.session_state.active_gh_account_id = None
if "quick_action_prompt" not in st.session_state:
    st.session_state.quick_action_prompt = None
if "top_nav" not in st.session_state:
    st.session_state.top_nav = "💬 Workspace & Chat"
if "selected_model_label" not in st.session_state:
    st.session_state.selected_model_label = "Google Gemini 2.5 Pro (Recommended)"

# =========================================================
# 0. HANDLE OAUTH CALLBACK (SAFE & ROBUST)
# =========================================================

if "code" in st.query_params and not st.session_state.user:
    auth_code = st.query_params.get("code")
    auth_state = st.query_params.get("state")
    st.query_params.clear()

    # Clear URL bar via JavaScript so stale codes are never re-evaluated
    st.html(
        "<script>window.history.replaceState({}, '', window.location.pathname);</script>"
    )

    try:
        with st.spinner("Completing authentication..."):
            oauth_res = db.exchange_oauth_code(auth_code, state=auth_state)
        if oauth_res.get("success"):
            st.session_state.user = oauth_res["data"].user
            st.session_state.auth_flash = (
                "success",
                "🎉 Logged in successfully!",
            )
            st.rerun()
        else:
            err = oauth_res.get("error", "")
            if "already used" not in err.lower() and "expired" not in err.lower():
                st.session_state.auth_flash = (
                    "info",
                    "💡 Could not complete sign in. You can also sign in with email below.",
                )
    except Exception:
        pass


@st.cache_data(ttl=180, show_spinner=False)
def fetch_user_github_repos(token: str):
    """Fetches list of repository full names (owner/repo) using GitHub token."""
    if not token:
        return []
    try:
        from github import Auth, Github

        try:
            gh = Github(auth=Auth.Token(token))
        except Exception:
            gh = Github(token)
        repos = []
        for r in gh.get_user().get_repos(sort="updated", direction="desc"):
            repos.append(r.full_name)
            if len(repos) >= 100:
                break
        return repos
    except Exception:
        return []


# =========================================================
# 1. LANDING & AUTHENTICATION VIEWS
# =========================================================


def render_landing_and_auth():
    """Renders the landing hero page and authentication controls."""
    st.title("⚡ AI Cloud Workspace")
    st.subheader(
        "Autonomous AI coding with live Linux sandboxes, terminal tests, & mobile web previews."
    )

    col1, col2 = st.columns([3, 2], gap="large")

    with col1:
        st.markdown(
            """
        ### What it does:
        * 💻 **Any Language & Command**: Runs `npm`, `python`, `go run`, `cargo`, `pytest`, `docker` in cloud Linux sandboxes.
        * 📱 **Live Mobile Web Previews**: Tell the AI *"run this project"*, and it launches a dev server with a public live URL accessible from your phone or any browser.
        * 🐙 **Multi-GitHub Account Support**: Connect your Personal, Work, or Client GitHub accounts and switch between them seamlessly.
        * 🔑 **Bring Your Own Key (BYOK)**: Connect your own Gemini, Claude 3.7, GPT-4o, or DeepSeek key with zero host markups.
        * ☁️ **100% Cloud**: Works from any device without running anything on localhost.
        """
        )

        st.info("💡 Ready to start? Sign in or create an account.")

    with col2:
        # Display persistent friendly flash notifications
        if st.session_state.auth_flash:
            flash_type, flash_msg = st.session_state.auth_flash
            if flash_type == "success":
                st.success(flash_msg)
            elif flash_type == "info":
                st.info(flash_msg)
            elif flash_type == "warning":
                st.warning(flash_msg)
            elif flash_type == "error":
                st.error(flash_msg)
            st.session_state.auth_flash = None

        # Tab Selection using Radio for instant switching
        tab_options = ["Log In", "Create Account"]
        current_index = 0 if st.session_state.auth_tab == "Log In" else 1
        selected_tab = st.radio(
            "Authentication Mode",
            tab_options,
            index=current_index,
            horizontal=True,
            label_visibility="collapsed",
        )
        st.session_state.auth_tab = selected_tab

        redirect_url = "http://localhost:8501"

        if selected_tab == "Log In":
            st.markdown("#### Quick Sign In")
            g_label = "🌐 Sign In with Google"
            gh_label = "🐙 Sign In with GitHub"
        else:
            st.markdown("#### Quick Sign Up")
            g_label = "🌐 Sign Up with Google"
            gh_label = "🐙 Sign Up with GitHub"

        o_col1, o_col2 = st.columns(2)
        google_auth = db.get_oauth_url("google", redirect_url)
        github_auth = db.get_oauth_url("github", redirect_url)

        with o_col1:
            if google_auth.get("success"):
                st.markdown(
                    f'<a class="oauth-btn" href="{google_auth["url"]}" target="_self">{g_label}</a>',
                    unsafe_allow_html=True,
                )
            else:
                st.caption("Google sign in currently unavailable")

        with o_col2:
            if github_auth.get("success"):
                st.markdown(
                    f'<a class="oauth-btn" href="{github_auth["url"]}" target="_self">{gh_label}</a>',
                    unsafe_allow_html=True,
                )
            else:
                st.caption("GitHub sign in currently unavailable")

        st.markdown("---")

        # TAB: LOG IN
        if selected_tab == "Log In":
            st.markdown("#### Sign In with Email")
            with st.form("login_form", clear_on_submit=False):
                login_email = st.text_input(
                    "Email Address",
                    value=st.session_state.login_prefill,
                    key="login_email_input",
                )
                login_password = st.text_input(
                    "Password", type="password", key="login_password_input"
                )
                submit_login = st.form_submit_button(
                    "Sign In", type="primary", use_container_width=True
                )

                if submit_login:
                    if not login_email or not login_password:
                        st.warning("Please enter your email and password.")
                    else:
                        with st.spinner("Signing in..."):
                            res = db.sign_in_with_email(
                                login_email.strip(), login_password
                            )
                        if res.get("success"):
                            st.session_state.user = res["data"].user
                            st.session_state.auth_flash = None
                            st.session_state.login_prefill = ""
                            st.rerun()
                        else:
                            err = res.get("error", "")
                            if (
                                "Invalid login credentials" in err
                                or "User not found" in err
                            ):
                                st.warning(
                                    f"👋 We couldn't find an account for '{login_email}'. Click below to create your account in seconds!"
                                )
                                st.session_state.signup_prefill = (
                                    login_email.strip()
                                )
                            elif "Email not confirmed" in err:
                                st.warning(
                                    "⚠️ Your email address hasn't been confirmed yet. Please check your inbox or disable email confirmation in your Supabase dashboard."
                                )
                            else:
                                st.info(
                                    "Could not sign in with those credentials. Please check your password or create a new account."
                                )

            if st.session_state.signup_prefill:
                if st.button(
                    f"👉 Create an Account with '{st.session_state.signup_prefill}'",
                    use_container_width=True,
                ):
                    st.session_state.auth_tab = "Create Account"
                    st.rerun()

        # TAB: CREATE ACCOUNT
        else:
            st.markdown("#### Create Account with Email")
            with st.form("signup_form", clear_on_submit=False):
                signup_email = st.text_input(
                    "Email Address",
                    value=st.session_state.signup_prefill,
                    key="signup_email_input",
                )
                signup_password = st.text_input(
                    "Password (min 6 characters)",
                    type="password",
                    key="signup_password_input",
                )
                signup_confirm = st.text_input(
                    "Confirm Password",
                    type="password",
                    key="signup_confirm_input",
                )
                submit_signup = st.form_submit_button(
                    "Create Account", type="primary", use_container_width=True
                )

                if submit_signup:
                    if not signup_email or not signup_password:
                        st.warning("Please fill in all fields.")
                    elif len(signup_password) < 6:
                        st.warning(
                            "Password must be at least 6 characters long."
                        )
                    elif signup_password != signup_confirm:
                        st.error("Passwords do not match!")
                    else:
                        with st.spinner("Creating your account..."):
                            res = db.sign_up_with_email(
                                signup_email.strip(), signup_password
                            )
                        if res.get("success"):
                            data = res.get("data")
                            if data and getattr(data, "session", None):
                                st.session_state.user = data.user
                                st.session_state.auth_flash = None
                                st.rerun()
                            else:
                                st.session_state.auth_flash = (
                                    "success",
                                    f"🎉 Account '{signup_email}' created successfully! Check your inbox to verify, or sign in below.",
                                )
                                st.session_state.auth_tab = "Log In"
                                st.session_state.login_prefill = signup_email
                                st.rerun()
                        else:
                            err = res.get("error", "")
                            if (
                                "already registered" in err.lower()
                                or "already exists" in err.lower()
                            ):
                                st.session_state.auth_tab = "Log In"
                                st.session_state.login_prefill = signup_email
                                st.session_state.auth_flash = (
                                    "info",
                                    f"ℹ️ You already have an account with '{signup_email}'! Please enter your password to sign in.",
                                )
                                st.rerun()
                            else:
                                st.info(
                                    "Sign up could not be completed. Please verify your email format or try another address."
                                )


# =========================================================
# 2. WORKSPACE & CHAT VIEW
# =========================================================


def render_workspace():
    """Renders the main authenticated dashboard with top navigation bar and views."""
    user = st.session_state.user

    # Auto-link GitHub account if logged in via GitHub OAuth
    db.auto_link_oauth_github(user)

    # 1. Restore persisted state across logouts / timeouts
    saved_state = db.load_user_workspace_state(user.id, user.email)
    if st.session_state.active_gh_account_id is None and saved_state.get(
        "active_gh_account_id"
    ):
        st.session_state.active_gh_account_id = saved_state[
            "active_gh_account_id"
        ]
    if st.session_state.active_repo is None and saved_state.get("active_repo"):
        st.session_state.active_repo = saved_state["active_repo"]
    if st.session_state.current_session_id is None and saved_state.get(
        "current_session_id"
    ):
        st.session_state.current_session_id = saved_state["current_session_id"]
    if saved_state.get("active_model"):
        st.session_state.selected_model_label = saved_state["active_model"]

    # 2. Fetch user API keys & GitHub accounts
    user_keys = db.get_user_api_keys(user.id, user.email)
    gh_accounts = db.get_user_github_accounts(user.id, user.email)

    # 3. Determine active GitHub account
    active_account = None
    if gh_accounts:
        if st.session_state.active_gh_account_id:
            active_account = next(
                (
                    a
                    for a in gh_accounts
                    if a["id"] == st.session_state.active_gh_account_id
                ),
                gh_accounts[0],
            )
        else:
            active_account = gh_accounts[0]
            st.session_state.active_gh_account_id = active_account["id"]
            db.save_user_workspace_state(
                user.id,
                user.email,
                active_gh_account_id=active_account["id"],
            )

    active_gh_token = (
        active_account.get("github_token", "") if active_account else ""
    )

    # 4. Model configuration
    model_groups = {
        "Google Gemini 2.5 Pro (Recommended)": "gemini/gemini-2.5-pro",
        "Google Gemini 2.5 Flash (Fast)": "gemini/gemini-2.5-flash",
        "Google Gemini 2.0 Flash": "gemini/gemini-2.0-flash",
        "Google Gemini 2.0 Flash Thinking": "gemini/gemini-2.0-flash-thinking-exp",
        "Google Gemini 1.5 Pro": "gemini/gemini-1.5-pro",
        "Google Gemini 1.5 Flash": "gemini/gemini-1.5-flash",
        "Anthropic Claude 3.7 Sonnet (Hybrid Thinking)": "anthropic/claude-3-7-sonnet-20250219",
        "Anthropic Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20241022",
        "Anthropic Claude 3.5 Haiku": "anthropic/claude-3-5-haiku-20241022",
        "OpenAI GPT-4o": "openai/gpt-4o",
        "OpenAI GPT-4o Mini": "openai/gpt-4o-mini",
        "OpenAI o3-mini (Reasoning)": "openai/o3-mini",
        "DeepSeek Reasoner (R1)": "deepseek/deepseek-reasoner",
        "DeepSeek Chat (V3)": "deepseek/deepseek-chat",
    }

    if st.session_state.get("selected_model_label") not in model_groups:
        st.session_state.selected_model_label = (
            "Google Gemini 2.5 Pro (Recommended)"
        )
    selected_model_label = st.session_state.selected_model_label
    selected_model = model_groups[selected_model_label]
    provider_name = (
        selected_model.split("/")[0] if "/" in selected_model else "gemini"
    )

    def switch_view(nav_target: str):
        """Cleanly switches views and synchronizes top navigation state."""
        st.session_state.top_nav = nav_target
        st.session_state.top_nav_bar_radio = nav_target

    # ------------------------------------------------------
    # TOP NAVIGATION BAR (Instant switching between views)
    # ------------------------------------------------------
    nav_options = [
        "💬 Workspace & Chat",
        "🐙 GitHub Accounts",
        "🤖 AI Models & Keys",
        "📁 Repos & Sessions",
        "👤 Profile & Settings",
    ]

    # Keep radio state in sync before rendering widget
    if "top_nav" in st.session_state:
        if st.session_state.get("top_nav_bar_radio") != st.session_state.top_nav:
            st.session_state.top_nav_bar_radio = st.session_state.top_nav

    cur_idx = (
        nav_options.index(st.session_state.top_nav)
        if st.session_state.top_nav in nav_options
        else 0
    )

    gh_user_str = (
        f"@{active_account['github_username']}"
        if active_account
        else "No GitHub Linked"
    )
    repo_str = st.session_state.active_repo or "No Repo Selected"
    model_short = selected_model_label.split(" (")[0]

    st.markdown(
        f"""
        <div style="display:flex; justify-content:space-between; align-items:center; background:#161b22; border:1px solid #30363d; border-radius:8px; padding:10px 18px; margin-bottom:12px;">
            <span style="font-weight:700; color:#58a6ff; font-size:16px;">⚡ AI Cloud Workspace</span>
            <div style="display:flex; gap:18px; font-size:13px; color:#8b949e;">
                <span>🐙 GitHub: <strong style="color:#f0f6fc;">{gh_user_str}</strong></span>
                <span>🤖 Model: <strong style="color:#f0f6fc;">{model_short}</strong></span>
                <span>📁 Repo: <strong style="color:#f0f6fc;">{repo_str}</strong></span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    selected_nav = st.radio(
        "Top Navigation Bar",
        nav_options,
        index=cur_idx,
        horizontal=True,
        label_visibility="collapsed",
        key="top_nav_bar_radio",
    )
    if selected_nav != st.session_state.top_nav:
        switch_view(selected_nav)
        st.rerun()

    # ------------------------------------------------------
    # SIDEBAR: Profile, Quick Links, Sign Out
    # ------------------------------------------------------
    with st.sidebar:
        user_meta = getattr(user, "user_metadata", {}) or {}
        app_meta = getattr(user, "app_metadata", {}) or {}
        provider = app_meta.get("provider", "Email")
        user_name = (
            user_meta.get("full_name")
            or user_meta.get("user_name")
            or user.email.split("@")[0]
        )

        st.markdown(
            f"""
        <div class="profile-card">
            <div style="font-size: 16px; font-weight: bold; color: #f0f6fc;">👤 {user_name}</div>
            <div style="font-size: 12px; color: #8b949e; margin-bottom: 6px;">{user.email}</div>
            <span class="badge" style="background-color: #1f6feb;">Auth: {provider.capitalize()}</span>
        </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown("### 🧭 Navigation")
        for nav_item in nav_options:
            is_cur = st.session_state.top_nav == nav_item
            if st.button(
                f"{'👉 ' if is_cur else ''}{nav_item}",
                key=f"side_nav_{nav_item}",
                use_container_width=True,
            ):
                switch_view(nav_item)
                st.rerun()

        st.markdown("---")
        st.caption(f"**🐙 GitHub:** {gh_user_str}")
        st.caption(f"**🤖 Model:** {model_short}")
        st.caption(f"**📁 Repo:** {repo_str}")

        if st.session_state.current_session_id:
            if st.button(
                "⏹️ Leave Active Chat",
                key="side_leave_chat",
                use_container_width=True,
            ):
                st.session_state.current_session_id = None
                db.save_user_workspace_state(
                    user.id, user.email, current_session_id=""
                )
                st.rerun()

        if st.button("Sign Out", use_container_width=True):
            db.sign_out()
            st.session_state.user = None
            st.session_state.current_session_id = None
            st.session_state.auth_flash = None
            st.rerun()

    # ------------------------------------------------------
    # TAB 1: 🐙 GITHUB ACCOUNTS VIEW
    # ------------------------------------------------------
    if st.session_state.top_nav == "🐙 GitHub Accounts":
        st.markdown("### 🐙 GitHub Account Management")
        st.caption(
            "Connect multiple personal or work GitHub accounts. Personal Access Tokens (PATs) are verified with GitHub and encrypted with AES-128."
        )

        if "gh_alert" in st.session_state:
            st.success(st.session_state.pop("gh_alert"))

        if gh_accounts:
            st.markdown("#### Linked Accounts")

            for acc in gh_accounts:
                is_active = acc["id"] == st.session_state.active_gh_account_id
                acc_label_text = acc.get("account_label", "Personal")
                has_pat = bool(acc.get("github_token"))

                # Choose category badge style
                cat_class = "cat-badge-personal"
                if "work" in acc_label_text.lower() or "business" in acc_label_text.lower():
                    cat_class = "cat-badge-work"
                elif "open" in acc_label_text.lower():
                    cat_class = "cat-badge-opensource"
                elif "org" in acc_label_text.lower():
                    cat_class = "cat-badge-org"
                elif acc_label_text.lower() not in ["personal", "default github"]:
                    cat_class = "cat-badge-custom"

                card_style = (
                    "border: 1px solid #238636; background: #0d1a10;"
                    if is_active
                    else "border: 1px solid #30363d; background: #161b22;"
                )
                badge_text = (
                    '<span class="badge" style="background-color:#238636;">● Active Account</span>'
                    if is_active
                    else '<span class="badge" style="background-color:#30363d; color:#8b949e;">Inactive</span>'
                )
                st.markdown(
                    f"""
                    <div style="{card_style} border-radius: 8px; padding: 14px; margin-bottom: 12px;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <div>
                                <span style="font-size:16px; font-weight:700; color:#f0f6fc;">@{acc['github_username']}</span>
                                &nbsp; <span class="{cat_class}">{acc_label_text}</span>
                            </div>
                            <span>{badge_text}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                if has_pat:
                    col_a, col_b, col_c = st.columns([2, 1, 1])
                    with col_a:
                        st.markdown(
                            f"**Masked PAT:** `{db.mask_secret(acc['github_token'])}`"
                        )
                        with st.expander("📋 Copy PAT"):
                            st.code(acc["github_token"], language="text")
                    with col_b:
                        if not is_active:
                            if st.button(
                                "⭐ Set as Active",
                                key=f"btn_activate_{acc['id']}",
                                use_container_width=True,
                            ):
                                st.session_state.active_gh_account_id = acc["id"]
                                db.save_user_workspace_state(
                                    user.id,
                                    user.email,
                                    active_gh_account_id=acc["id"],
                                )
                                st.toast(
                                    f"🐙 Switched to @{acc['github_username']}!",
                                    icon="✅",
                                )
                                st.rerun()
                        else:
                            if st.button(
                                "📁 Open Repositories",
                                key=f"btn_repos_{acc['id']}",
                                use_container_width=True,
                            ):
                                switch_view("📁 Repos & Sessions")
                                st.rerun()
                    with col_c:
                        if st.button(
                            "🗑️ Remove",
                            key=f"btn_del_acc_{acc['id']}",
                            use_container_width=True,
                        ):
                            db.delete_github_account(
                                user.id, acc["id"], user.email
                            )
                            if st.session_state.active_gh_account_id == acc["id"]:
                                st.session_state.active_gh_account_id = None
                            fetch_user_github_repos.clear()
                            st.session_state["gh_alert"] = f"🗑️ GitHub account @{acc['github_username']} removed."
                            st.rerun()
                else:
                    # Account exists but has no PAT attached yet
                    st.warning(
                        f"⚠️ **@{acc['github_username']}** has no Personal Access Token attached. To list and clone your repositories, attach a PAT below."
                    )
                    pat_in_col, pat_btn_col = st.columns([3, 1])
                    with pat_in_col:
                        inline_tok = st.text_input(
                            f"Paste Personal Access Token for @{acc['github_username']}",
                            type="password",
                            placeholder="ghp_...",
                            key=f"inline_tok_input_{acc['id']}",
                            label_visibility="collapsed",
                        )
                    with pat_btn_col:
                        attach_ready = bool(inline_tok.strip())
                        if st.button(
                            "Attach & Verify PAT",
                            key=f"btn_attach_tok_{acc['id']}",
                            disabled=not attach_ready,
                            use_container_width=True,
                            help="Enter token to attach" if not attach_ready else "Verify and attach PAT",
                        ):
                            with st.spinner("Verifying token with GitHub API..."):
                                v = db.verify_github_credentials(inline_tok.strip(), acc['github_username'])
                            if v["valid"]:
                                db.attach_pat_to_account(user.id, acc["id"], inline_tok.strip(), user.email)
                                fetch_user_github_repos.clear()
                                st.session_state["gh_alert"] = f"🐙 Verified and attached PAT for @{v['login']}!"
                                st.rerun()
                            else:
                                st.error(v["error"])

                    # Allow deleting an unlinked account
                    del_np_col1, del_np_col2 = st.columns([3, 1])
                    with del_np_col2:
                        if st.button("🗑️ Remove Account", key=f"btn_del_no_pat_{acc['id']}", use_container_width=True):
                            db.delete_github_account(user.id, acc["id"], user.email)
                            if st.session_state.active_gh_account_id == acc["id"]:
                                st.session_state.active_gh_account_id = None
                            fetch_user_github_repos.clear()
                            st.session_state["gh_alert"] = f"🗑️ GitHub account @{acc['github_username']} removed."
                            st.rerun()

                st.markdown("---")
        else:
            st.info(
                "💡 No GitHub accounts connected yet. Connect your GitHub account below to search repos and run projects."
            )

        st.markdown("#### ➕ Connect Another GitHub Account")
        st.markdown(
            "💡 *Need a token?* [🔗 Open Pre-configured GitHub Token Page](https://github.com/settings/tokens/new?scopes=repo,read:user&description=AI+Cloud+Workspace) *(pre-configures required `repo` scope on GitHub)*"
        )

        cat_col1, cat_col2 = st.columns(2)
        with cat_col1:
            cat_choice = st.selectbox(
                "Account Category (Optional)",
                ["Personal", "Work / Business", "Open Source", "Organization", "Custom Label..."],
                index=0,
                key="conn_gh_cat_select",
                help="Categorize this account to group personal and business repos",
            )
        with cat_col2:
            if cat_choice == "Custom Label...":
                custom_lbl = st.text_input(
                    "Custom Category Name",
                    placeholder="e.g. Freelance, Client Work",
                    key="conn_gh_cat_custom",
                )
                chosen_label = custom_lbl.strip() or "Custom"
            else:
                chosen_label = cat_choice

        f_col1, f_col2 = st.columns(2)
        with f_col1:
            new_gh_username = st.text_input(
                "GitHub Username *",
                placeholder="e.g. laxjovial",
                key="conn_gh_username_input",
            )
        with f_col2:
            new_gh_token = st.text_input(
                "Personal Access Token (PAT) *",
                type="password",
                help="Requires 'repo' scope from github.com/settings/tokens",
                placeholder="ghp_...",
                key="conn_gh_token_input",
            )

        is_ready_to_connect = bool(new_gh_username.strip() and new_gh_token.strip())

        connect_submitted = st.button(
            "✅ Verify & Connect GitHub Account",
            type="primary",
            disabled=not is_ready_to_connect,
            use_container_width=True,
            key="btn_connect_github_acc",
            help="Fill in both Username and Personal Access Token to connect" if not is_ready_to_connect else "Verify and connect your account",
        )

        # Seamless Enter Key navigation: Enter in username jumps to PAT; Enter in PAT clicks Connect if both ready
        st.html(
            """
            <div id="gh-connect-marker"></div>
            <script>
            (function() {
                function bindEnter() {
                    const uInput = document.querySelector('input[aria-label="GitHub Username *"]');
                    const tInput = document.querySelector('input[aria-label="Personal Access Token (PAT) *"]');
                    const btn = document.querySelector('button:has(div:contains("Verify & Connect"))') || 
                                Array.from(document.querySelectorAll('button')).find(b => b.innerText.includes("Connect GitHub"));
                    
                    if (uInput && !uInput.dataset.enterBound) {
                        uInput.dataset.enterBound = "true";
                        uInput.addEventListener('keydown', function(e) {
                            if (e.key === 'Enter') {
                                e.preventDefault();
                                if (tInput && !tInput.value.trim()) {
                                    tInput.focus();
                                } else if (btn && !btn.disabled) {
                                    btn.click();
                                }
                            }
                        });
                    }
                    if (tInput && !tInput.dataset.enterBound) {
                        tInput.dataset.enterBound = "true";
                        tInput.addEventListener('keydown', function(e) {
                            if (e.key === 'Enter') {
                                e.preventDefault();
                                if (uInput && !uInput.value.trim()) {
                                    uInput.focus();
                                } else if (btn && !btn.disabled) {
                                    btn.click();
                                }
                            }
                        });
                    }
                }
                setTimeout(bindEnter, 250);
                setTimeout(bindEnter, 800);
            })();
            </script>
            """
        )

        if connect_submitted:
            with st.spinner("Verifying credentials with GitHub API..."):
                val_result = db.verify_github_credentials(
                    new_gh_token.strip(), new_gh_username.strip()
                )
            if not val_result["valid"]:
                st.error(val_result["error"])
            else:
                real_login = val_result["login"]
                db.add_github_account(
                    user.id,
                    chosen_label,
                    real_login,
                    new_gh_token.strip(),
                    user.email,
                )
                fetch_user_github_repos.clear()
                st.toast(
                    f"🐙 Verified and connected @{real_login} ({val_result.get('public_repos', 0)} public repos)!",
                    icon="✅",
                )
                st.rerun()


    # ------------------------------------------------------
    # TAB 2: 🤖 AI MODELS & KEYS VIEW
    # ------------------------------------------------------
    elif st.session_state.top_nav == "🤖 AI Models & Keys":
        st.markdown("### 🤖 AI Models & API Key Management (BYOK)")
        st.caption(
            "Bring Your Own Key (BYOK) for direct provider access with zero markups. All keys are encrypted with AES-128."
        )

        if "key_alert" in st.session_state:
            st.success(st.session_state.pop("key_alert"))

        st.markdown("#### 1. Select Active AI Model & Reasoning Effort")
        m_col1, m_col2 = st.columns([3, 2])
        with m_col1:
            selected_model_idx = list(model_groups.keys()).index(
                selected_model_label
            )
            new_model_label = st.selectbox(
                "Active AI Model",
                list(model_groups.keys()),
                index=selected_model_idx,
                help="Choose the AI engine for code generation, terminal commands, and debugging",
            )
            if new_model_label != selected_model_label:
                st.session_state.selected_model_label = new_model_label
                db.save_user_workspace_state(
                    user.id, user.email, active_model=new_model_label
                )
                st.toast(
                    f"🤖 Switched to {new_model_label.split('(')[0]}!", icon="✅"
                )
                st.rerun()

        with m_col2:
            reasoning_levels = [
                "Auto / Standard",
                "Low (Fast Thinking)",
                "Medium (Balanced)",
                "High (Deep Multi-Step)",
            ]
            if "reasoning_effort" not in st.session_state:
                st.session_state.reasoning_effort = "Auto / Standard"

            cur_re_idx = 0
            for idx, r_opt in enumerate(reasoning_levels):
                if st.session_state.reasoning_effort.split(" ")[0].lower() in r_opt.lower():
                    cur_re_idx = idx
                    break

            new_re = st.selectbox(
                "🧠 Reasoning / Thinking Effort",
                reasoning_levels,
                index=cur_re_idx,
                help="Controls thinking budget / reasoning effort for models with reasoning capabilities (Claude 3.7 Sonnet, Gemini 2.5 / 2.0 Thinking, OpenAI o3-mini / o1, DeepSeek R1).",
            )
            if new_re != st.session_state.reasoning_effort:
                st.session_state.reasoning_effort = new_re
                st.toast(f"🧠 Reasoning effort set to: {new_re}", icon="✅")

        cur_prov = model_groups[new_model_label].split("/")[0]
        if user_keys.get(cur_prov):
            st.markdown(
                f'<span class="status-badge-green">● {cur_prov.capitalize()} API Key is Configured & Ready</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<span class="status-badge-red">○ Missing {cur_prov.capitalize()} API Key (Add below)</span>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("#### 2. Configured AI Keys (BYOK)")
        all_providers = [
            ("gemini", "Google Gemini", "https://aistudio.google.com/apikey"),
            ("anthropic", "Anthropic Claude", "https://console.anthropic.com/"),
            ("openai", "OpenAI GPT-4o", "https://platform.openai.com/api-keys"),
            ("deepseek", "DeepSeek", "https://platform.deepseek.com/"),
        ]

        grid_cols = st.columns(2)
        for i, (p_id, p_title, p_link) in enumerate(all_providers):
            with grid_cols[i % 2]:
                stored_key = user_keys.get(p_id)
                has_k = bool(stored_key)
                badge_html = (
                    '<span class="status-badge-green">● Configured (AES-128)</span>'
                    if has_k
                    else '<span class="status-badge-red">○ Not Added</span>'
                )
                st.markdown(
                    f"""
                    <div style="background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px; margin-bottom:12px;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <strong style="color:#f0f6fc; font-size:15px;">🔑 {p_title}</strong>
                            {badge_html}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if has_k:
                    k_col1, k_col2 = st.columns([3, 1])
                    with k_col1:
                        st.markdown(
                            f"**Masked:** `{db.mask_secret(stored_key)}`"
                        )
                    with k_col2:
                        if st.button(
                            "🗑️ Delete",
                            key=f"del_key_btn_{p_id}",
                            use_container_width=True,
                        ):
                            db.delete_user_api_key(user.id, p_id, user.email)
                            st.session_state["key_alert"] = f"🗑️ {p_title} API key deleted."
                            st.rerun()
                    with st.expander(f"📋 Copy {p_title} Key"):
                        st.code(stored_key, language="text")
                        st.caption("Click the copy button on the top right of the code box to copy.")
                else:
                    st.caption(f"Need a key? [Get {p_title} Key]({p_link})")

        st.markdown("---")
        st.markdown("#### 3. Add or Update API Key")
        with st.form("save_byok_key_form", clear_on_submit=True):
            f_col1, f_col2 = st.columns([1, 2])
            with f_col1:
                key_provider = st.selectbox(
                    "Provider", ["gemini", "anthropic", "openai", "deepseek"]
                )
            with f_col2:
                key_val_input = st.text_input(
                    "API Key",
                    type="password",
                    placeholder=f"Paste {key_provider.capitalize()} API key...",
                )
            submit_save_key = st.form_submit_button(
                "Save Key Securely", type="primary", use_container_width=True
            )
            if submit_save_key:
                if key_val_input.strip():
                    db.save_user_api_key(
                        user.id,
                        key_provider,
                        key_val_input.strip(),
                        user.email,
                    )
                    st.session_state["key_alert"] = f"🔒 {key_provider.capitalize()} API key saved and encrypted successfully!"
                    st.rerun()
                else:
                    st.warning("Please enter a valid API key.")

    # ------------------------------------------------------
    # TAB 3: 📁 REPOSITORIES & SESSIONS VIEW
    # ------------------------------------------------------
    elif st.session_state.top_nav == "📁 Repos & Sessions":
        st.markdown("### 📁 Repositories & Past Chat Sessions")
        st.caption(
            "Browse your GitHub repositories from the dropdown menu, search dynamically by typing, or resume any past coding session."
        )

        st.markdown("#### 1. Select Target Repository")

        # Active GitHub account status banner
        acc_name = f"@{active_account['github_username']}" if active_account else "None"
        has_active_pat = bool(active_gh_token)

        st.markdown(
            f"""
            <div style="background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px 16px; margin-bottom:12px; display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <span style="font-size:15px; font-weight:600; color:#f0f6fc;">🐙 Active GitHub Account: <b>{acc_name}</b></span>
                    <div style="font-size:12px; color:#8b949e; margin-top:3px;">
                        {'<span class="status-badge-green">● PAT Connected & Verified (Repositories Ready)</span>' if has_active_pat else '<span class="status-badge-red">⚠️ No PAT Attached (Attach below to populate repository dropdown)</span>'}
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Inline PAT attach if current active account has no token
        if not has_active_pat and active_account:
            st.info(f"💡 Attach a Personal Access Token for **@{active_account['github_username']}** to load all your private and public repositories into the dropdown menu.")
            p_col1, p_col2 = st.columns([3, 1])
            with p_col1:
                repo_tab_pat = st.text_input(
                    "Paste PAT",
                    type="password",
                    placeholder="ghp_...",
                    key="repo_tab_inline_pat_input",
                    label_visibility="collapsed",
                )
            with p_col2:
                can_attach_repo_pat = bool(repo_tab_pat.strip())
                if st.button(
                    "Attach PAT & Load Repos",
                    key="btn_repo_tab_attach_pat",
                    disabled=not can_attach_repo_pat,
                    use_container_width=True,
                    help="Enter token to load repos" if not can_attach_repo_pat else "Verify and load repositories",
                ):
                    with st.spinner("Verifying token with GitHub API..."):
                        v = db.verify_github_credentials(repo_tab_pat.strip(), active_account['github_username'])
                    if v["valid"]:
                        db.attach_pat_to_account(user.id, active_account["id"], repo_tab_pat.strip(), user.email)
                        fetch_user_github_repos.clear()
                        st.toast("🐙 Verified & connected! Loading repositories...", icon="✅")
                        st.rerun()
                    else:
                        st.error(v["error"])

        # Fetch repositories if token available
        user_repos = (
            fetch_user_github_repos(active_gh_token) if active_gh_token else []
        )

        # Header with Refresh button
        col_hdr1, col_hdr2 = st.columns([4, 1])
        with col_hdr1:
            st.markdown("##### 📋 Repository Dropdown & Dynamic Search")
        with col_hdr2:
            if active_gh_token:
                if st.button("🔄 Refresh Repos", key="btn_refresh_repos_list", use_container_width=True, help="Re-fetch fresh list of repositories from GitHub"):
                    fetch_user_github_repos.clear()
                    st.toast("🔄 Refreshed repository list from GitHub!", icon="🐙")
                    st.rerun()

        # Always construct and render the Dropdown Menu
        sessions = db.list_user_sessions(user.id, user.email)
        repo_input = ""

        if user_repos:
            dropdown_options = (
                [f"-- 📋 Browse all repositories ({len(user_repos)} available) --"]
                + user_repos
                + ["➕ Custom / Other External Repository..."]
            )
            default_idx = 0
            if st.session_state.active_repo in user_repos:
                default_idx = dropdown_options.index(st.session_state.active_repo)

            selected_repo_opt = st.selectbox(
                "Click dropdown to browse all repositories, or type to filter dynamically",
                dropdown_options,
                index=default_idx,
                help="Click the dropdown to browse all repos, or type to filter (e.g. 'g' -> 'gr' -> 'green...')",
                key="active_repo_dropdown_select",
            )

            if selected_repo_opt == "➕ Custom / Other External Repository...":
                repo_input = st.text_input(
                    "Enter Repository (owner/repo)",
                    value=""
                    if st.session_state.active_repo in user_repos
                    else (st.session_state.active_repo or ""),
                    placeholder="e.g. facebook/react",
                    key="custom_repo_external_input",
                ).strip()
                st.session_state.active_repo = repo_input
            elif not selected_repo_opt.startswith("--"):
                repo_input = selected_repo_opt
                st.session_state.active_repo = selected_repo_opt
            else:
                repo_input = st.session_state.active_repo or ""
        else:
            # Fallback dropdown menu with past sessions and manual entry
            past_repo_names = list(dict.fromkeys([s["repo_name"] for s in sessions if s.get("repo_name")]))
            fallback_options = (
                ["-- 📋 Select a Repository --"]
                + past_repo_names
                + ["➕ Enter Repository Manually..."]
            )
            selected_fallback = st.selectbox(
                "Repository Dropdown Menu",
                fallback_options,
                index=0,
                key="fallback_repo_dropdown_select",
                help="Select from past repositories or enter manually",
            )
            if selected_fallback == "➕ Enter Repository Manually..." or selected_fallback.startswith("--"):
                repo_input = st.text_input(
                    "GitHub Repository (owner/repo)",
                    value=st.session_state.active_repo or "",
                    placeholder="e.g. laxjovial/ai-cloud-workspace",
                    key="manual_repo_fallback_input",
                ).strip()
                st.session_state.active_repo = repo_input
            else:
                repo_input = selected_fallback
                st.session_state.active_repo = selected_fallback

        is_valid_repo_target = bool(
            repo_input
            and not repo_input.startswith("--")
            and "/" in repo_input
        )

        if st.button(
            "➕ Start New Chat Session on Selected Repo",
            type="primary",
            disabled=not is_valid_repo_target,
            use_container_width=True,
            key="btn_start_new_repo_session",
            help="Select or enter a valid repository (e.g. owner/repo) above to start" if not is_valid_repo_target else f"Start coding session on {repo_input}",
        ):
            new_session = db.create_chat_session(
                user_id=user.id,
                repo_name=repo_input.strip(),
                repo_url=f"https://github.com/{repo_input.strip()}",
                title=f"Task on {repo_input.strip()} ({datetime.now().strftime('%b %d, %H:%M')})",
                model_used=selected_model,
                email=user.email,
            )
            if new_session:
                st.session_state.current_session_id = new_session["id"]
                st.session_state.active_repo = repo_input.strip()
                switch_view("💬 Workspace & Chat")
                db.save_user_workspace_state(
                    user.id,
                    user.email,
                    active_repo=repo_input.strip(),
                    current_session_id=new_session["id"],
                )
                st.toast(
                    f"🚀 Session started on {repo_input.strip()}!",
                    icon="✅",
                )
                st.rerun()


        st.markdown("---")
        st.markdown("#### 2. Past Chat Sessions")
        sessions = db.list_user_sessions(user.id, user.email)
        if not sessions:
            st.info("No past chat sessions found. Start a new session above!")
        for s in sessions:
            is_active = s["id"] == st.session_state.current_session_id
            c_style = (
                "border: 1px solid #238636; background: #0d1a10;"
                if is_active
                else "border: 1px solid #30363d; background: #161b22;"
            )
            active_badge = (
                '<span class="badge" style="background-color:#238636;">Current Session</span>'
                if is_active
                else ""
            )
            st.markdown(
                f"""
                <div style="{c_style} border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <strong style="color:#f0f6fc; font-size:15px;">💬 {s['title']}</strong><br/>
                            <span style="font-size:12px; color:#8b949e;">📁 Repo: <b>{s['repo_name']}</b> | 🤖 Model: <b>{s.get('model_used', 'AI Model').split('/')[-1]}</b></span>
                        </div>
                        <div>
                            {active_badge}
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            sc1, sc2 = st.columns([4, 1])
            with sc1:
                btn_lbl = (
                    "👉 Currently Active (Open Chat)"
                    if is_active
                    else "▶️ Resume This Chat"
                )
                if st.button(
                    btn_lbl,
                    key=f"resume_sess_{s['id']}",
                    use_container_width=True,
                ):
                    st.session_state.current_session_id = s["id"]
                    st.session_state.active_repo = s["repo_name"]
                    switch_view("💬 Workspace & Chat")
                    db.save_user_workspace_state(
                        user.id,
                        user.email,
                        active_repo=s["repo_name"],
                        current_session_id=s["id"],
                    )
                    st.rerun()
            with sc2:
                if st.button(
                    "🗑️ Delete",
                    key=f"del_sess_{s['id']}",
                    use_container_width=True,
                ):
                    db.delete_chat_session(s["id"], user.id, user.email)
                    if st.session_state.current_session_id == s["id"]:
                        st.session_state.current_session_id = None
                        db.save_user_workspace_state(
                            user.id, user.email, current_session_id=""
                        )
                    st.toast("🗑️ Chat session deleted.", icon="🗑️")
                    st.rerun()
            st.markdown("---")

    # ------------------------------------------------------
    # TAB 4: 👤 PROFILE & SETTINGS VIEW
    # ------------------------------------------------------
    elif st.session_state.top_nav == "👤 Profile & Settings":
        st.markdown("### 👤 User Profile & Security Settings")
        st.markdown(
            f"""
            <div style="background:#161b22; border:1px solid #30363d; border-radius:8px; padding:20px; margin-bottom:16px;">
                <h4 style="color:#f0f6fc; margin-top:0;">{user_name}</h4>
                <p style="color:#8b949e; margin-bottom:8px;">📧 Email: <b>{user.email}</b></p>
                <p style="color:#8b949e; margin-bottom:8px;">🔐 Authentication: <b>{provider.capitalize()}</b></p>
                <p style="color:#8b949e; margin-bottom:8px;">🆔 User ID: <code>{user.id}</code></p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("#### 🔒 Security & Privacy Architecture")
        st.markdown(
            """
            * 🛡️ **AES-128 Encryption**: All BYOK keys and GitHub PATs are encrypted at the application layer prior to storage.
            * 🙈 **Password Masking**: Credentials display only the first 3 and last 2 characters (e.g. `AIz••••••••••8x`).
            * 💾 **Dual-Layer Persistence**: State is preserved across server restarts, browser reloads, and logouts.
            * ☁️ **Isolated Cloud Sandboxes**: All terminal execution and web dev servers run in isolated Linux environments via E2B.
            """
        )

        if st.button(
            "Sign Out of Account", type="primary", use_container_width=True
        ):
            db.sign_out()
            st.session_state.user = None
            st.session_state.current_session_id = None
            st.session_state.auth_flash = None
            st.rerun()

    # ------------------------------------------------------
    # TAB 5: 💬 WORKSPACE & CHAT VIEW (Active Arena)
    # ------------------------------------------------------
    elif st.session_state.top_nav == "💬 Workspace & Chat":
        if not st.session_state.current_session_id:
            st.markdown("### ⚡ AI Cloud Workspace Arena")
            if st.session_state.active_repo:
                st.info(f"📁 Target repository selected: **{st.session_state.active_repo}**")
                if st.button(
                    f"💬 Open / Start Chat on {st.session_state.active_repo}",
                    type="primary",
                    use_container_width=True,
                    key="btn_quick_open_active_repo",
                ):
                    recent_sessions = db.list_user_sessions(user.id, user.email)
                    matching = [s for s in recent_sessions if s.get("repo_name") == st.session_state.active_repo]
                    if matching:
                        sess_id = matching[0]["id"]
                    else:
                        new_s = db.create_chat_session(
                            user_id=user.id,
                            repo_name=st.session_state.active_repo,
                            repo_url=f"https://github.com/{st.session_state.active_repo}",
                            title=f"Chat on {st.session_state.active_repo}",
                            model_used=selected_model,
                            email=user.email,
                        )
                        sess_id = new_s["id"]
                    st.session_state.current_session_id = sess_id
                    db.save_user_workspace_state(
                        user.id,
                        user.email,
                        active_repo=st.session_state.active_repo,
                        current_session_id=sess_id,
                    )
                    st.rerun()
            else:
                st.info(
                    "👈 No active chat session loaded. Pick a repository below, select one from the **📁 Repos & Sessions** tab, or resume a recent chat!"
                )

            w_col1, w_col2 = st.columns(2)
            with w_col1:
                st.markdown(
                    """
                    <div style="background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px; margin-bottom:14px;">
                        <h4 style="margin-top:0; color:#58a6ff;">🐙 Active GitHub Account</h4>
                        <p style="color:#8b949e; font-size:14px;">Select or change the GitHub account used for git cloning, pushing, and pull requests.</p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if st.button(
                    "🐙 Manage GitHub Accounts",
                    key="quick_btn_gh",
                    use_container_width=True,
                ):
                    switch_view("🐙 GitHub Accounts")
                    st.rerun()

            with w_col2:
                st.markdown(
                    """
                    <div style="background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px; margin-bottom:14px;">
                        <h4 style="margin-top:0; color:#58a6ff;">🤖 Active AI Model & Keys</h4>
                        <p style="color:#8b949e; font-size:14px;">Configure Gemini, Claude, OpenAI, or DeepSeek API keys with zero host markup.</p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if st.button(
                    "🤖 Configure AI Models & Keys",
                    key="quick_btn_ai",
                    use_container_width=True,
                ):
                    switch_view("🤖 AI Models & Keys")
                    st.rerun()

            st.markdown("---")
            st.markdown("#### 💬 Recent Chat Sessions")
            recent_sessions = db.list_user_sessions(user.id, user.email)
            if recent_sessions:
                for s in recent_sessions[:5]:
                    r_col1, r_col2 = st.columns([4, 1])
                    with r_col1:
                        st.markdown(
                            f"**{s['title']}** &nbsp; (`{s['repo_name']}`)"
                        )
                    with r_col2:
                        if st.button(
                            "▶️ Resume",
                            key=f"ws_resume_{s['id']}",
                            use_container_width=True,
                        ):
                            st.session_state.current_session_id = s["id"]
                            st.session_state.active_repo = s["repo_name"]
                            db.save_user_workspace_state(
                                user.id,
                                user.email,
                                active_repo=s["repo_name"],
                                current_session_id=s["id"],
                            )
                            st.rerun()
            else:
                st.caption(
                    "No chat sessions yet. Click below to start your first session!"
                )
                if st.button(
                    "📁 Pick a Repository to Start",
                    key="btn_pick_repo_start",
                    type="primary",
                    use_container_width=True,
                ):
                    switch_view("📁 Repos & Sessions")
                    st.rerun()
            return

        # Load active session messages
        messages = db.get_session_messages(
            st.session_state.current_session_id, user.id, user.email
        )

        head_col1, head_col2, head_col3 = st.columns([4, 1, 1])
        with head_col1:
            st.markdown(
                f"### 🚀 Working on: `{st.session_state.active_repo}` &nbsp; <span class='badge'>{selected_model_label.split('(')[0]}</span>",
                unsafe_allow_html=True,
            )
        with head_col2:
            if st.button(
                "⏹️ Leave Chat",
                key="leave_current_chat",
                help="Leave this chat session and return to the session menu",
                use_container_width=True,
            ):
                st.session_state.current_session_id = None
                db.save_user_workspace_state(
                    user.id, user.email, current_session_id=""
                )
                st.rerun()
        with head_col3:
            if st.button(
                "🗑️ Delete Chat",
                key="del_current_chat",
                help="Delete this chat session and its history permanently",
                use_container_width=True,
            ):
                db.delete_chat_session(
                    st.session_state.current_session_id, user.id, user.email
                )
                st.session_state.current_session_id = None
                db.save_user_workspace_state(
                    user.id, user.email, current_session_id=""
                )
                st.toast("🗑️ Chat session deleted.", icon="🗑️")
                st.rerun()

        # Render previous messages
        for msg in messages:
            with st.chat_message(msg["sender"]):
                st.markdown(msg["content"])
                if msg.get("terminal_output"):
                    with st.expander("🖥️ Terminal & Test Execution Output"):
                        st.markdown(
                            f"<div class='terminal-box'>{msg['terminal_output']}</div>",
                            unsafe_allow_html=True,
                        )

        # QUICK ACTION SHORTCUT BUTTONS
        st.markdown("##### ⚡ Quick Actions")
        q1, q2, q3, q4 = st.columns(4)

        prompt_to_run = None

        with q1:
            if st.button("🔍 Audit Codebase", use_container_width=True):
                prompt_to_run = "Inspect the repository architecture and list all core files and dependencies."
        with q2:
            if st.button("🧪 Run Test Suite", use_container_width=True):
                prompt_to_run = "Detect the testing framework, execute all tests in the terminal, and report results."
        with q3:
            if st.button("📱 Start Web Preview", use_container_width=True):
                prompt_to_run = "Detect the web framework and run the project dev server in the background using start_dev_server so I can preview it on mobile."
        with q4:
            if st.button("🐙 Check Git Diffs", use_container_width=True):
                prompt_to_run = (
                    "Run git status and git diff to check all modified files."
                )

        # Chat Input
        user_prompt = st.chat_input(
            "Instruct the AI (e.g., 'Run npm test', 'Fix the failing tests', or 'Run this project')..."
        )

        final_prompt = prompt_to_run or user_prompt

        if final_prompt:
            active_api_key = user_keys.get(provider_name)

            if not active_api_key:
                st.error(
                    f"Missing API key for provider '{provider_name.capitalize()}'. Please configure your key in the '🤖 AI Models & Keys' tab at the top."
                )
                return

            if not active_gh_token:
                st.error(
                    "Missing GitHub token for active account. Please connect a GitHub token in the '🐙 GitHub Accounts' tab at the top to clone and inspect repos."
                )
                return

            # 1. Save and display User Message
            db.add_message(
                st.session_state.current_session_id,
                "user",
                final_prompt,
                user_id=user.id,
                email=user.email,
            )
            with st.chat_message("user"):
                st.markdown(final_prompt)

            # 2. Run Autonomous AI Execution in Sandbox
            with st.chat_message("assistant"):
                status_box = st.status(
                    "Initializing Sandbox and Repository...", expanded=True
                )

                sandbox = WorkspaceSandbox(
                    repo_name=st.session_state.active_repo,
                    github_token=active_gh_token,
                )

                status_box.update(
                    label="Cloning repository into cloud sandbox..."
                )
                boot_res = sandbox.start()

                if not boot_res.get("success"):
                    status_box.update(
                        label="Failed to start sandbox", state="error"
                    )
                    st.error(boot_res.get("error"))
                    return

                def update_status(text: str):
                    status_box.update(label=text)

                history_payload = []
                for m in messages:
                    history_payload.append(
                        {
                            "role": "user"
                            if m["sender"] == "user"
                            else "assistant",
                            "content": m["content"],
                        }
                    )
                history_payload.append(
                    {"role": "user", "content": final_prompt}
                )

                reasoning_val = st.session_state.get("reasoning_effort", "Auto / Standard").split(" ")[0].lower()
                try:
                    result = agent.run_agent_turn(
                        messages=history_payload,
                        model=selected_model,
                        api_key=active_api_key,
                        sandbox=sandbox,
                        status_callback=update_status,
                        reasoning_effort=reasoning_val if reasoning_val in ["low", "medium", "high"] else None,
                    )

                    if not result.get("success"):
                        if result.get("status") == "incomplete_step_limit":
                            status_box.update(label="⚠️ AI Execution reached step limit (incomplete)", state="error")
                            st.warning(result.get("content", "Task reached maximum allowed steps before completion."))
                        else:
                            status_box.update(label="❌ AI Execution encountered an issue", state="error")
                            st.error(result.get("error", "An error occurred during AI execution."))
                    else:
                        status_box.update(label="Execution complete!", state="complete")

                    reply_text = result.get("content", "")
                    terminal_logs = result.get("terminal_logs", "")

                    if reply_text:
                        st.markdown(reply_text)
                    if terminal_logs:
                        with st.expander("🖥️ Terminal & Test Execution Output"):
                            st.markdown(
                                f"<div class='terminal-box'>{terminal_logs}</div>",
                                unsafe_allow_html=True,
                            )

                    if reply_text or terminal_logs:
                        # Save assistant message to hybrid database & local store
                        db.add_message(
                            st.session_state.current_session_id,
                            "assistant",
                            reply_text or "Task execution finished.",
                            terminal_output=terminal_logs,
                            user_id=user.id,
                            email=user.email,
                        )
                finally:
                    if "Preview URL: https://" not in terminal_logs:
                        sandbox.close()


# =========================================================
# 3. ROUTER
# =========================================================

if st.session_state.user is None:
    render_landing_and_auth()
else:
    render_workspace()

