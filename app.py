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
        color: white;
    }
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

# =========================================================
# 0. HANDLE OAUTH CALLBACK (SAFE & ROBUST)
# =========================================================

if "code" in st.query_params and not st.session_state.user:
    auth_code = st.query_params.get("code")
    st.query_params.clear()

    # Clear URL bar via JavaScript so stale codes are never re-evaluated
    st.html(
        "<script>window.history.replaceState({}, '', window.location.pathname);</script>"
    )

    try:
        with st.spinner("Completing authentication..."):
            oauth_res = db.exchange_oauth_code(auth_code)
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
    """Renders the main authenticated dashboard, sidebar controls, and chat arena."""
    user = st.session_state.user

    # Auto-link GitHub account if logged in via GitHub OAuth
    db.auto_link_oauth_github(user)

    # Fetch user API keys & profiles
    user_keys = db.get_user_api_keys(user.id)
    gh_accounts = db.get_user_github_accounts(user.id)

    # Determine active GitHub account
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

    active_gh_token = active_account.get("github_token") if active_account else None

    # ------------------------------------------------------
    # SIDEBAR: Profile, Multi-GitHub Accounts, BYOK, History
    # ------------------------------------------------------
    with st.sidebar:
        # USER PROFILE CARD
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

        if st.button("Sign Out", use_container_width=True):
            db.sign_out()
            st.session_state.user = None
            st.session_state.current_session_id = None
            st.session_state.auth_flash = None  # Clean logout without stale flash
            st.rerun()

        st.markdown("---")

        # --------------------------------------------------
        # MULTI-GITHUB ACCOUNT MANAGER
        # --------------------------------------------------
        st.markdown("### 🐙 GitHub Accounts")
        if gh_accounts:
            account_labels = [
                f"{a['account_label']} (@{a['github_username']})"
                for a in gh_accounts
            ]
            current_acc_index = (
                [a["id"] for a in gh_accounts].index(
                    st.session_state.active_gh_account_id
                )
                if st.session_state.active_gh_account_id
                in [a["id"] for a in gh_accounts]
                else 0
            )

            selected_acc_label = st.selectbox(
                "Active GitHub Account",
                account_labels,
                index=current_acc_index,
            )
            st.session_state.active_gh_account_id = gh_accounts[
                account_labels.index(selected_acc_label)
            ]["id"]
            active_account = gh_accounts[account_labels.index(selected_acc_label)]
            active_gh_token = active_account.get("github_token", "")

            st.caption(
                f"✅ Active: **@{active_account['github_username']}** ({active_account['account_label']})"
            )

            if active_gh_token:
                st.markdown(
                    f"**Masked PAT:** `{db.mask_secret(active_gh_token)}`"
                )
                with st.expander("📋 Copy PAT", expanded=False):
                    st.code(active_gh_token, language="text")

            if st.button(
                "🗑️ Remove Selected Account",
                key=f"del_gh_{active_account['id']}",
                use_container_width=True,
            ):
                db.delete_github_account(user.id, active_account["id"])
                st.session_state.active_gh_account_id = None
                st.toast("🗑️ GitHub account removed.", icon="🗑️")
                st.rerun()
        else:
            st.warning("⚠️ No GitHub account connected yet.")
            st.caption(
                "Connect a Personal Access Token below to allow the AI to clone repos and create PRs."
            )

        # Expander to add or switch GitHub account
        with st.expander("➕ Connect Another GitHub Account"):
            acc_label = st.text_input(
                "Account Label",
                placeholder="e.g. Work, Personal, OpenSource",
                key="new_gh_acc_label",
            )
            acc_username = st.text_input(
                "GitHub Username",
                placeholder="e.g. laxjovial",
                key="new_gh_acc_user",
            )
            acc_token = st.text_input(
                "Personal Access Token",
                type="password",
                help="Requires 'repo' scope",
                key="new_gh_acc_token",
            )
            if st.button("Connect Account", use_container_width=True):
                if acc_label and acc_token:
                    if db.add_github_account(
                        user.id,
                        acc_label,
                        acc_username or acc_label,
                        acc_token,
                    ):
                        st.toast("🐙 GitHub account connected successfully!", icon="✅")
                        st.success("GitHub account connected and encrypted!")
                        st.rerun()
                else:
                    st.warning("Please provide a label and token.")

        st.markdown("---")

        # --------------------------------------------------
        # MODEL CONFIGURATION & KEY STATUS
        # --------------------------------------------------
        st.markdown("### 🤖 Model Configuration")

        model_groups = {
            "Google Gemini 2.5 Pro (Recommended)": "gemini/gemini-2.5-pro",
            "Google Gemini 2.5 Flash (Fast)": "gemini/gemini-2.5-flash",
            "Google Gemini 1.5 Pro": "gemini/gemini-1.5-pro-latest",
            "Anthropic Claude 3.7 Sonnet": "anthropic/claude-3-7-sonnet-20250219",
            "Anthropic Claude 3.5 Haiku": "anthropic/claude-3-5-haiku-20241022",
            "OpenAI GPT-4o": "openai/gpt-4o",
            "OpenAI GPT-4o Mini": "openai/gpt-4o-mini",
            "DeepSeek Chat (V3)": "deepseek/deepseek-chat",
            "DeepSeek Reasoner (R1)": "deepseek/deepseek-reasoner",
        }

        selected_model_label = st.selectbox(
            "Active AI Model", list(model_groups.keys())
        )
        selected_model = model_groups[selected_model_label]

        # Check API key status for active model provider
        provider_name = (
            selected_model.split("/")[0] if "/" in selected_model else "gemini"
        )
        has_key = bool(user_keys.get(provider_name))

        if has_key:
            st.markdown(
                f'<span class="status-badge-green">● {provider_name.capitalize()} API Key Ready</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<span class="status-badge-red">○ Missing {provider_name.capitalize()} API Key (Add below)</span>',
                unsafe_allow_html=True,
            )

        # BYOK Expander
        with st.expander("⚙️ Manage AI Keys (BYOK)", expanded=not has_key):
            st.caption("Keys are encrypted with AES-128 and stored securely in Supabase.")
            if user_keys:
                st.markdown("##### 🔐 Stored API Keys")
                for prov, key in user_keys.items():
                    k_col1, k_col2 = st.columns([3, 1])
                    with k_col1:
                        st.markdown(f"**{prov.capitalize()}**: `{db.mask_secret(key)}`")
                    with k_col2:
                        if st.button("🗑️", key=f"del_key_{prov}", help=f"Delete {prov.capitalize()} key"):
                            db.delete_user_api_key(user.id, prov)
                            st.toast(f"🗑️ {prov.capitalize()} API key deleted.", icon="🗑️")
                            st.rerun()
                    with st.expander(f"📋 Copy {prov.capitalize()} Key", expanded=False):
                        st.code(key, language="text")
                st.markdown("---")

            st.markdown("##### Add or Update Key")
            byok_provider = st.selectbox(
                "Select Provider",
                ["gemini", "anthropic", "openai", "deepseek"],
            )
            key_val = st.text_input(
                f"{byok_provider.capitalize()} API Key",
                value="",
                type="password",
                placeholder=f"Paste {byok_provider.capitalize()} key...",
                key="byok_input_key",
            )
            if st.button("Save API Key", use_container_width=True):
                if key_val:
                    if db.save_user_api_key(user.id, byok_provider, key_val):
                        st.toast(f"🔒 {byok_provider.capitalize()} API key saved securely!", icon="✅")
                        st.success(f"{byok_provider.capitalize()} key saved and encrypted!")
                        st.rerun()
                else:
                    st.warning("Please enter an API key.")

        st.markdown("---")

        # Target Repository Setup with Dynamic Prefix Search
        st.markdown("### 📁 Target Repository")

        user_repos = fetch_user_github_repos(active_gh_token) if active_gh_token else []

        repo_input = ""
        if user_repos:
            search_options = ["-- Select or Search Repository --"] + user_repos + ["➕ Custom / Other Repository..."]
            
            default_idx = 0
            if st.session_state.active_repo in user_repos:
                default_idx = search_options.index(st.session_state.active_repo)

            selected_repo_opt = st.selectbox(
                "Search & Select Repository",
                search_options,
                index=default_idx,
                help="Type to search your repos dynamically (e.g. 'gre' -> 'green...')",
            )

            if selected_repo_opt == "➕ Custom / Other Repository...":
                repo_input = st.text_input(
                    "Enter Repository (owner/repo)",
                    value="" if st.session_state.active_repo in user_repos else (st.session_state.active_repo or ""),
                    placeholder="e.g. laxjovial/ai-cloud-workspace",
                )
                st.session_state.active_repo = repo_input
            elif selected_repo_opt != "-- Select or Search Repository --":
                repo_input = selected_repo_opt
                st.session_state.active_repo = repo_input
            else:
                repo_input = st.session_state.active_repo or ""
        else:
            repo_input = st.text_input(
                "GitHub Repository",
                value=st.session_state.active_repo or "",
                placeholder="username/repo-name (e.g. laxjovial/ai-cloud-workspace)",
                help="Format: username/repo-name",
            )
            st.session_state.active_repo = repo_input
            if not active_gh_token:
                st.caption(
                    "💡 Connect a GitHub PAT above to dynamically list and search all your repositories here."
                )

        if st.button(
            "➕ Start New Chat Session",
            type="primary",
            use_container_width=True,
        ):
            if repo_input and repo_input != "-- Select or Search Repository --" and "/" in repo_input:
                new_session = db.create_chat_session(
                    user_id=user.id,
                    repo_name=repo_input.strip(),
                    repo_url=f"https://github.com/{repo_input.strip()}",
                    title=f"Task on {repo_input.strip()} ({datetime.now().strftime('%b %d, %H:%M')})",
                    model_used=selected_model,
                )
                if new_session:
                    st.session_state.current_session_id = new_session["id"]
                    st.session_state.active_repo = repo_input.strip()
                    st.rerun()
            else:
                st.error("Please enter or select a valid repository (e.g. username/repo-name)")


        st.markdown("### 💬 Recent Sessions")
        sessions = db.list_user_sessions(user.id)
        if not sessions:
            st.caption("No active chat sessions. Start one above!")
        for s in sessions:
            is_active = s["id"] == st.session_state.current_session_id
            s_col1, s_col2 = st.columns([5, 1])
            with s_col1:
                btn_label = f"{'👉 ' if is_active else '💬 '}{s['title']}"
                if st.button(
                    btn_label, key=f"sess_{s['id']}", use_container_width=True
                ):
                    st.session_state.current_session_id = s["id"]
                    st.session_state.active_repo = s["repo_name"]
                    st.rerun()
            with s_col2:
                if st.button(
                    "🗑️",
                    key=f"del_{s['id']}",
                    help=f"Delete session: {s['title']}",
                ):
                    db.delete_chat_session(s["id"])
                    if st.session_state.current_session_id == s["id"]:
                        st.session_state.current_session_id = None
                    st.rerun()

    # ------------------------------------------------------
    # MAIN CHAT & EXECUTION AREA
    # ------------------------------------------------------
    if not st.session_state.current_session_id:
        st.info(
            "👈 Select a repository in the sidebar and click **'Start New Chat Session'** to begin."
        )
        return

    # Load active session messages
    messages = db.get_session_messages(st.session_state.current_session_id)

    head_col1, head_col2 = st.columns([5, 1])
    with head_col1:
        st.markdown(
            f"### 🚀 Working on: `{st.session_state.active_repo}` &nbsp; <span class='badge'>{selected_model_label.split('(')[0]}</span>",
            unsafe_allow_html=True,
        )
    with head_col2:
        if st.button(
            "🗑️ Delete Chat",
            key="del_current_chat",
            help="Delete this chat session and its history",
            use_container_width=True,
        ):
            db.delete_chat_session(st.session_state.current_session_id)
            st.session_state.current_session_id = None
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
            prompt_to_run = "Run git status and git diff to check all modified files."

    # Chat Input
    user_prompt = st.chat_input(
        "Instruct the AI (e.g., 'Run npm test', 'Fix the failing tests', or 'Run this project')..."
    )

    final_prompt = prompt_to_run or user_prompt

    if final_prompt:
        active_api_key = user_keys.get(provider_name)

        if not active_api_key:
            st.error(
                f"Missing API key for provider '{provider_name.capitalize()}'. Please save your key in the sidebar under '⚙️ Manage AI Keys (BYOK)'."
            )
            return

        if not active_gh_token:
            st.error(
                "Missing GitHub token for active account. Please connect a GitHub token in the sidebar to clone and inspect repos."
            )
            return

        # 1. Save and display User Message
        db.add_message(
            st.session_state.current_session_id, "user", final_prompt
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

            status_box.update(label="Cloning repository into cloud sandbox...")
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
                        "role": "user" if m["sender"] == "user" else "assistant",
                        "content": m["content"],
                    }
                )
            history_payload.append({"role": "user", "content": final_prompt})

            result = agent.run_agent_turn(
                messages=history_payload,
                model=selected_model,
                api_key=active_api_key,
                sandbox=sandbox,
                status_callback=update_status,
            )

            status_box.update(label="Execution complete!", state="complete")

            reply_text = result.get("content", "")
            terminal_logs = result.get("terminal_logs", "")

            st.markdown(reply_text)
            if terminal_logs:
                with st.expander("🖥️ Terminal & Test Execution Output"):
                    st.markdown(
                        f"<div class='terminal-box'>{terminal_logs}</div>",
                        unsafe_allow_html=True,
                    )

            # Save assistant message to Supabase
            db.add_message(
                st.session_state.current_session_id,
                "assistant",
                reply_text,
                terminal_output=terminal_logs,
            )


# =========================================================
# 3. ROUTER
# =========================================================

if st.session_state.user is None:
    render_landing_and_auth()
else:
    render_workspace()
