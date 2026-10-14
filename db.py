"""Database and Authentication module using Supabase with deterministic PKCE."""

import os
from typing import Any, Dict, List, Optional
import streamlit as st
from supabase import Client, create_client
from supabase_auth._sync.gotrue_client import generate_pkce_challenge

# Deterministic PKCE pair so OAuth code verifiers never get lost or out of sync across reruns
FIXED_PKCE_VERIFIER = "ai_cloud_workspace_pkce_verifier_secure_key_2026_unique"
FIXED_PKCE_CHALLENGE = generate_pkce_challenge(FIXED_PKCE_VERIFIER)


def get_supabase_client() -> Client:
    """Initializes or retrieves the Supabase client from Streamlit secrets or environment."""
    url = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL", ""))
    key = st.secrets.get("SUPABASE_KEY", os.getenv("SUPABASE_KEY", ""))

    if not url or not key:
        raise ValueError(
            "Supabase credentials not configured. Please set SUPABASE_URL and SUPABASE_KEY in .streamlit/secrets.toml"
        )

    return create_client(url, key)


# ==========================================
# AUTHENTICATION
# ==========================================


def sign_up_with_email(email: str, password: str) -> Dict[str, Any]:
    """Signs up a new user with email and password."""
    supabase = get_supabase_client()
    try:
        response = supabase.auth.sign_up({"email": email, "password": password})
        return {"success": True, "data": response}
    except Exception as e:
        return {"success": False, "error": str(e)}


def sign_in_with_email(email: str, password: str) -> Dict[str, Any]:
    """Logs in an existing user with email and password."""
    supabase = get_supabase_client()
    try:
        response = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        return {"success": True, "data": response}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_oauth_url(provider: str, redirect_to: str) -> Dict[str, Any]:
    """Generates a reliable OAuth URL using the deterministic PKCE challenge."""
    supabase = get_supabase_client()
    try:
        auth_url = (
            f"{supabase.auth._url}/authorize?provider={provider}"
            f"&redirect_to={redirect_to}"
            f"&code_challenge={FIXED_PKCE_CHALLENGE}"
            f"&code_challenge_method=s256"
        )
        return {"success": True, "url": auth_url}
    except Exception as e:
        return {"success": False, "error": str(e)}


def exchange_oauth_code(code: str) -> Dict[str, Any]:
    """Exchanges an authorization code using the matched PKCE verifier."""
    supabase = get_supabase_client()
    try:
        supabase.auth._storage.set_item(
            f"{supabase.auth._storage_key}-code-verifier", FIXED_PKCE_VERIFIER
        )
        response = supabase.auth.exchange_code_for_session(
            {"auth_code": code, "code_verifier": FIXED_PKCE_VERIFIER}
        )
        return {"success": True, "data": response}
    except Exception as e:
        return {"success": False, "error": str(e)}


def sign_out() -> bool:
    """Signs out the current user."""
    supabase = get_supabase_client()
    try:
        supabase.auth.sign_out()
        return True
    except Exception:
        return False


# ==========================================
# USER PROFILE & MULTI-GITHUB ACCOUNTS
# ==========================================


def get_user_profile(user_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves user profile data."""
    supabase = get_supabase_client()
    try:
        res = supabase.table("profiles").select("*").eq("id", user_id).execute()
        return res.data[0] if res.data else None
    except Exception:
        return None


def get_user_github_accounts(user_id: str) -> List[Dict[str, Any]]:
    """Fetches all connected GitHub accounts for a user."""
    supabase = get_supabase_client()
    try:
        res = (
            supabase.table("user_github_accounts")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []
    except Exception:
        profile = get_user_profile(user_id)
        if profile and profile.get("github_token"):
            return [
                {
                    "id": "default",
                    "account_label": "Primary GitHub",
                    "github_username": "Primary Account",
                    "github_token": profile["github_token"],
                    "is_active": True,
                }
            ]
        return []


def add_github_account(
    user_id: str, label: str, username: str, token: str
) -> bool:
    """Adds a new GitHub account connection for the user."""
    supabase = get_supabase_client()
    try:
        supabase.table("user_github_accounts").insert(
            {
                "user_id": user_id,
                "account_label": label,
                "github_username": username,
                "github_token": token,
                "is_active": True,
            }
        ).execute()
        return True
    except Exception:
        return update_github_token(user_id, token)


def delete_github_account(user_id: str, account_id: str) -> bool:
    """Removes a connected GitHub account."""
    supabase = get_supabase_client()
    try:
        supabase.table("user_github_accounts").delete().eq(
            "id", account_id
        ).eq("user_id", user_id).execute()
        return True
    except Exception:
        return False


def auto_link_oauth_github(user: Any) -> Optional[Dict[str, Any]]:
    """Checks if the user authenticated with GitHub and ensures it is registered as default."""
    if not user:
        return None
    user_metadata = getattr(user, "user_metadata", {}) or {}
    app_metadata = getattr(user, "app_metadata", {}) or {}
    provider = app_metadata.get("provider", "")
    identities = getattr(user, "identities", []) or []
    has_github_identity = (
        any(getattr(i, "provider", "") == "github" for i in identities)
        or provider == "github"
    )

    if has_github_identity:
        username = (
            user_metadata.get("user_name")
            or user_metadata.get("preferred_username")
            or "GitHub User"
        )
        existing = get_user_github_accounts(user.id)
        if not existing:
            add_github_account(user.id, "Default GitHub", username, "")
            return {"username": username, "label": "Default GitHub"}
    return None


def update_github_token(user_id: str, token: str) -> bool:
    """Updates the primary GitHub token in profiles."""
    supabase = get_supabase_client()
    try:
        supabase.table("profiles").update({"github_token": token}).eq(
            "id", user_id
        ).execute()
        return True
    except Exception:
        return False


def get_user_api_keys(user_id: str) -> Dict[str, str]:
    """Fetches all BYOK API keys configured for the user."""
    supabase = get_supabase_client()
    try:
        res = (
            supabase.table("user_api_keys")
            .select("provider, api_key")
            .eq("user_id", user_id)
            .execute()
        )
        keys = {}
        for item in res.data or []:
            keys[item["provider"]] = item["api_key"]
        return keys
    except Exception:
        return {}


def save_user_api_key(user_id: str, provider: str, api_key: str) -> bool:
    """Upserts an API key for a given provider (gemini, anthropic, openai, deepseek)."""
    supabase = get_supabase_client()
    try:
        supabase.table("user_api_keys").upsert(
            {"user_id": user_id, "provider": provider, "api_key": api_key},
            on_conflict="user_id,provider",
        ).execute()
        return True
    except Exception as e:
        st.error(f"Error saving API key: {e}")
        return False


# ==========================================
# CHAT SESSIONS & MESSAGES
# ==========================================


def list_user_sessions(user_id: str) -> List[Dict[str, Any]]:
    """Lists all chat sessions for a user, ordered by most recent."""
    supabase = get_supabase_client()
    try:
        res = (
            supabase.table("chat_sessions")
            .select("*")
            .eq("user_id", user_id)
            .order("updated_at", desc=True)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def create_chat_session(
    user_id: str, repo_name: str, repo_url: str, title: str, model_used: str
) -> Optional[Dict[str, Any]]:
    """Creates a new chat session for a repository."""
    supabase = get_supabase_client()
    try:
        res = (
            supabase.table("chat_sessions")
            .insert(
                {
                    "user_id": user_id,
                    "repo_name": repo_name,
                    "repo_url": repo_url,
                    "title": title,
                    "model_used": model_used,
                }
            )
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        st.error(f"Error creating session: {e}")
        return None


def delete_chat_session(session_id: str) -> bool:
    """Deletes a chat session and all associated messages."""
    supabase = get_supabase_client()
    try:
        supabase.table("chat_sessions").delete().eq("id", session_id).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting session: {e}")
        return False


def get_session_messages(session_id: str) -> List[Dict[str, Any]]:
    """Fetches all messages in a session in chronological order."""
    supabase = get_supabase_client()
    try:
        res = (
            supabase.table("chat_messages")
            .select("*")
            .eq("session_id", session_id)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def add_message(
    session_id: str,
    sender: str,
    content: str,
    terminal_output: Optional[str] = None,
) -> bool:
    """Inserts a new message into the chat session."""
    supabase = get_supabase_client()
    try:
        payload = {
            "session_id": session_id,
            "sender": sender,
            "content": content,
            "terminal_output": terminal_output,
        }
        supabase.table("chat_messages").insert(payload).execute()
        supabase.table("chat_sessions").update(
            {"updated_at": "now()"}
        ).eq("id", session_id).execute()
        return True
    except Exception as e:
        st.error(f"Error saving message: {e}")
        return False
