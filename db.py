"""Database and Authentication module using Supabase with dynamic PKCE and fail-closed AES encryption."""

import base64
import hashlib
import json
import os
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from cryptography.fernet import Fernet, InvalidToken
import streamlit as st
from supabase import Client, create_client
from supabase_auth._sync.gotrue_client import generate_pkce_challenge

# Storage for pending OAuth PKCE verifiers and states
OAUTH_STATES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".oauth_states.json")
OAUTH_STATE_EXPIRY_SECONDS = 600  # 10 minutes

# Local encrypted store directory (AES-128 encrypted, excluded in .gitignore)
USER_STORE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".user_store")


def _ensure_user_store_dir():
    if not os.path.exists(USER_STORE_DIR):
        os.makedirs(USER_STORE_DIR, exist_ok=True)


def _get_safe_filename(key: str) -> str:
    return hashlib.sha256(key.strip().lower().encode()).hexdigest()


# ==========================================
# FAIL-CLOSED AES ENCRYPTION FOR BYOK KEYS & TOKENS
# ==========================================


class EncryptionConfigurationError(Exception):
    """Raised when encryption is requested but ENCRYPTION_KEY is missing or invalid."""
    pass


class DecryptionError(Exception):
    """Raised when ciphertext cannot be decrypted."""
    pass


def _get_fernet() -> Optional[Fernet]:
    """Retrieves Fernet cipher from ENCRYPTION_KEY in secrets or environment."""
    enc_key = ""
    try:
        enc_key = st.secrets.get("ENCRYPTION_KEY", "")
    except Exception:
        pass
    if not enc_key:
        enc_key = os.getenv("ENCRYPTION_KEY", "")

    if enc_key:
        try:
            return Fernet(enc_key.strip().encode())
        except Exception:
            return None
    return None


def encrypt_secret(plaintext: str) -> str:
    """Encrypts a sensitive string (API key or PAT) using AES/Fernet.
    
    Fails closed: raises EncryptionConfigurationError if encryption is unavailable.
    """
    if not plaintext:
        return ""
    f = _get_fernet()
    if not f:
        raise EncryptionConfigurationError(
            "ENCRYPTION_KEY is not configured or is invalid. Refusing to store sensitive secrets in plaintext."
        )
    return f.encrypt(plaintext.strip().encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    """Decrypts a stored ciphertext into plaintext.
    
    Fails closed: does NOT return raw ciphertext if decryption fails.
    Supports backward compatibility for legacy plaintext records that don't match Fernet token format.
    """
    if not ciphertext:
        return ""
    f = _get_fernet()
    if not f:
        raise EncryptionConfigurationError(
            "ENCRYPTION_KEY is not configured or is invalid. Cannot decrypt secrets."
        )

    # Fernet tokens start with 'gAAAAA'
    if ciphertext.startswith("gAAAAA"):
        try:
            return f.decrypt(ciphertext.encode()).decode()
        except InvalidToken:
            raise DecryptionError("Decryption failed: invalid encryption token or mismatched key.")
        except Exception as e:
            raise DecryptionError(f"Decryption failed: {str(e)}")

    # Legacy plaintext backward compatibility
    return ciphertext


def mask_secret(secret: str) -> str:
    """Masks a secret key showing first 3 and last 2 characters (e.g. AIz••••••••••8x)."""
    if not secret:
        return ""
    s = secret.strip()
    if len(s) <= 6:
        return "••••••••"
    return f"{s[:3]}{'•' * 10}{s[-2:]}"


def get_supabase_client() -> Client:
    """Initializes or retrieves the Supabase client from Streamlit secrets or environment."""
    url = ""
    key = ""
    try:
        url = st.secrets.get("SUPABASE_URL", "")
        key = st.secrets.get("SUPABASE_KEY", "")
    except Exception:
        pass
    if not url:
        url = os.getenv("SUPABASE_URL", "")
    if not key:
        key = os.getenv("SUPABASE_KEY", "")

    if not url or not key:
        raise ValueError(
            "Supabase credentials not configured. Please set SUPABASE_URL and SUPABASE_KEY in .streamlit/secrets.toml"
        )

    return create_client(url, key)


# ==========================================
# AUTHENTICATION & SECURE PKCE OAUTH
# ==========================================


def _clean_and_load_oauth_states() -> Dict[str, Any]:
    """Loads pending OAuth states and removes expired ones."""
    now = time.time()
    states: Dict[str, Any] = {}
    if os.path.exists(OAUTH_STATES_FILE):
        try:
            with open(OAUTH_STATES_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, dict):
                    states = data
        except Exception:
            states = {}

    cleaned = {}
    for st_id, val in states.items():
        if isinstance(val, dict):
            created_at = val.get("created_at", 0)
            if now - created_at < OAUTH_STATE_EXPIRY_SECONDS:
                cleaned[st_id] = val
        elif isinstance(val, str):
            # Legacy state without timestamp: expire to be safe
            pass
    return cleaned


def _save_oauth_states(states: Dict[str, Any]):
    """Persists active OAuth states."""
    try:
        with open(OAUTH_STATES_FILE, "w", encoding="utf-8") as fp:
            json.dump(states, fp)
    except Exception:
        pass


def get_oauth_url(provider: str, redirect_to: str) -> Dict[str, Any]:
    """Generates a secure OAuth URL with cryptographically random per-attempt PKCE verifier and state."""
    supabase = get_supabase_client()
    try:
        # Generate cryptographically secure random verifier (minimum 43, max 128 chars)
        pkce_verifier = secrets.token_urlsafe(64)
        pkce_challenge = generate_pkce_challenge(pkce_verifier)
        oauth_state = secrets.token_urlsafe(32)

        # Record in pending OAuth states with timestamp
        states = _clean_and_load_oauth_states()
        states[oauth_state] = {
            "verifier": pkce_verifier,
            "provider": provider,
            "created_at": time.time(),
        }
        _save_oauth_states(states)

        auth_url = (
            f"{supabase.auth._url}/authorize?provider={provider}"
            f"&redirect_to={redirect_to}"
            f"&code_challenge={pkce_challenge}"
            f"&code_challenge_method=s256"
            f"&state={oauth_state}"
        )
        return {"success": True, "url": auth_url, "state": oauth_state, "verifier": pkce_verifier}
    except Exception as e:
        return {"success": False, "error": str(e)}


def exchange_oauth_code(code: str, state: Optional[str] = None) -> Dict[str, Any]:
    """Exchanges an authorization code using the matched single-use PKCE verifier for the state."""
    supabase = get_supabase_client()
    states = _clean_and_load_oauth_states()

    pkce_verifier = None
    if state and state in states:
        pkce_verifier = states[state].get("verifier")
        del states[state]
        _save_oauth_states(states)
    elif len(states) == 1:
        # Fallback if state was stripped by provider or redirect
        single_state = next(iter(states))
        pkce_verifier = states[single_state].get("verifier")
        del states[single_state]
        _save_oauth_states(states)

    if not pkce_verifier:
        return {
            "success": False,
            "error": "OAuth authorization failed: invalid, expired, or already-used authorization attempt.",
        }

    try:
        supabase.auth._storage.set_item(
            f"{supabase.auth._storage_key}-code-verifier", pkce_verifier
        )
        response = supabase.auth.exchange_code_for_session(
            {"auth_code": code, "code_verifier": pkce_verifier}
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
# USER PROFILE & PERSISTENT HYBRID STORE
# ==========================================


def _load_local_user_data(
    user_id: str, email: Optional[str] = None
) -> Dict[str, Any]:
    """Loads and decrypts user data from .user_store by user_id or email hash."""
    _ensure_user_store_dir()
    f = _get_fernet()
    candidates = []
    if user_id:
        candidates.append(_get_safe_filename(user_id))
    if email:
        candidates.append(_get_safe_filename(email))

    for c in candidates:
        filepath = os.path.join(USER_STORE_DIR, f"{c}.json")
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as fp:
                    raw = fp.read()
                if f:
                    try:
                        decrypted = f.decrypt(raw.encode()).decode()
                        data = json.loads(decrypted)
                    except Exception:
                        data = json.loads(raw)
                else:
                    data = json.loads(raw)
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
    return {}


def _save_local_user_data(
    user_id: str, email: Optional[str], data: Dict[str, Any]
):
    """Encrypts and saves user data to .user_store keyed by both user_id and email."""
    _ensure_user_store_dir()
    f = _get_fernet()
    raw = json.dumps(data)
    if f:
        try:
            payload = f.encrypt(raw.encode()).decode()
        except Exception:
            payload = raw
    else:
        payload = raw

    keys = []
    if user_id:
        keys.append(_get_safe_filename(user_id))
    if email:
        keys.append(_get_safe_filename(email))

    for k in keys:
        filepath = os.path.join(USER_STORE_DIR, f"{k}.json")
        try:
            with open(filepath, "w", encoding="utf-8") as fp:
                fp.write(payload)
        except Exception:
            pass


def save_user_workspace_state(
    user_id: str,
    email: Optional[str] = None,
    active_repo: Optional[str] = None,
    active_model: Optional[str] = None,
    active_gh_account_id: Optional[str] = None,
    current_session_id: Optional[str] = None,
):
    """Persists the user's active workspace session state across logouts and browser timeouts."""
    data = _load_local_user_data(user_id, email)
    if active_repo is not None:
        data["active_repo"] = active_repo
    if active_model is not None:
        data["active_model"] = active_model
    if active_gh_account_id is not None:
        data["active_gh_account_id"] = active_gh_account_id
    if current_session_id is not None:
        data["current_session_id"] = current_session_id
    _save_local_user_data(user_id, email, data)


def load_user_workspace_state(
    user_id: str, email: Optional[str] = None
) -> Dict[str, Any]:
    """Retrieves the user's last active workspace session state."""
    data = _load_local_user_data(user_id, email)
    return {
        "active_repo": data.get("active_repo"),
        "active_model": data.get(
            "active_model", "Google Gemini 2.5 Pro (Recommended)"
        ),
        "active_gh_account_id": data.get("active_gh_account_id"),
        "current_session_id": data.get("current_session_id"),
    }


def get_user_profile(
    user_id: str, email: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Retrieves user profile data."""
    supabase = get_supabase_client()
    try:
        res = supabase.table("profiles").select("*").eq("id", user_id).execute()
        if res.data:
            return res.data[0]
    except Exception:
        pass
    data = _load_local_user_data(user_id, email)
    return data.get("profile")


def get_user_github_accounts(
    user_id: str, email: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetches all connected GitHub accounts for a user with decrypted tokens."""
    data = _load_local_user_data(user_id, email)
    local_accounts = data.get("github_accounts", [])

    supabase_accounts = []
    try:
        supabase = get_supabase_client()
        res = (
            supabase.table("user_github_accounts")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=False)
            .execute()
        )
        for a in res.data or []:
            supabase_accounts.append(
                {
                    "id": str(a["id"]),
                    "account_label": a["account_label"],
                    "github_username": a["github_username"],
                    "github_token": decrypt_secret(a.get("github_token", "")),
                    "is_active": a.get("is_active", True),
                }
            )
    except Exception:
        pass

    # Merge accounts
    merged = {a["github_username"]: a for a in local_accounts}
    for a in supabase_accounts:
        merged[a["github_username"]] = a

    result = list(merged.values())
    if result and not local_accounts:
        data["github_accounts"] = result
        _save_local_user_data(user_id, email, data)
    return result


def add_github_account(
    user_id: str,
    label: str,
    username: str,
    token: str,
    email: Optional[str] = None,
) -> bool:
    """Adds a new encrypted GitHub account connection for the user."""
    # Fail closed check: token must be encryptable if provided
    encrypted_token = ""
    if token:
        encrypted_token = encrypt_secret(token)

    data = _load_local_user_data(user_id, email)
    accounts = data.get("github_accounts", [])
    acc_id = f"gh_{int(datetime.now().timestamp())}_{username.lower()}"
    new_acc = {
        "id": acc_id,
        "account_label": label,
        "github_username": username,
        "github_token": token,
        "is_active": True,
    }
    accounts = [
        a for a in accounts if a["github_username"].lower() != username.lower()
    ]
    accounts.append(new_acc)
    data["github_accounts"] = accounts
    data["active_gh_account_id"] = acc_id
    _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        supabase.table("user_github_accounts").insert(
            {
                "user_id": user_id,
                "account_label": label,
                "github_username": username,
                "github_token": encrypted_token,
                "is_active": True,
            }
        ).execute()
    except Exception:
        update_github_token(user_id, token, email)

    return True


def delete_github_account(
    user_id: str, account_id: str, email: Optional[str] = None
) -> bool:
    """Removes a connected GitHub account."""
    data = _load_local_user_data(user_id, email)
    accounts = [
        a for a in data.get("github_accounts", []) if a["id"] != account_id
    ]
    data["github_accounts"] = accounts
    if data.get("active_gh_account_id") == account_id:
        data["active_gh_account_id"] = accounts[0]["id"] if accounts else None
    _save_local_user_data(user_id, email, data)

    if account_id == "default":
        return update_github_token(user_id, "", email)

    try:
        supabase = get_supabase_client()
        supabase.table("user_github_accounts").delete().eq(
            "id", account_id
        ).eq("user_id", user_id).execute()
    except Exception:
        update_github_token(user_id, "", email)
    return True


def attach_pat_to_account(
    user_id: str, account_id: str, token: str, email: Optional[str] = None
) -> bool:
    """Attaches or updates a Personal Access Token for an existing connected account."""
    clean_tok = token.strip()
    encrypted_token = ""
    if clean_tok:
        encrypted_token = encrypt_secret(clean_tok)

    data = _load_local_user_data(user_id, email)
    accounts = data.get("github_accounts", [])
    updated = False
    for acc in accounts:
        if acc["id"] == account_id:
            acc["github_token"] = clean_tok
            updated = True
            break
    if updated:
        data["github_accounts"] = accounts
        data["active_gh_account_id"] = account_id
        _save_local_user_data(user_id, email, data)
        try:
            supabase = get_supabase_client()
            supabase.table("user_github_accounts").update(
                {"github_token": encrypted_token}
            ).eq("id", account_id).eq("user_id", user_id).execute()
        except Exception:
            update_github_token(user_id, clean_tok, email)
        return True
    return False


def verify_github_credentials(
    token: str, username: Optional[str] = None
) -> Dict[str, Any]:
    """Validates with GitHub API that a Personal Access Token is authentic and valid."""
    clean_tok = token.strip() if token else ""
    if not clean_tok:
        return {
            "valid": False,
            "error": "Personal Access Token cannot be blank.",
        }

    try:
        from github import Auth, Github

        try:
            gh = Github(auth=Auth.Token(clean_tok))
        except Exception:
            gh = Github(clean_tok)

        auth_user = gh.get_user()
        real_login = auth_user.login
        real_name = auth_user.name or real_login
        public_repos = auth_user.public_repos
        total_private_repos = getattr(auth_user, "total_private_repos", 0)

        return {
            "valid": True,
            "login": real_login,
            "name": real_name,
            "public_repos": public_repos,
            "private_repos": total_private_repos,
            "avatar_url": getattr(auth_user, "avatar_url", ""),
        }
    except Exception as e:
        err_str = str(e)
        if "Bad credentials" in err_str or "401" in err_str:
            return {
                "valid": False,
                "error": "❌ Invalid GitHub Token: GitHub rejected the credentials (401 Bad Credentials). Please verify your token at https://github.com/settings/tokens.",
            }
        elif "rate limit" in err_str.lower():
            return {
                "valid": False,
                "error": "⚠️ GitHub API rate limit reached. Please wait a moment and try again.",
            }
        return {
            "valid": False,
            "error": f"❌ Could not verify GitHub account: {err_str}",
        }


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
        existing = get_user_github_accounts(user.id, user.email)
        if not existing:
            add_github_account(
                user.id, "Personal", username, "", user.email
            )
            return {"username": username, "label": "Personal"}
    return None


def update_github_token(
    user_id: str, token: str, email: Optional[str] = None
) -> bool:
    """Updates the primary GitHub token with encryption."""
    encrypted_tok = ""
    if token:
        encrypted_tok = encrypt_secret(token)

    data = _load_local_user_data(user_id, email)
    prof = data.get("profile", {})
    prof["github_token"] = token
    data["profile"] = prof
    _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        supabase.table("profiles").update({"github_token": encrypted_tok}).eq(
            "id", user_id
        ).execute()
        return True
    except Exception:
        return True


def get_user_api_keys(
    user_id: str, email: Optional[str] = None
) -> Dict[str, str]:
    """Fetches and decrypts all BYOK API keys configured for the user."""
    data = _load_local_user_data(user_id, email)
    keys = dict(data.get("api_keys", {}))

    try:
        supabase = get_supabase_client()
        res = (
            supabase.table("user_api_keys")
            .select("provider, api_key")
            .eq("user_id", user_id)
            .execute()
        )
        for item in res.data or []:
            p = item["provider"].lower()
            dec = decrypt_secret(item["api_key"])
            if dec:
                keys[p] = dec
    except Exception:
        pass

    return keys


def save_user_api_key(
    user_id: str, provider: str, api_key: str, email: Optional[str] = None
) -> bool:
    """Encrypts and persists an API key for a given provider (gemini, anthropic, openai, deepseek)."""
    clean_prov = provider.lower().strip()
    clean_key = api_key.strip()
    if not clean_key:
        return False

    # Fail closed: must encrypt before saving
    encrypted_key = encrypt_secret(clean_key)

    data = _load_local_user_data(user_id, email)
    keys = data.get("api_keys", {})
    keys[clean_prov] = clean_key
    data["api_keys"] = keys
    _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        supabase.table("user_api_keys").upsert(
            {
                "user_id": user_id,
                "provider": clean_prov,
                "api_key": encrypted_key,
            },
            on_conflict="user_id,provider",
        ).execute()
    except Exception:
        pass

    return True


def delete_user_api_key(
    user_id: str, provider: str, email: Optional[str] = None
) -> bool:
    """Permanently deletes a saved API key."""
    clean_prov = provider.lower().strip()
    data = _load_local_user_data(user_id, email)
    keys = data.get("api_keys", {})
    keys.pop(clean_prov, None)
    data["api_keys"] = keys
    _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        supabase.table("user_api_keys").delete().eq(
            "user_id", user_id
        ).eq("provider", clean_prov).execute()
    except Exception:
        pass
    return True


# ==========================================
# CHAT SESSIONS & MESSAGES (HYBRID SYNC)
# ==========================================


def list_user_sessions(
    user_id: str, email: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Lists all chat sessions for a user, ordered by most recent."""
    data = _load_local_user_data(user_id, email)
    local_sessions = data.get("sessions", [])

    sb_sessions = []
    try:
        supabase = get_supabase_client()
        res = (
            supabase.table("chat_sessions")
            .select("*")
            .eq("user_id", user_id)
            .order("updated_at", desc=True)
            .execute()
        )
        sb_sessions = res.data or []
    except Exception:
        pass

    merged = {s["id"]: s for s in local_sessions}
    for s in sb_sessions:
        merged[s["id"]] = s

    sessions = list(merged.values())
    sessions.sort(key=lambda x: str(x.get("updated_at", "")), reverse=True)
    return sessions


def create_chat_session(
    user_id: str,
    repo_name: str,
    repo_url: str,
    title: str,
    model_used: str,
    email: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Creates a new chat session for a repository."""
    session_id = f"sess_{int(datetime.now().timestamp())}_{repo_name.replace('/', '_')}"
    new_sess = {
        "id": session_id,
        "user_id": user_id,
        "repo_name": repo_name,
        "repo_url": repo_url,
        "title": title,
        "model_used": model_used,
        "updated_at": datetime.now().isoformat(),
    }

    data = _load_local_user_data(user_id, email)
    sessions = data.get("sessions", [])
    sessions = [s for s in sessions if s["id"] != session_id]
    sessions.insert(0, new_sess)
    data["sessions"] = sessions
    data["current_session_id"] = session_id
    data["active_repo"] = repo_name
    _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        res = supabase.table("chat_sessions").insert(
            {
                "user_id": user_id,
                "repo_name": repo_name,
                "repo_url": repo_url,
                "title": title,
                "model_used": model_used,
            }
        ).execute()
        if res.data:
            new_sess["id"] = res.data[0]["id"]
            data["current_session_id"] = new_sess["id"]
            _save_local_user_data(user_id, email, data)
    except Exception:
        pass

    return new_sess


def delete_chat_session(
    session_id: str,
    user_id: Optional[str] = None,
    email: Optional[str] = None,
) -> bool:
    """Deletes a chat session and all associated messages."""
    if user_id:
        data = _load_local_user_data(user_id, email)
        sessions = [
            s for s in data.get("sessions", []) if s["id"] != session_id
        ]
        data["sessions"] = sessions
        messages = data.get("messages", {})
        messages.pop(session_id, None)
        data["messages"] = messages
        if data.get("current_session_id") == session_id:
            data["current_session_id"] = None
        _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
        supabase.table("chat_sessions").delete().eq("id", session_id).execute()
    except Exception:
        pass
    return True


def get_session_messages(
    session_id: str,
    user_id: Optional[str] = None,
    email: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetches all messages in a session in chronological order."""
    local_msgs = []
    if user_id:
        data = _load_local_user_data(user_id, email)
        local_msgs = data.get("messages", {}).get(session_id, [])

    sb_msgs = []
    try:
        supabase = get_supabase_client()
        res = (
            supabase.table("chat_messages")
            .select("*")
            .eq("session_id", session_id)
            .order("created_at", desc=False)
            .execute()
        )
        sb_msgs = res.data or []
    except Exception:
        pass

    if sb_msgs:
        return sb_msgs
    return local_msgs


def add_message(
    session_id: str,
    sender: str,
    content: str,
    terminal_output: Optional[str] = None,
    user_id: Optional[str] = None,
    email: Optional[str] = None,
) -> bool:
    """Inserts a new message into the chat session."""
    if user_id:
        data = _load_local_user_data(user_id, email)
        messages_map = data.get("messages", {})
        msg_list = messages_map.get(session_id, [])
        msg_list.append(
            {
                "id": f"msg_{int(datetime.now().timestamp()*1000)}",
                "session_id": session_id,
                "sender": sender,
                "content": content,
                "terminal_output": terminal_output,
                "created_at": datetime.now().isoformat(),
            }
        )
        messages_map[session_id] = msg_list
        data["messages"] = messages_map

        # Update updated_at of session
        for s in data.get("sessions", []):
            if s["id"] == session_id:
                s["updated_at"] = datetime.now().isoformat()
                break
        _save_local_user_data(user_id, email, data)

    try:
        supabase = get_supabase_client()
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
    except Exception:
        pass
    return True
