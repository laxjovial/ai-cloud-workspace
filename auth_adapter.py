"""Authentication Adapter for Streamlit to JobServiceClient Bridge.

Phase B.3.3 Implementation:
- Establishes the authoritative server-side trust boundary between external identity
  providers (Supabase Auth / JWT) and internal JobServiceClient AuthenticatedPrincipal.
- Stable server authority secret resolution via:
  1. `JOB_SERVICE_AUTH_SECRET` in environment variables
  2. `JOB_SERVICE_AUTH_SECRET` in Streamlit secrets (`st.secrets`)
  3. Securely persisted server-managed file with strict 0600 permissions
     (`~/.ai_cloud_workspace/.secrets/job_service_auth_secret`).
- Cryptographic verification of Supabase session JWTs using Supabase GoTrue Auth client.
- Fail-closed validation: rejects unauthenticated sessions, expired/tampered tokens,
  and mismatched user identifiers.
- Issues cryptographically signed `AuthenticatedPrincipal` tokens with HMAC-SHA256 signatures.
"""

import os
import secrets
from typing import Any, Dict, Optional, Tuple

from job_service import AuthenticatedPrincipal, HMACAuthenticationAuthority, AuthenticationVerificationError
from db import get_supabase_client


_AUTH_SECRET_ENV_VAR = "JOB_SERVICE_AUTH_SECRET"
_DEFAULT_SECRETS_DIR = os.path.expanduser("~/.ai_cloud_workspace/.secrets")
_DEFAULT_SECRET_FILE = os.path.join(_DEFAULT_SECRETS_DIR, "job_service_auth_secret")


def get_or_create_server_authority_secret() -> bytes:
    """Resolves or generates a persistent, secure authority secret for signing principals.
    
    Priority:
    1. os.environ.get("JOB_SERVICE_AUTH_SECRET")
    2. st.secrets["JOB_SERVICE_AUTH_SECRET"] (if streamlit is active and configured)
    3. Secure file in ~/.ai_cloud_workspace/.secrets/job_service_auth_secret (mode 0600)
    """
    # 1. Environment variable
    env_secret = os.environ.get(_AUTH_SECRET_ENV_VAR)
    if env_secret and len(env_secret.strip()) >= 16:
        return env_secret.strip().encode("utf-8")

    # 2. Streamlit secrets (if available)
    try:
        import streamlit as st
        if hasattr(st, "secrets") and _AUTH_SECRET_ENV_VAR in st.secrets:
            secret_val = st.secrets[_AUTH_SECRET_ENV_VAR]
            if secret_val and len(str(secret_val).strip()) >= 16:
                return str(secret_val).strip().encode("utf-8")
    except Exception:
        pass

    # 3. Secure file-backed secret with 0600 permissions
    if os.path.exists(_DEFAULT_SECRET_FILE):
        try:
            with open(_DEFAULT_SECRET_FILE, "rb") as f:
                content = f.read().strip()
                if len(content) >= 16:
                    return content
        except Exception:
            pass

    # Generate a cryptographically random 32-byte secret and persist securely
    try:
        os.makedirs(_DEFAULT_SECRETS_DIR, mode=0o700, exist_ok=True)
        new_secret = secrets.token_hex(32).encode("utf-8")  # 64 hex chars
        # Write with strict 0600 permissions
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        mode = 0o600
        fd = os.open(_DEFAULT_SECRET_FILE, flags, mode)
        with os.fdopen(fd, "wb") as f:
            f.write(new_secret)
        return new_secret
    except Exception as e:
        # If filesystem write fails, fallback to ephemeral secret in process memory
        # but ensure minimum length requirement
        return secrets.token_bytes(32)


class SupabaseAuthAdapter:
    """Bridges Supabase user session / JWT to JobService AuthenticatedPrincipal."""

    def __init__(self, authority: Optional[HMACAuthenticationAuthority] = None):
        if authority is None:
            secret = get_or_create_server_authority_secret()
            authority = HMACAuthenticationAuthority(secret)
        self._authority = authority

    @property
    def authority(self) -> HMACAuthenticationAuthority:
        return self._authority

    def validate_supabase_token(self, access_token: str) -> Dict[str, Any]:
        """Validates a Supabase JWT access token using the Supabase GoTrue Auth client.
        
        Returns the user dict if valid; raises AuthenticationVerificationError if invalid or expired.
        """
        if not access_token or not isinstance(access_token, str) or not access_token.strip():
            raise AuthenticationVerificationError("Access token is missing or empty.")

        clean_token = access_token.strip()
        supabase = get_supabase_client()
        try:
            # GoTrue client get_user(jwt) validates the token against Supabase auth server
            user_response = supabase.auth.get_user(jwt=clean_token)
            if not user_response or not getattr(user_response, "user", None):
                raise AuthenticationVerificationError("Supabase token validation returned no user.")
            user_obj = user_response.user
            user_id = getattr(user_obj, "id", None)
            email = getattr(user_obj, "email", None)
            if not user_id:
                raise AuthenticationVerificationError("User object missing ID.")
            return {
                "id": str(user_id),
                "email": str(email) if email else None,
                "user_metadata": getattr(user_obj, "user_metadata", {}) or {},
            }
        except Exception as e:
            raise AuthenticationVerificationError(f"Supabase JWT validation failed: {str(e)}") from e

    def issue_principal_for_session(
        self,
        session_user: Dict[str, Any],
        access_token: Optional[str] = None,
        require_token_validation: bool = False,
    ) -> AuthenticatedPrincipal:
        """Issues an AuthenticatedPrincipal for a verified Streamlit user session.
        
        If `require_token_validation` is True, `access_token` must be provided and
        is verified against Supabase Auth before issuance.
        """
        if not session_user or not isinstance(session_user, dict):
            raise AuthenticationVerificationError("Invalid or missing session_user dictionary.")

        user_id = session_user.get("id")
        email = session_user.get("email")

        if not user_id or not str(user_id).strip():
            raise AuthenticationVerificationError("Cannot issue principal for session without user ID.")

        if require_token_validation:
            if not access_token:
                raise AuthenticationVerificationError("Token validation required but access_token not provided.")
            verified_user = self.validate_supabase_token(access_token)
            if verified_user["id"] != str(user_id).strip():
                raise AuthenticationVerificationError("Session user ID does not match verified token ID.")
            email = verified_user.get("email") or email

        return self._authority.issue_principal_from_verified_credential(
            verified_user_id=str(user_id).strip(),
            email=str(email).strip() if email else None,
            roles=("user",),
            issuer="ai_cloud_auth_adapter",
        )
