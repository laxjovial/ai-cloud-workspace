"""UI Theme and Continuous Design Dial Token Architecture.

Phase B.3.3 Implementation:
- Implements a continuous design spectrum from 0.0 (Classic / Legacy) to 1.0 (Futuristic / AI-Native),
  with 0.5 as the Balanced Modern default.
- Generates cohesive CSS variables across:
  - Font families (System UI -> Clean Geometric -> Technical / Monospace accents)
  - Border radii (0-2px -> 8-10px -> 16-20px)
  - Card background surfaces and subtle glassmorphic blurs
  - Border accents and glow effects
  - Spacing, padding, and elevation shadows
- Persists user design dial preference per-user in local encrypted `.user_store`
  via `db.py` secure storage.
"""

from typing import Dict, Any, Optional
from db import _load_local_user_data, _save_local_user_data


DEFAULT_DESIGN_DIAL = 0.5


def clamp_dial(val: Any) -> float:
    """Clamps dial value to float between 0.0 and 1.0, defaulting to 0.5."""
    try:
        f = float(val)
        if f < 0.0:
            return 0.0
        if f > 1.0:
            return 1.0
        return round(f, 2)
    except (ValueError, TypeError):
        return DEFAULT_DESIGN_DIAL


def get_user_design_dial(user_id: str, email: Optional[str] = None) -> float:
    """Retrieves the user's persisted design dial value (0.0 to 1.0)."""
    try:
        data = _load_local_user_data(user_id, email)
        dial = data.get("design_dial", DEFAULT_DESIGN_DIAL)
        return clamp_dial(dial)
    except Exception:
        return DEFAULT_DESIGN_DIAL


def set_user_design_dial(user_id: str, email: Optional[str], dial_value: float) -> float:
    """Persists the user's design dial value (0.0 to 1.0) into the encrypted user store."""
    valid_dial = clamp_dial(dial_value)
    try:
        data = _load_local_user_data(user_id, email)
        data["design_dial"] = valid_dial
        _save_local_user_data(user_id, email, data)
    except Exception:
        pass
    return valid_dial


def compute_theme_tokens(dial: float) -> Dict[str, str]:
    """Interpolates CSS design tokens across the continuous dial [0.0, 1.0].
    
    0.0 -> Classic / Enterprise (structured, high-density, sharp corners, subtle borders)
    0.5 -> Balanced Modern (sleek, comfortable spacing, rounded corners, soft shadows)
    1.0 -> Futuristic / AI-Native (deep dark canvas, glowing cyan/purple accents, glassy cards, pill tags)
    """
    t = clamp_dial(dial)

    # 1. Border radius: 2px (at 0.0) -> 8px (at 0.5) -> 18px (at 1.0)
    radius_card = f"{int(2 + t * 16)}px"
    radius_btn = f"{int(2 + t * 22)}px"
    radius_input = f"{int(2 + t * 12)}px"

    # 2. Font family
    if t < 0.3:
        font_family = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
        code_font = "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, Courier, monospace"
    elif t < 0.7:
        font_family = "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
        code_font = "'JetBrains Mono', 'Fira Code', Menlo, monospace"
    else:
        font_family = "'Outfit', 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif"
        code_font = "'JetBrains Mono', 'Space Mono', monospace"

    # 3. Colors and glow shadows
    # Interpolation of accent hue (30 = classic blue 210, 50 = modern indigo 235, 100 = neon cyan/purple 180)
    if t < 0.3:
        accent_color = "#2563eb"  # Royal blue
        accent_hover = "#1d4ed8"
        card_bg = "rgba(255, 255, 255, 0.04)"
        card_border = "1px solid rgba(255, 255, 255, 0.12)"
        box_shadow = "0 1px 3px rgba(0, 0, 0, 0.12)"
        glow_accent = "none"
        backdrop_filter = "none"
        pill_badge_bg = "rgba(37, 99, 235, 0.15)"
        pill_badge_color = "#60a5fa"
    elif t < 0.7:
        accent_color = "#6366f1"  # Modern Indigo
        accent_hover = "#4f46e5"
        card_bg = "rgba(255, 255, 255, 0.06)"
        card_border = "1px solid rgba(99, 102, 241, 0.25)"
        box_shadow = "0 4px 14px rgba(0, 0, 0, 0.25)"
        glow_accent = "0 0 15px rgba(99, 102, 241, 0.2)"
        backdrop_filter = "blur(8px)"
        pill_badge_bg = "rgba(99, 102, 241, 0.2)"
        pill_badge_color = "#a5b4fc"
    else:
        accent_color = "#06b6d4"  # Neon Cyan / AI-Native
        accent_hover = "#0891b2"
        card_bg = "rgba(15, 23, 42, 0.65)"
        card_border = "1px solid rgba(6, 182, 212, 0.4)"
        box_shadow = "0 8px 30px rgba(6, 182, 212, 0.15)"
        glow_accent = "0 0 24px rgba(6, 182, 212, 0.35)"
        backdrop_filter = "blur(14px)"
        pill_badge_bg = "rgba(6, 182, 212, 0.25)"
        pill_badge_color = "#67e8f9"

    return {
        "dial_value": str(t),
        "radius_card": radius_card,
        "radius_btn": radius_btn,
        "radius_input": radius_input,
        "font_family": font_family,
        "code_font": code_font,
        "accent_color": accent_color,
        "accent_hover": accent_hover,
        "card_bg": card_bg,
        "card_border": card_border,
        "box_shadow": box_shadow,
        "glow_accent": glow_accent,
        "backdrop_filter": backdrop_filter,
        "pill_badge_bg": pill_badge_bg,
        "pill_badge_color": pill_badge_color,
    }


def generate_theme_css(dial: float) -> str:
    """Generates a complete CSS block based on the current design dial value."""
    tokens = compute_theme_tokens(dial)

    css = f"""
    <style>
    :root {{
        --ui-font-family: {tokens['font_family']};
        --ui-code-font: {tokens['code_font']};
        --ui-radius-card: {tokens['radius_card']};
        --ui-radius-btn: {tokens['radius_btn']};
        --ui-radius-input: {tokens['radius_input']};
        --ui-accent: {tokens['accent_color']};
        --ui-accent-hover: {tokens['accent_hover']};
        --ui-card-bg: {tokens['card_bg']};
        --ui-card-border: {tokens['card_border']};
        --ui-box-shadow: {tokens['box_shadow']};
        --ui-glow: {tokens['glow_accent']};
        --ui-backdrop: {tokens['backdrop_filter']};
        --ui-badge-bg: {tokens['pill_badge_bg']};
        --ui-badge-color: {tokens['pill_badge_color']};
    }}

    body, .stApp {{
        font-family: var(--ui-font-family);
    }}

    /* Card styling */
    .stCard, .ui-card {{
        background: var(--ui-card-bg);
        border: var(--ui-card-border);
        border-radius: var(--ui-radius-card);
        box-shadow: var(--ui-box-shadow);
        backdrop-filter: var(--ui-backdrop);
        padding: 1.25rem;
        margin-bottom: 1rem;
        transition: all 0.25s ease-in-out;
    }}

    .stCard:hover, .ui-card:hover {{
        box-shadow: var(--ui-glow);
    }}

    /* Buttons */
    button[kind="primary"], .stButton > button {{
        border-radius: var(--ui-radius-btn);
        transition: all 0.2s ease-in-out;
    }}

    /* Inputs */
    .stTextInput > div > div > input, .stTextArea textarea, .stSelectbox select {{
        border-radius: var(--ui-radius-input);
    }}

    /* Status badge */
    .status-badge {{
        display: inline-block;
        padding: 0.25rem 0.65rem;
        font-size: 0.8rem;
        font-weight: 600;
        border-radius: var(--ui-radius-btn);
        background: var(--ui-badge-bg);
        color: var(--ui-badge-color);
        letter-spacing: 0.03em;
        text-transform: uppercase;
    }}
    </style>
    """
    return css
