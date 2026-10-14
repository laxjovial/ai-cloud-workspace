# ⚡ AI Cloud Workspace

Autonomous AI coding platform with live Linux sandboxes, terminal tests, multi-model BYOK support, and mobile web previews.

## 🚀 Features

- **Autonomous Agent**: Loops through inspecting repos, editing code, running bash tests, and opening Pull Requests.
- **Any Language & Command**: Executes Python, Node.js, Go, Rust, and arbitrary Linux bash commands inside isolated cloud sandboxes via [E2B](https://e2b.dev).
- **Mobile Live Previews**: Runs dev servers (`npm run dev`, `python app.py`) in the background and generates public HTTPS URLs accessible directly from your phone.
- **Multi-GitHub Account Switcher**: Connect multiple GitHub accounts (Work, Personal, Clients) and switch identities dynamically.
- **Bring Your Own Key (BYOK)**: Supports Google Gemini (2.5 Pro / Flash), Anthropic Claude (3.7 Sonnet), OpenAI (GPT-4o), and DeepSeek (V3 / R1) with zero markup.
- **Indefinite Chat History**: User sessions and message history are permanently persisted in PostgreSQL via Supabase.

---

## 🛠️ Tech Stack

- **Frontend**: Streamlit
- **Backend Sandbox**: E2B Cloud Sandboxes (Ubuntu Linux)
- **Database & Auth**: Supabase (PostgreSQL, OAuth with Google & GitHub, Row Level Security)
- **AI Engine**: LiteLLM / Google Gemini / Anthropic / OpenAI

---

## 📦 Setup & Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/laxjovial/ai-cloud-workspace.git
   cd ai-cloud-workspace
   ```

2. **Create a virtual environment & install dependencies:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Secrets:**
   Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`:
   ```bash
   cp .streamlit/secrets.toml.example .streamlit/secrets.toml
   ```
   Add your `SUPABASE_URL`, `SUPABASE_KEY` (anon public key), and `E2B_API_KEY`.

4. **Launch the application:**
   ```bash
   streamlit run app.py
   ```
