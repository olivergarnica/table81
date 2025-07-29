import os
import pickle
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

PROJECT_ROOT = Path(__file__).parent.parent
CLIENT_SECRET = PROJECT_ROOT / "client_secret.json"

TOKENS_DIR = Path(os.getenv("YT_TOKENS_DIR", PROJECT_ROOT / "tokens"))
TOKENS_DIR.mkdir(parents=True, exist_ok=True)

OPEN_BROWSER = str(os.getenv("OAUTH_OPEN_BROWSER", "1")).lower() not in ("0", "false")

API_KEY = os.getenv("YOUTUBE_API_KEY")

def get_public_youtube():
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in your .env")
    return build("youtube", "v3", developerKey=API_KEY)

def _token_path(channel_id: str) -> Path:
    return TOKENS_DIR / f"{channel_id}.pickle"

def _atomic_write_pickle(obj, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        pickle.dump(obj, f)
    tmp.replace(path)

def _run_flow():
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
    # select_account helps you choose the Brand identity; include_granted_scopes is optional
    return flow.run_local_server(
        port=0,
        prompt="select_account",
        access_type="offline",
        open_browser=OPEN_BROWSER,
        # authorization_prompt_message and success_message can be customized if desired
    )

def _verify_token_matches_channel(creds, expected_channel_id: str) -> None:
    yt = build("youtube", "v3", credentials=creds)
    resp = yt.channels().list(part="id", mine=True, maxResults=50).execute()
    mine_ids: List[str] = [it["id"] for it in resp.get("items", [])]
    if expected_channel_id not in mine_ids:
        raise RuntimeError(
            f"Authorized channel(s) {mine_ids} do not include {expected_channel_id}. "
            "When the Google chooser opens, pick the Brand Account for this channel."
        )

"""
Ensure a valid token exists for channel_id. If missing/invalid, run OAuth.
Returns the token file path.
"""
def ensure_channel_token(channel_id: str, force_reauth: bool = False) -> Path:
    tp = _token_path(channel_id)
    creds = None

    if tp.exists() and not force_reauth:
        with tp.open("rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.refresh_token and not force_reauth:
            try:
                creds.refresh(Request())
                _verify_token_matches_channel(creds, channel_id)
            except RefreshError:
                # Refresh token invalid/expired/revoked -> re-consent
                creds = _run_flow()
                _verify_token_matches_channel(creds, channel_id)
                _atomic_write_pickle(creds, tp)
        else:
            # No creds or no refresh token -> re-consent
            creds = _run_flow()
            _verify_token_matches_channel(creds, channel_id)
            _atomic_write_pickle(creds, tp)
    else:
        # Valid creds
        try:
            _verify_token_matches_channel(creds, channel_id)
        except Exception:
            creds = _run_flow()
            _verify_token_matches_channel(creds, channel_id)
            _atomic_write_pickle(creds, tp)

    return tp

# Load a token for channel_id and return authenticated clients.
def get_oauth_services(channel_id: str):
    tp = _token_path(channel_id)
    if not tp.exists():
        ensure_channel_token(channel_id, force_reauth=True)

    with tp.open("rb") as f:
        creds = pickle.load(f)

    youtube = build("youtube", "v3", credentials=creds)
    analytics = build("youtubeAnalytics", "v2", credentials=creds)
    return youtube, analytics
