""" Handles getting and refresshing API clients """
import os                
import pickle
from pathlib import Path 
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Read dotenv in my proj. root
load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

# Path to Oauth client
CLIENT_SECRET = Path(__file__).parent.parent / "client_secret.json"

# Pull key
API_KEY = os.getenv("YOUTUBE_API_KEY")

""" Runs the flow and returns two objects: youtube_data and youtube_analytics"""
def get_oauth_services(channel_id: str):
    project_root = Path(__file__).parent.parent
    tokens_dir   = project_root / "tokens"
    tokens_dir.mkdir(exist_ok=True)
    token_path   = tokens_dir / f"{channel_id}.pickle"
    creds = None

    if token_path.exists():
        with token_path.open("rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRET), SCOPES
            )
            # This one line both opens the browser and waits for the redirect
            creds = flow.run_local_server(
                port=8080,
                prompt="consent",
                access_type="offline"
            )

        # save the fresh creds
        with token_path.open("wb") as f:
            pickle.dump(creds, f)

    youtube   = build("youtube", "v3", credentials=creds)
    analytics = build("youtubeAnalytics", "v2", credentials=creds)
    return youtube, analytics


"""
Returns a YouTube Data API client authenticated via API Key. Use this for public endpoints 
"""
def get_public_youtube():
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in your .env")
    # build a YouTube Data API client using just the devKey
    return build("youtube", "v3", developerKey=API_KEY)
