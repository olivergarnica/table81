import argparse
import time
from src.auth import ensure_channel_token, get_public_youtube, TOKENS_DIR
from src.config import ALL_CHANNEL_IDS

def _title_for(yt_pub, cid: str) -> str:
    try:
        resp = yt_pub.channels().list(part="snippet", id=cid, maxResults=1).execute()
        return resp["items"][0]["snippet"]["title"]
    except Exception:
        return cid

def main():
    p = argparse.ArgumentParser(description="Mint/refresh OAuth tokens for all Brand Channels.")
    p.add_argument("--force", action="store_true",
                   help="Force re-consent for every channel (use sparingly).")
    p.add_argument("--sleep", type=float, default=0.5,
                   help="Delay between channels (seconds).")
    args = p.parse_args()

    yt_pub = get_public_youtube()
    print(f"Tokens directory: {TOKENS_DIR.resolve()}")

    successes, failures = [], []
    total = len(ALL_CHANNEL_IDS)

    for i, cid in enumerate(ALL_CHANNEL_IDS, 1):
        title = _title_for(yt_pub, cid)
        print(f"\n[{i}/{total}] {title} ({cid})")
        try:
            path = ensure_channel_token(cid, force_reauth=args.force)
            print(f"YESYES Saved token -> {path}")
            successes.append(cid)
        except Exception as e:
            print(f"NONO Failed -> {e}")
            failures.append((cid, str(e)))
        time.sleep(args.sleep)

    print("\nSummary:")
    print(f"  Success: {len(successes)}")
    print(f"  Failed : {len(failures)}")
    if failures:
        for cid, err in failures:
            print(f"   - {cid}: {err}")

if __name__ == "__main__":
    main()
