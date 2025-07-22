from datetime import date, timedelta
from src.auth import get_public_youtube, get_oauth_services
from src.config import ALL_CHANNEL_IDS
from src.fetch import (
    fetch_channel_metadata,
    fetch_upload_playlist_video_ids,
    fetch_video_metadata_bulk,
    fetch_channel_daily_analytics
)

def test_single_channel(channel_id: str):
    yt_pub = get_public_youtube()

    # Channel metadata
    ch_meta = fetch_channel_metadata(yt_pub, channel_id)
    if not ch_meta:
        print(f"[{channel_id}] No metadata returned.")
        return
    print(f"[{channel_id}] Title: {ch_meta['snippet']['title']}")

    uploads_id = ch_meta["contentDetails"]["relatedPlaylists"]["uploads"]

    # Playlist (video IDs)
    video_ids = fetch_upload_playlist_video_ids(yt_pub, uploads_id, max_pages=1)
    if not video_ids:
        print(f"[{channel_id}] No uploads yet (empty uploads playlist).")
    else:
        print(f"[{channel_id}] Fetched {len(video_ids)} video IDs (first page). First few: {video_ids[:5]}")

        # Bulk video metadata (limit to 10 for test)
        meta_items = fetch_video_metadata_bulk(yt_pub, video_ids[:10])
        print(f"[{channel_id}] Got metadata for {len(meta_items)} videos.")
        if meta_items:
            sample = meta_items[0]
            print(f"[{channel_id}] Sample video title: {sample['snippet']['title']}")

    # Channel analytics (yesterday) – should run even if no videos yet
    yesterday = date.today() - timedelta(days=1)
    _yt, analytics = get_oauth_services(channel_id)
    ch_daily = fetch_channel_daily_analytics(analytics, channel_id, yesterday, yesterday)
    rows = ch_daily.get("rows") or []
    if rows:
        print(f"[{channel_id}] Channel analytics yesterday: {rows}")
    else:
        print(f"[{channel_id}] No channel analytics rows for yesterday (maybe no activity).")
    print("-" * 60)


def main():
    # Just test the first channel for now
    first = ALL_CHANNEL_IDS[13]
    test_single_channel(first)

if __name__ == "__main__":
    main()
