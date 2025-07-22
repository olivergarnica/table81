import traceback, sys
from datetime import date, timedelta
from src.config import ALL_CHANNEL_IDS
from src.auth import get_public_youtube, get_oauth_services
from src.fetch import (
    fetch_channel_metadata,
    fetch_upload_playlist_video_ids,
    fetch_video_metadata_bulk,
    fetch_channel_daily_analytics,
    fetch_video_daily_analytics,
)
from src.transform import (
    transform_channel_item,
    transform_video_items,
    transform_channel_daily_response,
    transform_video_daily_response,
)
from src.db import (
    upsert_channels,
    upsert_videos,
    upsert_channel_daily,
    upsert_video_daily,
    prune_old_video_daily,
)

#  Helper that ingests ONE channel for ONE day NOTE: CHANGED IT TO TWO DAYS AGO
YESTERDAY = date.today() - timedelta(days=2)

def ingest_channel(channel_id: str, keep_days: int = 30) -> None:
    # Public client (no OAuth needed for Data API calls)
    yt_pub = get_public_youtube()

    # Channel metadata
    raw_channel = fetch_channel_metadata(yt_pub, channel_id)
    ch_row = transform_channel_item(raw_channel)
    if ch_row:
        upsert_channels([ch_row])

    # Video discovery & metadata
    uploads_id = raw_channel.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    video_ids: list[str] = []
    if uploads_id:
        # max_pages=2 -> up to 100 newest uploads each run; adjust if you need deeper scans
        video_ids = fetch_upload_playlist_video_ids(yt_pub, uploads_id, max_pages=2)
        if video_ids:
            raw_video_items = fetch_video_metadata_bulk(yt_pub, video_ids)
            video_rows = transform_video_items(raw_video_items)
            upsert_videos(video_rows)

    # OAuth client (Analytics API requires channel-scoped creds)
    _yt_oauth, analytics = get_oauth_services(channel_id)

    # Channel daily stats (yesterday)
    raw_ch_daily = fetch_channel_daily_analytics(analytics, channel_id, YESTERDAY, YESTERDAY)
    ch_daily_rows = transform_channel_daily_response(raw_ch_daily, channel_id)
    upsert_channel_daily(ch_daily_rows)

    # Per-video daily stats (yesterday)
    if video_ids:
        video_daily_rows = []
        for vid in video_ids:
            raw_vid_daily = fetch_video_daily_analytics(
                analytics, channel_id, vid, YESTERDAY, YESTERDAY
            )
            video_daily_rows.extend(transform_video_daily_response(raw_vid_daily, vid))

        upsert_video_daily(video_daily_rows)

    # House-keeping: prune old daily rows
    prune_old_video_daily(retain_days=keep_days)


#  Entry-point
def main() -> None:
    for cid in ALL_CHANNEL_IDS:
        print(f"=== Processing channel {cid} ===")
        try:
            ingest_channel(cid)
            print(f"Done channel {cid}")
        except Exception as exc:
            # Log and continue; don't break whole run on one failure
            print(f"***  Error on channel {cid}: {exc} ***")
            traceback.print_exc(file=sys.stdout)
            
    print("Ingest complete.")

if __name__ == "__main__":
    main()