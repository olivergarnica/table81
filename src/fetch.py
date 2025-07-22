from datetime import date, timedelta
import time, functools
from googleapiclient.errors import HttpError

# Retry decorator
def retry(backoff: float = 1.6, max_attempts: int = 5, statuses=(403, 500, 503, 504)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kw):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kw)
                except HttpError as e:
                    status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
                    attempt += 1
                    if status not in statuses or attempt >= max_attempts:
                        raise
                    time.sleep(backoff ** attempt)
        return wrapped
    return deco


# YT Data API (Key)
@retry()
def fetch_channel_metadata(youtube_pub, channel_id: str) -> dict:
    """Fetch one channel's snippet, statistics, contentDetails. Return {} if not found."""
    resp = youtube_pub.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id,
        maxResults=1
    ).execute()
    items = resp.get("items", [])
    return items[0] if items else {}

"""
Walk the uploads playlist (newest first) and collect video IDs.
Early-stop if we hit a known ID. Returns [] if playlist not found (new channel).
"""
@retry()
def fetch_upload_playlist_video_ids(
    youtube_pub,
    uploads_playlist_id: str,
    stop_after_known: set[str] | None = None,
    max_pages: int | None = None
) -> list[str]:
    video_ids: list[str] = []
    page_token = None
    pages = 0
    while True:
        try:
            resp = youtube_pub.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=page_token
            ).execute()
        except HttpError as e:
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            if status == 404:
                return []  # empty channel
            raise

        for item in resp.get("items", []):
            vid = item["contentDetails"]["videoId"]
            if stop_after_known and vid in stop_after_known:
                return video_ids
            video_ids.append(vid)

        page_token = resp.get("nextPageToken")
        pages += 1
        if not page_token:
            break
        if max_pages and pages >= max_pages:
            break
    return video_ids

# Successive size-length chunks.
def _chunked(seq: list[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

# Batch fetch stats (50 IDs per call)
@retry()
def fetch_video_metadata_bulk(youtube_pub, video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    items: list[dict] = []
    for chunk in _chunked(video_ids, 50):
        resp = youtube_pub.videos().list(
            part="snippet,statistics",
            id=",".join(chunk),
            maxResults=len(chunk)
        ).execute()
        items.extend(resp.get("items", []))
    return items

# Analytics API (OAuth) 
@retry()
def fetch_channel_daily_analytics(
    analytics,
    channel_id: str,
    start: date,
    end: date
) -> dict:
    return analytics.reports().query(
        ids=f"channel=={channel_id}",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        dimensions="day",
        metrics="views,subscribersGained,subscribersLost,estimatedMinutesWatched"
    ).execute()

@retry()
def fetch_video_daily_analytics(
    analytics,
    channel_id: str,
    video_id: str,
    start: date,
    end: date
) -> dict:
    return analytics.reports().query(
        ids=f"channel=={channel_id}",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics=(
            "views,likes,comments,shares,"
            "averageViewDuration,averageViewPercentage,"
            "estimatedMinutesWatched"
        ),
        dimensions="day",
        filters=f"video=={video_id}"
    ).execute()