from __future__ import annotations
from datetime import datetime, date
from typing import List, Sequence, Optional


# Helpers: ISO 8601 parsing
def _parse_iso_datetime(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Replace 'Z' with '+00:00' for fromisoformat compatibility
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_iso_date(value: str | None) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


# Channel Metadata
"""
Flatten a raw channel resource (from fetch_channel_metadata) into a channels row.
raw structure contains at least: id, snippet{title, publishedAt}, statistics{subscriberCount, viewCount}
Returns dict with keys needed by `channels` table.
Missing numeric stats default to 0
"""
def transform_channel_item(raw: dict) -> dict:
    if not raw:
        return {}

    snippet = raw.get("snippet", {})
    stats = raw.get("statistics", {})

    channel_id = raw.get("id")
    title = snippet.get("title") or ""
    created_at_iso = snippet.get("publishedAt")
    created_at = None
    if created_at_iso:
        dt = _parse_iso_datetime(created_at_iso)
        if dt:
            created_at = dt.date()

    subscribers = int(stats.get("subscriberCount", 0) or 0)
    total_views = int(stats.get("viewCount", 0) or 0)

    return {
        "channel_id": channel_id,
        "title": title,
        "subscribers": subscribers,
        "total_views": total_views,
        "created_at": created_at,
    }


# Video Metadata
"""
Heuristic to label a thumbnail as 'custom' or 'default'.
YouTube doesn't give a direct flag; presence of higher resolutions
often implies a custom upload. It's simplistic; refine later if needed.
"""
def _infer_thumbnail_type(thumbnails: dict | None) -> str:
    if not thumbnails:
        return "unknown"
    # Sizes often: default, medium, high, standard, maxres
    keys = set(thumbnails.keys())
    if "maxres" in keys or "standard" in keys:
        return "custom"
    return "default"

"""
Flatten raw 'videos().list' items (snippet + statistics) into rows for the videos table.
NOTE: Some channels might have zero videos (returns []).
Each raw item contains:
    id, snippet{title, description, tags, publishedAt, thumbnails}, statistics{...}
For initial load we do not store counts here (they go to daily stats).
"""
def transform_video_items(video_items: List[dict]) -> List[dict]:
    rows: List[dict] = []
    for item in video_items:
        vid = item.get("id")
        snippet = item.get("snippet", {})
        published_at = _parse_iso_datetime(snippet.get("publishedAt"))
        tags_list = snippet.get("tags", []) or []
        tags_str = ",".join(tags_list) if tags_list else None
        thumb_type = _infer_thumbnail_type(snippet.get("thumbnails"))

        rows.append({
            "video_id": vid,
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title") or "",
            "description": snippet.get("description") or "",
            "tags": tags_str,
            "thumbnail_type": thumb_type,
            "published_at": published_at,
        })
    return rows


# Channel Daily Analytics
"""
Convert a raw analytics response (channel daily) into list of row dicts for channel_daily_stats table.
Expected columns order (dimensions=day): date, views, subscribersGained, subscribersLost, estimatedMinutesWatched
We do a defensive mapping using columnHeaders to avoid relying purely on order.
"""
def transform_channel_daily_response(resp: dict, channel_id: str) -> List[dict]:
    
    if not resp or not resp.get("rows"):
        return []

    headers = resp.get("columnHeaders", [])
    # Build name->index mapping
    idx_map = {h["name"]: i for i, h in enumerate(headers)}

    rows_out: List[dict] = []
    for raw_row in resp["rows"]:
        # dimension 'day' is always first if included
        day_str = raw_row[idx_map.get("day", 0)]
        day_d = _parse_iso_date(day_str)

        def _get_int(name: str) -> int:
            idx = idx_map.get(name)
            if idx is None:
                return 0
            try:
                return int(raw_row[idx])
            except (TypeError, ValueError):
                return 0

        def _get_float(name: str) -> float:
            idx = idx_map.get(name)
            if idx is None:
                return 0.0
            try:
                return float(raw_row[idx])
            except (TypeError, ValueError):
                return 0.0

        rows_out.append({
            "channel_id": channel_id,
            "date": day_d,
            "views": _get_int("views"),
            "subs_gained": _get_int("subscribersGained"),
            "subs_lost": _get_int("subscribersLost"),
            "estimated_minutes_watched": _get_float("estimatedMinutesWatched"),
        })

    return rows_out

# Video Daily Analytics
VIDEO_DAILY_METRIC_NAMES = [
    "views","likes","comments","shares",
    "averageViewDuration","averageViewPercentage",
    "estimatedMinutesWatched"
]

def transform_video_daily_response(resp: dict, video_id: str) -> List[dict]:
    """
    Convert raw per-video analytics response (day dimension) into list of row dicts
    for video_daily_stats table.

    Raw row shape:
      [day, views, likes, comments, shares, averageViewDuration,
       averageViewPercentage, estimatedMinutesWatched]
    """
    if not resp or not resp.get("rows"):
        return []

    headers = resp.get("columnHeaders", [])
    idx_map = {h["name"]: i for i, h in enumerate(headers)}

    out: List[dict] = []
    for raw_row in resp["rows"]:
        day_str = raw_row[idx_map.get("day", 0)]
        day_d = _parse_iso_date(day_str)

        def _get_int(name: str) -> int:
            idx = idx_map.get(name)
            if idx is None:
                return 0
            try:
                return int(raw_row[idx])
            except (TypeError, ValueError):
                return 0

        def _get_float(name: str) -> float:
            idx = idx_map.get(name)
            if idx is None:
                return 0.0
            try:
                return float(raw_row[idx])
            except (TypeError, ValueError):
                return 0.0

        avg_dur = _get_float("averageViewDuration")
        avg_pct = _get_float("averageViewPercentage")
        watch_time = _get_float("estimatedMinutesWatched")

        out.append({
            "video_id": video_id,
            "date": day_d,
            "views": _get_int("views"),
            "likes": _get_int("likes"),
            "comments": _get_int("comments"),
            "shares": _get_int("shares"),
            "watch_time": watch_time,
            "avg_view_duration": avg_dur,
            "avg_view_percent": avg_pct,
        })

    return out

"""
Given a sequence of raw per-video analytics responses (each for a single video),
flatten into one list of row dicts for bulk upsert.
The video_id is not always in the response... must pass responses
that already retain context OR each resp must contain a 'filters' echo.
For safety, we accept that fetch_video_daily_analytics *does not* include the video id
in rows—so caller should wrap transform_video_daily_response individually.
This helper is provided only if accumulate rows yourself.
"""
def transform_many_video_daily(responses: Sequence[dict]) -> List[dict]:
    # Example usage pattern in a calling script:
    #   all_rows = []
    #   for (video_id, resp) in video_responses:
    #       all_rows.extend(transform_video_daily_response(resp, video_id))
    return []  # Implement if standardize a structure containing (video_id, resp)
