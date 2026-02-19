"""
YouTube Shorts Product Giveaway "SCAM" Video Scraper
Targets fake product giveaway scam content (iPhone, iPad, AirPods, etc.)
for dataset labeling and classifier training.
Uses Selenium for discovery + yt-dlp for metadata + JSON duplicate tracking.
"""

import os
import json
import time
import socket
import random
from collections import deque
from urllib.parse import quote_plus
import yt_dlp
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ==================================================
# CONFIG
# ==================================================
OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "video_crawler_scam")
MAX_VIDEOS = 15           # change to 2000 later
SCROLL_ROUNDS = 15        # increase for better Shorts discovery
DOWNLOAD_VIDEOS = True    # set False to only scrape metadata
MIN_VIEW_COUNT = 500      # ignore very low-quality / spam content
MAX_DURATION = 60         # seconds — keep Shorts focus (change to None for all videos)

DUPLICATE_TRACKING_FILE = os.path.join(
    OUTPUT_DIR, "scraped_videos_index_youtube_shorts_product_scam.json"
)

# YouTube Shorts product giveaway scam queries
SEARCH_QUERIES = [
    # iPhone giveaways
    "free iPhone giveaway", "win free iPhone", "iPhone giveaway 2024",
    "iPhone giveaway 2025", "iPhone 15 giveaway", "iPhone 16 giveaway",
    "free iPhone 15", "free iPhone 16", "iPhone winner",
    "claim free iPhone", "iPhone giveaway winner",

    # iPad giveaways
    "free iPad giveaway", "win free iPad", "iPad giveaway 2024",
    "iPad giveaway 2025", "iPad Pro giveaway", "free iPad",
    "claim free iPad", "iPad winner giveaway",

    # AirPods giveaways
    "free AirPods giveaway", "win AirPods", "AirPods giveaway 2024",
    "AirPods giveaway 2025", "AirPods Pro giveaway", "free AirPods",
    "claim free AirPods", "AirPods winner",

    # Keyboard giveaways
    "free keyboard giveaway", "win gaming keyboard", "keyboard giveaway",
    "mechanical keyboard giveaway free", "free gaming keyboard",
    "keyboard winner giveaway",

    # Mouse giveaways
    "free gaming mouse giveaway", "win gaming mouse", "mouse giveaway",
    "free mouse giveaway", "gaming mouse giveaway winner",

    # Headset / headphones giveaways
    "free headset giveaway", "win gaming headset", "headset giveaway",
    "free headphones giveaway", "AirPods Max giveaway free",
    "Sony headphones giveaway", "free headphones winner",

    # General tech / gadget giveaways
    "free gadget giveaway", "win free tech", "free PS5 giveaway",
    "free Xbox giveaway", "free Nintendo Switch giveaway",
    "free Samsung giveaway", "free MacBook giveaway",
    "free laptop giveaway", "win free laptop", "free smartwatch giveaway",
    "free Apple Watch giveaway", "free gaming pc giveaway",

    # Scam patterns — comment / like / follow bait
    "like and win iPhone", "comment to win iPhone", "follow to win free iPhone",
    "subscribe win iPhone", "like to win free gadget",
    "comment win free AirPods", "follow win free headset",
    "tag friend win iPhone", "share to win free tech",

    # Fake prize / lottery
    "you won iPhone", "you won free gadget", "claim your prize iPhone",
    "free prize iPhone link bio", "dm to claim free iPhone",
    "free gift iPhone", "gift card iPhone giveaway",
]

# ==================================================
# KEYWORD SETS
# ==================================================

# Presence of ANY of these = likely a product giveaway scam — INCLUDE the video
SCAM_KEYWORDS = [
    # iPhone scam signals
    "free iphone", "iphone giveaway", "win iphone", "claim iphone",
    "iphone winner", "free iphone 15", "free iphone 16",

    # iPad scam signals
    "free ipad", "ipad giveaway", "win ipad", "claim ipad", "ipad winner",

    # AirPods scam signals
    "free airpods", "airpods giveaway", "win airpods", "claim airpods",
    "airpods winner", "free airpods pro", "airpods max giveaway",

    # Keyboard scam signals
    "free keyboard", "keyboard giveaway", "win keyboard",
    "free gaming keyboard", "claim keyboard",

    # Mouse scam signals
    "free mouse", "mouse giveaway", "win gaming mouse",
    "free gaming mouse", "claim mouse",

    # Headset / headphones scam signals
    "free headset", "headset giveaway", "win headset",
    "free headphones", "headphones giveaway", "win headphones",

    # General tech giveaway scam signals
    "free ps5", "ps5 giveaway", "free xbox", "xbox giveaway",
    "free nintendo switch", "switch giveaway", "free macbook",
    "macbook giveaway", "free laptop", "laptop giveaway",
    "free samsung", "samsung giveaway", "free smartwatch",
    "apple watch giveaway", "free gaming pc", "pc giveaway",
    "free gadget", "gadget giveaway", "win free tech",

    # Engagement-bait giveaway triggers
    "like and win", "comment to win", "follow to win",
    "subscribe win", "tag friend win", "share to win",
    "like to win", "comment win free", "follow win free",

    # Fake prize / lottery language
    "you won", "claim your prize", "you have been selected",
    "congratulations winner", "dm to claim", "link in bio free",
    "free gift", "gift card giveaway", "claim free gift",
    "working 2024", "working 2025", "100% real working",
]

# Presence of ANY of these = educational/legit — EXCLUDE the video
LEGIT_EXCLUSION_KEYWORDS = [
    "scam warning", "scam alert", "scam awareness", "avoid scam",
    "how to spot scam", "protect yourself", "red flags",
    "scam exposed", "scam explained", "scam analysis",
    "educational", "tutorial", "explained", "how to",
    "honest review", "unbiased", "honest opinion",
    "analysis", "breakdown", "deep dive", "review",
    "unboxing", "hands on", "first look",
]


# ==================================================
# DUPLICATE PREVENTION SYSTEM
# ==================================================
class DuplicateTracker:
    """Manages tracking of already-scraped videos to prevent duplicates."""

    def __init__(self, tracking_file):
        self.tracking_file = tracking_file
        self.scraped_videos = self._load_index()

    def _load_index(self):
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                print(f"✓ Loaded {len(data)} previously scraped videos from index")
                return data
            except Exception as e:
                print(f"⚠ Error loading index, starting fresh: {e}")
                return {}
        print("✓ Starting new video index")
        return {}

    def _save_index(self):
        try:
            os.makedirs(os.path.dirname(self.tracking_file), exist_ok=True)
            with open(self.tracking_file, "w", encoding="utf-8") as f:
                json.dump(self.scraped_videos, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠ Error saving index: {e}")

    def _normalize_youtube_url(self, url):
        import re
        shorts_match = re.search(r"/shorts/([a-zA-Z0-9_-]+)", url)
        if shorts_match:
            return f"https://www.youtube.com/shorts/{shorts_match.group(1)}"
        watch_match = re.search(r"[?&]v=([a-zA-Z0-9_-]+)", url)
        if watch_match:
            return f"https://www.youtube.com/shorts/{watch_match.group(1)}"
        return url.split("?")[0].split("&")[0]

    def is_duplicate(self, video_url, video_id=None):
        normalized_url = self._normalize_youtube_url(video_url)
        if normalized_url in self.scraped_videos:
            return True
        if video_id:
            for data in self.scraped_videos.values():
                if data.get("video_id") == video_id:
                    return True
        return False

    def add_video(self, video_url, video_id, metadata=None):
        normalized_url = self._normalize_youtube_url(video_url)
        self.scraped_videos[normalized_url] = {
            "video_id": video_id,
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "title": metadata.get("title", "") if metadata else "",
            "uploader": metadata.get("uploader", "") if metadata else "",
            "channel": metadata.get("channel", "") if metadata else "",
        }
        self._save_index()

    def get_stats(self):
        return {
            "total_scraped": len(self.scraped_videos),
            "oldest": min(
                (v["scraped_at"] for v in self.scraped_videos.values()), default=None
            ),
            "newest": max(
                (v["scraped_at"] for v in self.scraped_videos.values()), default=None
            ),
        }


# ==================================================
# UTILS
# ==================================================
def classify_category(text: str) -> str:
    """Auto-classify scam video into a subcategory based on keywords."""
    t = text.lower()
    if any(k in t for k in ["giveaway", "airdrop", "free bitcoin", "free crypto", "free eth"]):
        return "Crypto Giveaway"
    if any(k in t for k in ["double", "triple", "multiply", "doubler", "multiplier", "send btc", "send eth"]):
        return "Crypto Doubler"
    if any(k in t for k in ["guaranteed profit", "guaranteed returns", "risk free", "instant profit", "easy money", "get rich"]):
        return "Guaranteed Profit"
    if any(k in t for k in ["elon musk", "vitalik", "binance giveaway", "coinbase giveaway", "musk"]):
        return "Celebrity Impersonation"
    if any(k in t for k in ["generator", "hack", "adder", "bot", "working 2024", "working 2025"]):
        return "Crypto Generator/Hack"
    return "Crypto Scam General"


def is_scam(text: str) -> bool:
    """Return True if the text contains giveaway/scam signals and no legit-exclusion markers."""
    if not text:
        return False
    t = text.lower()
    # Exclude if it looks like an awareness/educational video about scams
    if any(k in t for k in LEGIT_EXCLUSION_KEYWORDS):
        return False
    # Accept if it contains scam markers
    return any(k in t for k in SCAM_KEYWORDS)


def extract_hashtags(description: str, tags: list) -> list:
    hashtags = []
    if description:
        hashtags.extend([w for w in description.split() if w.startswith("#")])
    if tags:
        hashtags.extend([f"#{tag}" for tag in tags if tag])
    return list(set(hashtags)) if hashtags else None


def setup_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # options.add_argument("--headless")  # uncomment to run without opening Chrome
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )


# ==================================================
# DISCOVERY
# ==================================================
def youtube_shorts_search_url(query):
    # sp=EgIYAQ%3D%3D filters results to Shorts only
    return f"https://www.youtube.com/results?search_query={quote_plus(query)}&sp=EgIYAQ%3D%3D"


def discover_video_links(driver, url):
    print(f"  Loading search page...")
    driver.get(url)
    time.sleep(5)

    for i in range(SCROLL_ROUNDS):
        driver.execute_script(
            "window.scrollBy(0, document.documentElement.scrollHeight);"
        )
        time.sleep(random.uniform(2, 3))
        print(f"  Scroll {i + 1}/{SCROLL_ROUNDS}")

    links = driver.execute_script("""
        return Array.from(document.querySelectorAll('a#video-title, a.ytd-thumbnail'))
            .map(a => a.href)
            .filter(h => h && (h.includes('shorts/') || h.includes('watch?v=')));
    """)

    unique_links = list(set(links))
    print(f"  Found {len(unique_links)} unique videos")
    return unique_links


# ==================================================
# METADATA EXTRACTION
# ==================================================
def extract_metadata(url):
    try:
        ydl_opts = {"quiet": True, "skip_download": True, "no_warnings": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        # Skip live streams
        if info.get("is_live") or info.get("was_live"):
            print("  ⊗ Skipping live stream")
            return None

        # Duration filter
        duration = info.get("duration", 0)
        if MAX_DURATION is not None and duration > MAX_DURATION:
            print(f"  ⊗ Too long ({duration}s > {MAX_DURATION}s) - skipped")
            return None

        # View count filter — skip very low-traffic / spam
        view_count = info.get("view_count", 0) or 0
        if view_count < MIN_VIEW_COUNT:
            print(f"  ⊗ Too few views ({view_count:,} < {MIN_VIEW_COUNT:,}) - skipped")
            return None

        title = info.get("title", "")
        description = info.get("description", "")
        tags = info.get("tags", [])
        text_blob = f"{title} {description} {' '.join(tags)}"

        if not is_scam(text_blob):
            print("  ⊗ Filtered out (does not meet scam content criteria)")
            return None

        hashtags = extract_hashtags(description, tags)
        video_id = info["id"]
        shorts_url = f"https://www.youtube.com/shorts/{video_id}"
        category = classify_category(text_blob)

        return {
            "video_id": f"youtube_{video_id}",
            "platform": "youtube",
            "video_url": shorts_url,
            "title": title,
            "description": description,
            "uploader": info.get("uploader"),
            "channel": info.get("channel"),
            "upload_date": info.get("upload_date"),
            "duration": duration,
            "view_count": view_count,
            "like_count": info.get("like_count"),
            "comment_count": info.get("comment_count"),
            "tags": tags if tags else [],
            "hashtags": hashtags,
            "is_short": True,
            "label": "SCAM",
            "category": category,
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "scraper_id": socket.gethostname(),
        }
    except Exception as e:
        print(f"  Error extracting metadata: {e}")
        return None


# ==================================================
# SAVE
# ==================================================
def save_metadata(meta):
    base = os.path.join(OUTPUT_DIR, "metadata", "youtube_shorts_crypto_scam")
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, f"{meta['video_id']}.json")
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        print(
            f"  ✓ Saved: {meta['video_id']} | {meta['view_count']:,} views"
            f" | [{meta['category']}]"
        )
        return True
    return False


def is_already_downloaded(video_id):
    """Return True if the mp4 file for this video already exists on disk."""
    path = os.path.join(
        OUTPUT_DIR, "videos", "youtube_shorts_crypto_scam", f"{video_id}.mp4"
    )
    return os.path.exists(path)


def download_video(url, video_id):
    if not DOWNLOAD_VIDEOS:
        return False

    base = os.path.join(OUTPUT_DIR, "videos", "youtube_shorts_crypto_scam")
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, f"{video_id}.mp4")

    if os.path.exists(path):
        print(f"  ⊗ Already downloaded: {video_id}")
        return True

    print("  Downloading video...")
    ydl_opts = {
        "outtmpl": path,
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  ⬇ Downloaded: {video_id} ({size_mb:.1f} MB)")
            return True
    except Exception as e:
        print(f"  Error downloading: {e}")
    return False


# ==================================================
# MAIN CRAWLER
# ==================================================
def main():
    print("=" * 70)
    print("YouTube Shorts Crypto SCAM / Giveaway Video Scraper")
    print(f"Min views: {MIN_VIEW_COUNT:,} | Target: {MAX_VIDEOS} videos")
    print("=" * 70)

    duplicate_tracker = DuplicateTracker(DUPLICATE_TRACKING_FILE)
    stats = duplicate_tracker.get_stats()
    print(f"✓ Previously scraped: {stats['total_scraped']} videos")
    if stats["oldest"]:
        print(f"  First scraped: {stats['oldest']}")
        print(f"  Last scraped:  {stats['newest']}")
    print("=" * 70)

    driver = setup_driver()
    visited = set()
    collected = 0
    downloaded = 0
    skipped_duplicates = 0
    queue = deque([youtube_shorts_search_url(q) for q in SEARCH_QUERIES])

    try:
        while queue and collected < MAX_VIDEOS:
            page = queue.popleft()
            print(f"\n[>] Crawling: {page[:80]}...")

            try:
                links = discover_video_links(driver, page)
            except Exception as e:
                print(f"  Error discovering links: {e}")
                continue

            for video_url in links:
                if video_url in visited or collected >= MAX_VIDEOS:
                    continue

                visited.add(video_url)

                # Check for duplicates before processing
                if duplicate_tracker.is_duplicate(video_url):
                    skipped_duplicates += 1
                    print(f"\n[DUPLICATE SKIPPED] {video_url[:60]}...")
                    print(
                        f"  ⊗ Already scraped previously"
                        f" (Total duplicates: {skipped_duplicates})"
                    )
                    continue

                print(f"\n[{collected + 1}/{MAX_VIDEOS}] Processing: {video_url[:60]}...")

                meta = extract_metadata(video_url)
                if not meta:
                    continue

                # Skip if the video file is already on disk
                if DOWNLOAD_VIDEOS and is_already_downloaded(meta["video_id"]):
                    print(f"  ⊗ Already downloaded: {meta['video_id']} — skipping")
                    skipped_duplicates += 1
                    continue

                # Secondary duplicate check by video ID
                if duplicate_tracker.is_duplicate(video_url, meta["video_id"]):
                    skipped_duplicates += 1
                    print(
                        f"  ⊗ Duplicate by video ID"
                        f" (Total duplicates: {skipped_duplicates})"
                    )
                    continue

                if save_metadata(meta):
                    collected += 1
                    duplicate_tracker.add_video(video_url, meta["video_id"], meta)

                if DOWNLOAD_VIDEOS:
                    if download_video(video_url, meta["video_id"]):
                        downloaded += 1

                print(
                    f"  ✓ Total collected: {collected}/{MAX_VIDEOS}"
                    f" | Downloaded: {downloaded}"
                    f" | Duplicates skipped: {skipped_duplicates}"
                )

                # Queue the channel's Shorts page for further discovery
                if meta.get("channel") and collected < MAX_VIDEOS:
                    channel_name = meta["channel"].replace(" ", "")
                    channel_shorts_url = (
                        f"https://www.youtube.com/@{channel_name}/shorts"
                    )
                    if channel_shorts_url not in visited:
                        queue.append(channel_shorts_url)
                        print("  + Added channel Shorts to queue")

                time.sleep(random.uniform(2, 5))

        print("\n" + "=" * 70)
        print("✓ SCRAPING COMPLETE!")
        print(f"  New videos collected:           {collected}")
        print(f"  Videos downloaded:              {downloaded}")
        print(f"  Duplicates skipped:             {skipped_duplicates}")

        final_stats = duplicate_tracker.get_stats()
        print(f"  Total unique videos in database: {final_stats['total_scraped']}")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n\n✗ Fatal error: {e}")
    finally:
        driver.quit()
        print(
            f"\nFinal count: {collected} new videos"
            f" | {skipped_duplicates} duplicates skipped"
        )
        print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
        print(f"\nFiles saved:")
        print(f"  └── {OUTPUT_DIR}/")
        print(
            f"      ├── scraped_videos_index_youtube_shorts_crypto_scam.json"
            f"  (duplicate tracking)"
        )
        print(f"      ├── metadata/youtube_shorts_crypto_scam/*.json")
        print(f"      └── videos/youtube_shorts_crypto_scam/*.mp4")


if __name__ == "__main__":
    main()
