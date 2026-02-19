"""
YouTube Shorts Gift Card "NOT SCAM" / Legitimate Video Scraper
Targets educational, review-based, and awareness content about gift cards.
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
OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "video_crawler_legit")
MAX_VIDEOS = 15           # change to 2000 later
SCROLL_ROUNDS = 15        # increase for better Shorts discovery
DOWNLOAD_VIDEOS = True    # set False to only scrape metadata
MIN_VIEW_COUNT = 500      # ignore very low-quality / spam content
MAX_DURATION = 60         # seconds — keep Shorts focus (change to None for all videos)

DUPLICATE_TRACKING_FILE = os.path.join(
    OUTPUT_DIR, "scraped_videos_index_youtube_shorts_giftcards_legit.json"
)

# YouTube Shorts legitimate gift card queries
SEARCH_QUERIES = [
    # How-to and tutorials
    "how to use gift card", "how to redeem gift card", "gift card tutorial",
    "how to buy gift card", "gift card explained", "gift card for beginners",
    "how to check gift card balance", "gift card tips and tricks",
    "how to use amazon gift card", "how to use google play gift card",
    "how to use steam gift card", "how to use apple gift card",
    "how to use visa gift card", "how to use mastercard gift card",
    "how to use playstation gift card", "how to use xbox gift card",

    # Honest reviews
    "gift card honest review", "best gift cards 2024", "best gift cards 2025",
    "gift card worth it", "gift card comparison", "gift card pros and cons",
    "prepaid card review", "gift card vs cash", "digital gift card review",
    "egift card review", "gift card unboxing",

    # Safety and scam awareness
    "gift card scam warning", "how to spot gift card scam", "avoid gift card scam",
    "gift card fraud awareness", "gift card scam red flags", "gift card safety tips",
    "protect yourself gift card scam", "gift card scam explained",
    "irs gift card scam", "amazon gift card scam warning",
    "google play gift card scam awareness", "gift card scam alert",
    "never pay with gift card", "gift card scam not scam",

    # Reselling and deals (legitimate secondary market)
    "how to sell gift cards", "gift card resell tips", "gift card exchange guide",
    "cardcash review", "raise gift card review", "gift card granny review",
    "how to get discounted gift cards legitimately", "gift card deals tips",

    # News and consumer information
    "gift card news update", "gift card regulation", "gift card consumer rights",
    "gift card expiration rules", "gift card fees explained",
    "gift card fraud report", "gift card law explained",
]

# ==================================================
# KEYWORD SETS
# ==================================================

# Presence of ANY of these = likely legitimate/educational
LEGIT_KEYWORDS = [
    # Educational language
    "explained", "tutorial", "how to", "guide", "learn", "education",
    "beginner", "introduction", "basics", "overview", "lesson",
    "what is", "understanding", "walkthrough", "tips", "tricks",

    # Review / comparison language
    "review", "comparison", "honest", "pros and cons", "worth it",
    "unboxing", "real talk", "unbiased", "opinion", "thoughts",
    "best gift card", "top gift cards", "recommended",

    # Redemption / legitimate use language
    "redeem", "redemption", "balance check", "activate", "pin",
    "how to use", "step by step", "instructions", "how to buy",
    "purchase", "legitimate", "legit", "official",

    # Safety / awareness
    "scam warning", "avoid scam", "scam alert", "protect yourself",
    "safety tips", "how to spot", "red flags", "be careful",
    "scam awareness", "not a scam", "not scam", "fraud warning",
    "consumer protection", "fraud prevention", "never pay with gift card",
    "irs warning", "scam explained",

    # Specific platforms (signals legitimate context)
    "amazon gift card", "google play gift card", "apple gift card",
    "steam gift card", "visa gift card", "mastercard gift card",
    "playstation gift card", "xbox gift card", "netflix gift card",
    "walmart gift card", "target gift card", "starbucks gift card",

    # Reselling / deals (legitimate secondary market signals)
    "cardcash", "raise.com", "gift card granny", "resell gift card",
    "discounted gift card", "gift card exchange", "sell gift card",

    # News / regulatory
    "news", "update", "regulation", "consumer rights", "expiration",
    "fees", "law", "policy", "report", "ftc", "consumer alert",
]

# Presence of ANY of these = likely a scam — exclude the video
SCAM_EXCLUSION_KEYWORDS = [
    "free gift card", "free amazon gift card", "free google play",
    "free steam gift card", "free apple gift card", "free playstation",
    "free xbox gift card", "free netflix gift card", "free walmart gift card",
    "unlimited gift cards", "infinite gift cards", "gift card generator",
    "gift card hack", "gift card glitch", "gift card exploit",
    "free gift card codes", "gift card code generator", "working gift card codes",
    "100% working gift card", "gift card method working",
    "free robux gift card", "free v-bucks gift card", "free roblox gift card",
    "gift card giveaway unlimited", "win unlimited gift cards",
    "gift card trick free", "get free gift card fast",
    "gift card cheat", "gift card loophole free",
    "claim free gift card", "generate gift card codes free",
    "working 2024 gift card", "working 2025 gift card",
    "free gift card no survey", "gift card no verification free",
    "earn unlimited gift cards", "gift card money glitch",
    "100 dollar gift card free", "gift card instantly free",
    "link in bio free gift", "dm me free gift card",
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
    """All gift card content is labeled under a single unified category."""
    return "Gift Card General"


def is_legitimate(text: str) -> bool:
    """Return True if the text contains legitimate/educational gift card signals."""
    if not text:
        return False
    t = text.lower()
    # Exclude if it looks like a scam
    if any(k in t for k in SCAM_EXCLUSION_KEYWORDS):
        return False
    # Accept if it contains legitimate markers
    return any(k in t for k in LEGIT_KEYWORDS)


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

        if not is_legitimate(text_blob):
            print("  ⊗ Filtered out (does not meet legitimate content criteria)")
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
            "label": "NOT SCAM",
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
    base = os.path.join(OUTPUT_DIR, "metadata", "youtube_shorts_giftcards_legit")
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
        OUTPUT_DIR, "videos", "youtube_shorts_giftcards_legit", f"{video_id}.mp4"
    )
    return os.path.exists(path)


def download_video(url, video_id):
    if not DOWNLOAD_VIDEOS:
        return False

    base = os.path.join(OUTPUT_DIR, "videos", "youtube_shorts_giftcards_legit")
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
    print("YouTube Shorts Gift Card NOT SCAM / Legitimate Video Scraper")
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
            f"      ├── scraped_videos_index_youtube_shorts_giftcards_legit.json"
            f"  (duplicate tracking)"
        )
        print(f"      ├── metadata/youtube_shorts_giftcards_legit/*.json")
        print(f"      └── videos/youtube_shorts_giftcards_legit/*.mp4")


if __name__ == "__main__":
    main()
