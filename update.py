#!/usr/bin/env python3
"""
DC Morning Brief — Daily Update Script
Fetches RSS feeds from data centre industry sources, stock market data,
categorises articles, and generates a static HTML page.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

import feedparser
import yfinance as yf
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
TEMPLATE_FILE = "template.html"
OUTPUT_FILE = "index.html"
STATE_FILE = "state.json"

# RSS feed sources
RSS_FEEDS = [
    {"url": "https://www.datacenterdynamics.com/en/rss/", "name": "DatacenterDynamics"},
    {"url": "https://www.datacenterknowledge.com/rss.xml", "name": "Data Center Knowledge"},
    {"url": "https://www.theregister.com/data_centre/headlines.atom", "name": "The Register"},
    {"url": "https://capacitymedia.com/feed/", "name": "Capacity Media"},
    {"url": "https://journal.uptimeinstitute.com/feed/", "name": "Uptime Institute"},
    {"url": "https://datacenterfrontier.com/feed/", "name": "Datacenter Frontier"},
    {"url": "https://www.broadgroup.com/feed", "name": "BroadGroup"},
    {"url": "https://www.datacentermap.com/feed/", "name": "DatacenterMap"},
    {"url": "https://siliconangle.com/category/datacenter/feed/", "name": "SiliconANGLE"},
    {"url": "https://www.servethehome.com/feed/", "name": "ServeTheHome"},
    {"url": "https://www.datacenters.com/news/rss", "name": "Datacenters.com"},
    {"url": "https://www.constructiondive.com/feeds/news/", "name": "Construction Dive"},
]

# Podcast RSS feeds
PODCAST_FEEDS = [
    {"url": "https://feeds.megaphone.fm/ROAM7120986792", "name": "Utilizing Tech"},
    {"url": "https://feeds.transistor.fm/data-center-podcast", "name": "Data Center Podcast"},
    {"url": "https://feeds.megaphone.fm/datacenterworld", "name": "Data Center World"},
    {"url": "https://anchor.fm/s/5e2a4f00/podcast/rss", "name": "DC Frontier Podcast"},
    {"url": "https://www.spreaker.com/show/5765098/episodes/feed", "name": "Uptime Institute"},
]

# Stock tickers to track
STOCK_TICKERS = [
    "EQIX", "DLR", "AMT", "CCI", "SBAC",  # DC REITs
    "GOOGL", "MSFT", "AMZN", "NVDA", "META",  # Hyperscalers
]

# Podcast cover colours (rotating)
PODCAST_COLORS = [
    ("#e11d48", "#ec4899"),
    ("#2563eb", "#0891b2"),
    ("#16a34a", "#0d9488"),
    ("#d97706", "#ea580c"),
    ("#7c3aed", "#6366f1"),
]

# ---------------------------------------------------------------------------
# META SUPPLIERS — tracked for dedicated "Supplier Watch" section
# ---------------------------------------------------------------------------

# Short supplier names that need word-boundary matching to avoid false positives
# e.g. "IES" must not match "companies", "technologies", "facilities"
SHORT_SUPPLIER_NAMES = {
    "DPR", "IES", "FTI", "HDR", "WPI", "MPS", "HITT",
    "Turner", "Holder", "Stoner", "Arup", "Syska",
    "Southland", "Mortenson", "Clayco",
}

META_SUPPLIERS = {
    # General Contractors (GC)
    "DPR": {"scope": "General Contractor", "initials": "DP"},
    "DPR Construction": {"scope": "General Contractor", "initials": "DP"},
    "Fortis Construction": {"scope": "General Contractor", "initials": "FC"},
    "Fortis SGA": {"scope": "General Contractor", "initials": "FS"},
    "Hensel Phelps": {"scope": "General Contractor", "initials": "HP"},
    "Turner Construction": {"scope": "General Contractor", "initials": "TC"},
    "Turner": {"scope": "General Contractor", "initials": "TC"},
    "Je Dunn": {"scope": "General Contractor", "initials": "JD"},
    "JE Dunn": {"scope": "General Contractor", "initials": "JD"},
    "Mortenson": {"scope": "General Contractor", "initials": "MO"},
    "M.A. Mortenson": {"scope": "General Contractor", "initials": "MO"},
    "Clayco": {"scope": "General Contractor", "initials": "CL"},
    "Hitt Contracting": {"scope": "General Contractor", "initials": "HT"},
    "HITT": {"scope": "General Contractor", "initials": "HT"},
    "Holder Construction": {"scope": "General Contractor", "initials": "HC"},
    "Holder": {"scope": "General Contractor", "initials": "HC"},
    # Trade Contractors (TC)
    "IES Holdings": {"scope": "Trade Contractor", "initials": "IE"},
    "IES": {"scope": "Trade Contractor", "initials": "IE"},
    "E2 Optics": {"scope": "Trade Contractor", "initials": "E2"},
    "Direct Line": {"scope": "Trade Contractor", "initials": "DL"},
    "Black Box": {"scope": "Trade Contractor", "initials": "BB"},
    "Black Box Network": {"scope": "Trade Contractor", "initials": "BB"},
    "Stoner Electric": {"scope": "Trade Contractor", "initials": "SE"},
    "Stoner": {"scope": "Trade Contractor", "initials": "SE"},
    "MP Nexlevel": {"scope": "Trade Contractor", "initials": "MP"},
    "Team Linx": {"scope": "Trade Contractor", "initials": "TL"},
    # Integrators
    "FTI": {"scope": "Integrator", "initials": "FT"},
    "MC Dean": {"scope": "Integrator", "initials": "MC"},
    "Southland Industries": {"scope": "Integrator", "initials": "SI"},
    "Southland": {"scope": "Integrator", "initials": "SI"},
    "McKinstry": {"scope": "Integrator", "initials": "MK"},
    "Mckinstry": {"scope": "Integrator", "initials": "MK"},
    "US Engineering": {"scope": "Integrator", "initials": "US"},
    "MPS Group": {"scope": "Integrator", "initials": "MP"},
    "CEI Modular": {"scope": "Integrator", "initials": "CM"},
    "WPI": {"scope": "Integrator", "initials": "WP"},
    # Engineers of Record (EOR)
    "AlfaTech": {"scope": "Engineer of Record", "initials": "AT"},
    "Stantec": {"scope": "Engineer of Record", "initials": "ST"},
    "HDR": {"scope": "Engineer of Record", "initials": "HD"},
    "HDR Inc": {"scope": "Engineer of Record", "initials": "HD"},
    "Syska Hennessy": {"scope": "Engineer of Record", "initials": "SH"},
    "Syska": {"scope": "Engineer of Record", "initials": "SH"},
    "Arup": {"scope": "Engineer of Record", "initials": "AR"},
}

# Keywords for categorisation
DEALS_KEYWORDS = [
    "acquisition", "acquire", "acquired", "merger", "merge", "deal",
    "fund", "investment", "invest", "buyout", "stake", "purchase",
    "financing", "billion", "million", "capital", "valuation",
    "joint venture", "partnership", "IPO", "SPAC", "private equity",
]

LAND_KEYWORDS = [
    "land", "site", "campus", "acres", "hectares", "build",
    "construction", "planning permission", "groundbreaking",
    "development", "facility", "megawatt", "MW capacity",
    "new data center", "new data centre", "expansion",
    "zoning", "permit", "broke ground",
]

META_KEYWORDS = [
    "Meta ", "Meta's", "Facebook", "Instagram infrastructure",
    "Zuckerberg", "Meta Platforms", "Meta AI",
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_state():
    """Load edition counter from state file."""
    state_path = SCRIPT_DIR / STATE_FILE
    if state_path.exists():
        with open(state_path) as f:
            return json.load(f)
    return {"edition": 432, "last_run": None}


def save_state(state):
    """Save edition counter to state file."""
    state_path = SCRIPT_DIR / STATE_FILE
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def clean_html(text):
    """Strip HTML tags and clean up text."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Truncate long summaries
    if len(text) > 300:
        text = text[:297] + "..."
    return text


def time_ago(published):
    """Convert a published datetime to a human-readable 'time ago' string."""
    if not published:
        return "today"
    try:
        from email.utils import parsedate_to_datetime
        if isinstance(published, str):
            dt = parsedate_to_datetime(published)
        else:
            dt = datetime(*published[:6], tzinfo=timezone.utc)
    except Exception:
        return "today"

    now = datetime.now(timezone.utc)
    diff = now - dt
    hours = int(diff.total_seconds() / 3600)
    if hours < 1:
        return "just now"
    elif hours < 24:
        return f"{hours}h ago"
    elif hours < 48:
        return "yesterday"
    else:
        return f"{hours // 24}d ago"


def matches_keywords(text, keywords):
    """Check if text contains any of the given keywords (case-insensitive)."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def find_matched_supplier(text):
    """Check if text mentions any Meta supplier. Returns (supplier_name, info) or None."""
    # Sort by length descending to match longer names first
    for name in sorted(META_SUPPLIERS.keys(), key=len, reverse=True):
        # Use word boundary matching for short/ambiguous names
        if name in SHORT_SUPPLIER_NAMES:
            if re.search(r'\b' + re.escape(name) + r'\b', text, re.IGNORECASE):
                return name, META_SUPPLIERS[name]
        else:
            if name.lower() in text.lower():
                return name, META_SUPPLIERS[name]
    return None, None


# ---------------------------------------------------------------------------
# DATA FETCHING
# ---------------------------------------------------------------------------

def fetch_rss_feeds():
    """Fetch and parse all RSS feeds. Returns list of article dicts."""
    articles = []
    for feed_info in RSS_FEEDS:
        try:
            print(f"  Fetching: {feed_info['name']}...")
            feed = feedparser.parse(feed_info["url"])
            for entry in feed.entries[:15]:  # max 15 per feed
                title = clean_html(entry.get("title", ""))
                summary = clean_html(
                    entry.get("summary", entry.get("description", ""))
                )
                link = entry.get("link", "")
                published = entry.get("published_parsed", entry.get("updated_parsed"))

                if not title:
                    continue

                articles.append({
                    "title": escape(title),
                    "summary": escape(summary),
                    "link": link,
                    "source": feed_info["name"],
                    "published": published,
                    "time_ago": time_ago(published or entry.get("published", "")),
                })
        except Exception as e:
            print(f"  Warning: Failed to fetch {feed_info['name']}: {e}")

    # Sort by published date (newest first)
    def sort_key(a):
        if a["published"]:
            try:
                return datetime(*a["published"][:6])
            except Exception:
                pass
        return datetime(2000, 1, 1)

    articles.sort(key=sort_key, reverse=True)
    return articles


def fetch_podcasts():
    """Fetch podcast episodes from RSS feeds."""
    episodes = []
    for i, feed_info in enumerate(PODCAST_FEEDS):
        try:
            print(f"  Fetching podcast: {feed_info['name']}...")
            feed = feedparser.parse(feed_info["url"])
            for entry in feed.entries[:2]:  # latest 2 per podcast
                title = clean_html(entry.get("title", ""))
                summary = clean_html(
                    entry.get("summary", entry.get("description", ""))
                )
                link = entry.get("link", "")
                duration = entry.get("itunes_duration", "")

                if not title:
                    continue

                # Format duration
                if duration:
                    try:
                        if ":" in str(duration):
                            parts = str(duration).split(":")
                            if len(parts) == 3:
                                mins = int(parts[0]) * 60 + int(parts[1])
                            else:
                                mins = int(parts[0])
                            duration = f"{mins} min"
                        else:
                            mins = int(duration) // 60
                            duration = f"{mins} min"
                    except (ValueError, TypeError):
                        duration = ""

                colors = PODCAST_COLORS[i % len(PODCAST_COLORS)]
                episodes.append({
                    "title": escape(title),
                    "summary": escape(summary),
                    "link": link,
                    "show": feed_info["name"],
                    "duration": duration,
                    "color_from": colors[0],
                    "color_to": colors[1],
                })
        except Exception as e:
            print(f"  Warning: Failed to fetch podcast {feed_info['name']}: {e}")

    return episodes[:6]  # max 6 podcast episodes


def fetch_stock_data():
    """Fetch current stock prices for tracked tickers."""
    markets = []
    try:
        print("  Fetching stock data...")
        tickers = yf.Tickers(" ".join(STOCK_TICKERS))
        for symbol in STOCK_TICKERS:
            try:
                ticker = tickers.tickers[symbol]
                info = ticker.fast_info
                price = info.get("lastPrice", 0) or info.get("last_price", 0)
                prev_close = info.get("previousClose", 0) or info.get("previous_close", price)
                if price and prev_close:
                    change = price - prev_close
                    change_pct = (change / prev_close) * 100
                else:
                    change = 0
                    change_pct = 0

                markets.append({
                    "symbol": symbol,
                    "price": round(price, 2),
                    "change": round(change, 2),
                    "change_pct": round(change_pct, 2),
                })
            except Exception as e:
                print(f"  Warning: Failed to fetch {symbol}: {e}")
                markets.append({
                    "symbol": symbol,
                    "price": 0,
                    "change": 0,
                    "change_pct": 0,
                })
    except Exception as e:
        print(f"  Warning: Stock data fetch failed: {e}")
        # Return placeholder data
        for symbol in STOCK_TICKERS:
            markets.append({
                "symbol": symbol,
                "price": 0,
                "change": 0,
                "change_pct": 0,
            })

    return markets


# ---------------------------------------------------------------------------
# CATEGORISATION
# ---------------------------------------------------------------------------

def categorise_articles(articles):
    """Sort articles into categories based on keyword matching."""
    top_stories = []
    deals = []
    land = []
    supplier_news = []
    industry_news = []

    seen_titles = set()

    for article in articles:
        title = article["title"]
        # Deduplicate
        if title in seen_titles:
            continue
        seen_titles.add(title)

        full_text = f"{title} {article['summary']}"

        # Check for Meta supplier mention
        supplier_name, supplier_info = find_matched_supplier(full_text)
        if supplier_name:
            article["matched_supplier"] = supplier_name
            article["supplier_scope"] = supplier_info["scope"]
            article["supplier_initials"] = supplier_info["initials"]
            article["category"] = "Supplier"
            article["tag_class"] = "supplier"
            supplier_news.append(article)
            continue

        # Check for deals/M&A
        if matches_keywords(full_text, DEALS_KEYWORDS):
            article["category"] = "Deal"
            article["tag_class"] = "deals"
            deals.append(article)
            continue

        # Check for land/development
        if matches_keywords(full_text, LAND_KEYWORDS):
            article["category"] = "Land"
            article["tag_class"] = "land"
            land.append(article)
            continue

        # Check for Meta-related
        if matches_keywords(full_text, META_KEYWORDS):
            article["category"] = "Meta"
            article["tag_class"] = "meta-partner"
            top_stories.append(article)
            continue

        # Default to industry news
        article["category"] = "Industry"
        article["tag_class"] = "infra"
        industry_news.append(article)

    # If we don't have enough top stories, promote from other categories
    if len(top_stories) < 5:
        # Take the most recent articles overall as top stories
        remaining_needed = 5 - len(top_stories)
        all_other = deals[:1] + land[:1] + industry_news[:remaining_needed]
        for a in all_other:
            if a not in top_stories:
                top_stories.append(a)
                if len(top_stories) >= 5:
                    break

    return {
        "top_stories": top_stories[:8],
        "deals": deals[:10],
        "land": land[:8],
        "supplier_news": supplier_news[:12],
        "industry_news": industry_news[:10],
    }


# ---------------------------------------------------------------------------
# HTML GENERATION
# ---------------------------------------------------------------------------

def generate_html(categories, markets, podcasts, edition_number):
    """Render the Jinja2 template with live data."""
    now = datetime.now(timezone.utc)

    # Determine greeting based on UK time (UTC)
    hour = now.hour
    if hour < 12:
        greeting = "Good Morning"
    elif hour < 17:
        greeting = "Good Afternoon"
    else:
        greeting = "Good Evening"

    total_stories = sum(
        len(v) for v in categories.values()
    )

    template_data = {
        "date_display": now.strftime("%A, %B %d, %Y").upper(),
        "greeting": greeting,
        "edition_number": edition_number,
        "markets": markets,
        "total_stories": total_stories,
        "feed_count": len(RSS_FEEDS),
        "update_time": now.strftime("%H:%M"),
        "update_date": now.strftime("%d %b"),
        **categories,
        "podcasts": podcasts,
    }

    env = Environment(
        loader=FileSystemLoader(str(SCRIPT_DIR)),
        autoescape=False,  # We handle escaping in the data
    )
    template = env.get_template(TEMPLATE_FILE)
    html = template.render(**template_data)

    output_path = SCRIPT_DIR / OUTPUT_FILE
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Generated {output_path}")
    print(f"  Edition: #{edition_number}")
    print(f"  Total stories: {total_stories}")
    print(f"  Top stories: {len(categories['top_stories'])}")
    print(f"  Deals: {len(categories['deals'])}")
    print(f"  Land: {len(categories['land'])}")
    print(f"  Supplier mentions: {len(categories['supplier_news'])}")
    print(f"  Industry news: {len(categories['industry_news'])}")
    print(f"  Podcasts: {len(podcasts)}")
    print(f"  Market tickers: {len(markets)}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 50)
    print("DC Morning Brief — Daily Update")
    print("=" * 50)

    # Load state
    state = load_state()
    state["edition"] = state.get("edition", 432) + 1
    state["last_run"] = datetime.now(timezone.utc).isoformat()

    # Fetch data
    print("\n[1/3] Fetching RSS feeds...")
    articles = fetch_rss_feeds()
    print(f"  Found {len(articles)} articles total")

    print("\n[2/3] Fetching market data...")
    markets = fetch_stock_data()

    print("\n[3/3] Fetching podcasts...")
    podcasts = fetch_podcasts()

    # Categorise
    print("\nCategorising articles...")
    categories = categorise_articles(articles)

    # Generate HTML
    print("\nGenerating HTML...")
    generate_html(categories, markets, podcasts, state["edition"])

    # Save state
    save_state(state)
    print("\nDone!")


if __name__ == "__main__":
    main()
