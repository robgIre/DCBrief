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

# RSS feed sources — matching original Manus brief sources + extras
RSS_FEEDS = [
    # DC-specific (always relevant, no filtering needed)
    {"url": "https://www.datacenterdynamics.com/en/rss/", "name": "DatacenterDynamics"},
    {"url": "https://www.datacenterknowledge.com/rss.xml", "name": "Data Center Knowledge"},
    {"url": "https://datacenterfrontier.com/feed/", "name": "Datacenter Frontier"},
    {"url": "https://capacitymedia.com/feed/", "name": "Capacity Media"},
    {"url": "https://journal.uptimeinstitute.com/feed/", "name": "Uptime Institute"},
    {"url": "https://www.broadgroup.com/feed", "name": "BroadGroup"},
    {"url": "https://www.servethehome.com/feed/", "name": "ServeTheHome"},
    # Construction
    {"url": "https://www.constructiondive.com/feeds/news/", "name": "Construction Dive"},
    {"url": "https://www.enr.com/rss/articles", "name": "ENR"},
    # Tech news (filtered for DC/AI relevance)
    {"url": "https://feeds.bloomberg.com/technology/news.rss", "name": "Bloomberg Technology"},
    {"url": "https://techcrunch.com/feed/", "name": "TechCrunch"},
    {"url": "https://www.theverge.com/rss/index.xml", "name": "The Verge"},
    {"url": "https://feeds.arstechnica.com/arstechnica/technology-lab", "name": "Ars Technica"},
    # Newsletters / analysis
    {"url": "https://www.semianalysis.com/feed", "name": "SemiAnalysis"},
    {"url": "https://www.techmeme.com/feed.xml", "name": "Techmeme"},
]

# Podcast section is static/curated in the template — no RSS fetching needed

# Stock tickers to track
STOCK_TICKERS = [
    "EQIX", "DLR", "AMT", "CCI", "SBAC",  # DC REITs
    "GOOGL", "MSFT", "AMZN", "NVDA", "META",  # Hyperscalers
]

# Podcast cover colours not needed — podcasts are static in template

# ---------------------------------------------------------------------------
# META SUPPLIERS — tracked for dedicated "Supplier Watch" section
# ---------------------------------------------------------------------------

# Short supplier names that need word-boundary matching to avoid false positives
# e.g. "IES" must not match "companies", "technologies", "facilities"
SHORT_SUPPLIER_NAMES = {
    "DPR", "FTI", "HDR", "WPI", "MPS", "HITT",
    "Arup", "Syska",
}

# These names are TOO short/ambiguous even with word boundaries — only match longer forms
# "IES" removed (matches too many words), use "IES Holdings" or "IES Commercial" instead
# "Turner" removed (too common a surname), use "Turner Construction" instead
# "Holder" removed (common English word), use "Holder Construction" instead
# "Stoner" removed (ambiguous), use "Stoner Electric" instead
# "Southland" removed (common place name), use "Southland Industries" instead
# "Mortenson" removed (common surname), use "M.A. Mortenson" or full name only
# "Clayco" kept (unique enough)
# "Black Box" removed (common phrase), use "Black Box Network" instead
# "Direct Line" removed (common phrase / UK insurance company)

META_SUPPLIERS = {
    # General Contractors (GC)
    "DPR Construction": {"scope": "General Contractor", "initials": "DP"},
    "DPR": {"scope": "General Contractor", "initials": "DP"},
    "Fortis Construction": {"scope": "General Contractor", "initials": "FC"},
    "Fortis SGA": {"scope": "General Contractor", "initials": "FS"},
    "Hensel Phelps": {"scope": "General Contractor", "initials": "HP"},
    "Turner Construction": {"scope": "General Contractor", "initials": "TC"},
    "Je Dunn": {"scope": "General Contractor", "initials": "JD"},
    "JE Dunn": {"scope": "General Contractor", "initials": "JD"},
    "M.A. Mortenson": {"scope": "General Contractor", "initials": "MO"},
    "Mortenson Construction": {"scope": "General Contractor", "initials": "MO"},
    "Clayco": {"scope": "General Contractor", "initials": "CL"},
    "Hitt Contracting": {"scope": "General Contractor", "initials": "HT"},
    "HITT": {"scope": "General Contractor", "initials": "HT"},
    "Holder Construction": {"scope": "General Contractor", "initials": "HC"},
    # Trade Contractors (TC)
    "IES Holdings": {"scope": "Trade Contractor", "initials": "IE"},
    "IES Commercial": {"scope": "Trade Contractor", "initials": "IE"},
    "IES Communications": {"scope": "Trade Contractor", "initials": "IE"},
    "E2 Optics": {"scope": "Trade Contractor", "initials": "E2"},
    "Black Box Network": {"scope": "Trade Contractor", "initials": "BB"},
    "Black Box Corporation": {"scope": "Trade Contractor", "initials": "BB"},
    "Stoner Electric": {"scope": "Trade Contractor", "initials": "SE"},
    "MP Nexlevel": {"scope": "Trade Contractor", "initials": "MP"},
    "Team Linx": {"scope": "Trade Contractor", "initials": "TL"},
    # Integrators
    "FTI": {"scope": "Integrator", "initials": "FT"},
    "MC Dean": {"scope": "Integrator", "initials": "MC"},
    "Southland Industries": {"scope": "Integrator", "initials": "SI"},
    "McKinstry": {"scope": "Integrator", "initials": "MK"},
    "Mckinstry": {"scope": "Integrator", "initials": "MK"},
    "US Engineering": {"scope": "Integrator", "initials": "US"},
    "MPS Group": {"scope": "Integrator", "initials": "MP"},
    "CEI Modular": {"scope": "Integrator", "initials": "CM"},
    "WPI": {"scope": "Integrator", "initials": "WP"},
    # Engineers of Record (EOR)
    "AlfaTech": {"scope": "Engineer of Record", "initials": "AT"},
    "Stantec": {"scope": "Engineer of Record", "initials": "ST"},
    "HDR Inc": {"scope": "Engineer of Record", "initials": "HD"},
    "HDR": {"scope": "Engineer of Record", "initials": "HD"},
    "Syska Hennessy": {"scope": "Engineer of Record", "initials": "SH"},
    "Syska": {"scope": "Engineer of Record", "initials": "SH"},
    "Arup": {"scope": "Engineer of Record", "initials": "AR"},
}

# Keywords for categorisation
DEALS_KEYWORDS = [
    "acquisition", "acquire", "acquired", "merger", "merge",
    "buyout", "stake", "purchase", "takeover", "take over",
    "private equity", "joint venture", "IPO", "SPAC",
    "raises", "raised", "funding round", "series ",
    "closes fund", "closed fund", "capital raise",
]

LAND_KEYWORDS = [
    "land acquisition", "land deal", "land purchase",
    "acres", "hectares", "campus",
    "groundbreaking", "broke ground", "break ground",
    "planning permission", "zoning approval", "zoning permit",
    "new data center", "new data centre",
    "data center construction", "data centre construction",
    "data center site", "data centre site",
    "data center campus", "data centre campus",
    "data center development", "data centre development",
    "data center expansion", "data centre expansion",
    "data center facility", "data centre facility",
    "hyperscale campus", "hyperscale facility",
    "megawatt campus", "MW campus",
]

AI_KEYWORDS = [
    "artificial intelligence", "AI model", "AI training",
    "AI infrastructure", "AI chip", "AI accelerator",
    "GPU cluster", "GPU capacity", "GPU server",
    "large language model", "LLM", "generative AI",
    "machine learning infrastructure",
    "NVIDIA H100", "NVIDIA H200", "NVIDIA B200", "NVIDIA GB200",
    "AI workload", "AI compute", "AI factory",
    "inference", "training cluster",
    "neural network", "foundation model",
]

META_KEYWORDS = [
    "Meta data center", "Meta data centre",
    "Meta campus", "Meta hyperscale", "Meta infrastructure",
    "Meta AI", "Meta Platforms", "Meta LLC",
    "Zuckerberg data", "Zuckerberg AI",
    "Llama model", "Llama 4",
]

# Relevance filter — articles must match at least one of these to be included at all
# This ensures we only show news pertinent to the DC world
DC_RELEVANCE_KEYWORDS = [
    "data center", "data centre", "datacenter", "datacentre",
    "hyperscale", "colocation", "colo ", "colo-",
    "server farm", "cloud infrastructure",
    "rack", "cooling", "liquid cooling", "immersion cooling",
    "power density", "megawatt", " MW ", "gigawatt", " GW ",
    "UPS ", "uninterruptible", "generator", "power grid",
    "fiber", "fibre", "interconnect", "network fabric",
    "GPU", "AI chip", "AI training", "AI infrastructure",
    "NVIDIA", "AMD EPYC", "Intel Xeon",
    "Equinix", "Digital Realty", "CyrusOne", "QTS",
    "EdgeConneX", "Vantage", "CoreSite", "Switch",
    "Meta data", "Meta campus", "Meta AI",
    "cloud computing", "AWS", "Azure", "Google Cloud",
    "construction", "modular", "prefab",
    "renewable energy", "sustainability", "carbon",
    "nuclear power", "PPA ", "power purchase",
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

def is_dc_relevant(text):
    """Check if an article is relevant to the data centre industry."""
    return matches_keywords(text, DC_RELEVANCE_KEYWORDS)


def categorise_articles(articles):
    """Sort articles into categories. Each article appears in ONE section only."""
    supplier_news = []
    meta_news = []
    deals = []
    land = []
    ai_news = []
    industry_news = []

    used_titles = set()  # Track titles to prevent ANY duplicates

    # First pass: filter to DC-relevant articles only
    relevant_articles = []
    for article in articles:
        title = article["title"]
        if title in used_titles:
            continue
        full_text = f"{title} {article['summary']}"
        # Articles from DC-specific feeds (DatacenterDynamics, etc.) are always relevant
        dc_feeds = {"DatacenterDynamics", "Data Center Knowledge", "Datacenter Frontier",
                     "BroadGroup", "DatacenterMap", "Datacenters.com", "Uptime Institute"}
        if article["source"] in dc_feeds or is_dc_relevant(full_text):
            relevant_articles.append(article)

    # Second pass: categorise into sections (priority order, each article used ONCE)
    for article in relevant_articles:
        title = article["title"]
        if title in used_titles:
            continue

        full_text = f"{title} {article['summary']}"

        # 1. Check for Meta supplier mention (highest priority)
        supplier_name, supplier_info = find_matched_supplier(full_text)
        if supplier_name:
            used_titles.add(title)
            article["matched_supplier"] = supplier_name
            article["supplier_scope"] = supplier_info["scope"]
            article["supplier_initials"] = supplier_info["initials"]
            article["category"] = "Supplier"
            article["tag_class"] = "supplier"
            supplier_news.append(article)
            continue

        # 2. Check for Meta-related news
        if matches_keywords(full_text, META_KEYWORDS):
            used_titles.add(title)
            article["category"] = "Meta"
            article["tag_class"] = "meta-partner"
            meta_news.append(article)
            continue

        # 3. Check for deals/M&A
        if matches_keywords(full_text, DEALS_KEYWORDS):
            used_titles.add(title)
            article["category"] = "Deal"
            article["tag_class"] = "deals"
            deals.append(article)
            continue

        # 4. Check for land/construction
        if matches_keywords(full_text, LAND_KEYWORDS):
            used_titles.add(title)
            article["category"] = "Land"
            article["tag_class"] = "land"
            land.append(article)
            continue

        # 5. Check for AI & compute
        if matches_keywords(full_text, AI_KEYWORDS):
            used_titles.add(title)
            article["category"] = "AI"
            article["tag_class"] = "infra"
            ai_news.append(article)
            continue

        # 6. Everything else goes to industry news
        used_titles.add(title)
        article["category"] = "Industry"
        article["tag_class"] = "infra"
        industry_news.append(article)

    # Build top stories from the best of each category (no duplicates)
    top_stories = []
    # Take 1-2 from each populated category for the top stories section
    for source_list in [meta_news, deals, land, ai_news, supplier_news, industry_news]:
        if source_list and source_list[0] not in top_stories:
            top_stories.append(source_list[0])
        if len(top_stories) >= 5:
            break
    # Fill remaining from industry news
    for a in industry_news:
        if a not in top_stories:
            top_stories.append(a)
            if len(top_stories) >= 5:
                break

    return {
        "top_stories": top_stories[:6],
        "deals": deals[:8],
        "land": land[:8],
        "ai_news": ai_news[:8],
        "supplier_news": supplier_news[:10],
        "meta_news": meta_news[:6],
        "industry_news": industry_news[:8],
    }


# ---------------------------------------------------------------------------
# HTML GENERATION
# ---------------------------------------------------------------------------

def generate_html(categories, markets, edition_number):
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
    print(f"  AI & Compute: {len(categories['ai_news'])}")
    print(f"  Meta news: {len(categories['meta_news'])}")
    print(f"  Supplier mentions: {len(categories['supplier_news'])}")
    print(f"  Industry news: {len(categories['industry_news'])}")
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

    print("\n[2/2] Fetching market data...")
    markets = fetch_stock_data()

    # Categorise
    print("\nCategorising articles...")
    categories = categorise_articles(articles)

    # Generate HTML
    print("\nGenerating HTML...")
    generate_html(categories, markets, state["edition"])

    # Save state
    save_state(state)
    print("\nDone!")


if __name__ == "__main__":
    main()
