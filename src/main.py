thonimport argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure local src directory is on sys.path so we can import subpackages
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from extractors.scroll_manager import ScrollManager  # type: ignore
from extractors.facebook_parser import FacebookPostParser  # type: ignore
from utils.logger import setup_logging, get_logger  # type: ignore
from utils.formatter import ExportFormat, export_posts  # type: ignore

logger = get_logger(__name__)

def load_settings() -> Dict[str, Any]:
    """
    Load settings from config/settings.json.
    """
    config_path = CURRENT_DIR / "config" / "settings.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Settings file not found at {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def parse_args(settings: Dict[str, Any]) -> argparse.Namespace:
    facebook_settings = settings.get("facebook", {})
    export_settings = settings.get("export", {})

    parser = argparse.ArgumentParser(
        description="Facebook Posts Search Scraper - search public posts and export structured data."
    )
    parser.add_argument(
        "--query",
        "-q",
        required=True,
        help="Search query to run on Facebook (e.g. 'Andrés Iniesta retirement').",
    )
    parser.add_argument(
        "--max-posts",
        "-n",
        type=int,
        default=int(facebook_settings.get("max_posts", 100)),
        help="Maximum number of posts to scrape (default from settings.json).",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=[f.value for f in ExportFormat],
        default=export_settings.get("default_format", "json"),
        help="Export format for scraped data.",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path. If omitted, a name will be generated under the configured output_dir.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Force headless browser (overrides config).",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Force visible browser window (overrides config).",
    )
    return parser.parse_args()

def resolve_output_path(
    output: Optional[str],
    export_format: ExportFormat,
    settings: Dict[str, Any],
) -> Path:
    export_settings = settings.get("export", {})
    output_dir = export_settings.get("output_dir", "data")
    root_dir = CURRENT_DIR.parent
    base_dir = (root_dir / output_dir).resolve()

    if output:
        out_path = Path(output).expanduser().resolve()
        if not out_path.is_absolute():
            out_path = base_dir / out_path
    else:
        base_dir.mkdir(parents=True, exist_ok=True)
        sanitized_query = "results"
        out_path = base_dir / f"{sanitized_query}.{export_format.value}"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path

async def run_scraper(
    query: str,
    max_posts: int,
    export_format: ExportFormat,
    output_path: Path,
    settings: Dict[str, Any],
    force_headless: Optional[bool] = None,
) -> None:
    facebook_settings = settings.get("facebook", {})
    browser_settings = settings.get("browser", {})

    base_search_url: str = facebook_settings.get(
        "base_search_url", "https://www.facebook.com/search/posts?q={query}"
    )
    scroll_pause: float = float(facebook_settings.get("scroll_pause", 2.0))
    max_scroll: int = int(facebook_settings.get("max_scroll", 100))

    headless_config: bool = bool(browser_settings.get("headless", True))
    if force_headless is True:
        headless = True
    elif force_headless is False:
        headless = False
    else:
        headless = headless_config

    user_agent: Optional[str] = browser_settings.get("user_agent")
    locale: Optional[str] = browser_settings.get("locale", "en-US")

    logger.info("Starting Facebook search scraper")
    logger.info("Query: %s | Max posts: %d | Format: %s", query, max_posts, export_format.value)

    parser = FacebookPostParser(logger=logger)

    async with ScrollManager(
        base_search_url=base_search_url,
        scroll_pause=scroll_pause,
        max_scroll=max_scroll,
        headless=headless,
        user_agent=user_agent,
        locale=locale,
        logger=logger,
    ) as manager:
        posts: List[Dict[str, Any]] = await manager.scrape_search(query, parser, max_posts)

    if not posts:
        logger.warning("No posts were extracted for query '%s'.", query)
    else:
        logger.info("Extracted %d posts.", len(posts))

    export_posts(posts, output_path, export_format)
    logger.info("Export complete: %s", output_path)

def main() -> None:
    settings = load_settings()
    setup_logging(settings.get("logging", {}))
    global logger
    logger = get_logger(__name__)

    args = parse_args(settings)

    export_format = ExportFormat(args.format)
    output_path = resolve_output_path(args.output, export_format, settings)

    force_headless: Optional[bool]
    if args.headless and args.no_headless:
        logger.warning("Both --headless and --no-headless specified, falling back to config.")
        force_headless = None
    elif args.headless:
        force_headless = True
    elif args.no_headless:
        force_headless = False
    else:
        force_headless = None

    try:
        asyncio.run(
            run_scraper(
                query=args.query,
                max_posts=args.max_posts,
                export_format=export_format,
                output_path=output_path,
                settings=settings,
                force_headless=force_headless,
            )
        )
    except KeyboardInterrupt:
        logger.warning("Scraper interrupted by user.")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fatal error in scraper: %s", exc)
        raise

if __name__ == "__main__":
    main()