thonimport json
import logging
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set

from playwright.async_api import Page, ElementHandle

@dataclass
class FacebookPost:
    facebookUrl: Optional[str] = None
    pageId: Optional[str] = None
    postId: Optional[str] = None
    pageName: Optional[str] = None
    url: Optional[str] = None
    time: Optional[str] = None
    timestamp: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    text: Optional[str] = None
    link: Optional[str] = None
    thumb: Optional[str] = None
    topLevelUrl: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

class FacebookPostParser:
    """
    Encapsulates logic for extracting structured post data from a Facebook
    search result page. The CSS selectors are intentionally defensive and
    may need refinement for specific layouts or locales.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    async def extract_posts(
        self,
        page: Page,
        seen_post_ids: Optional[Set[str]] = None,
        max_posts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract posts that are currently visible on the page.

        :param page: Playwright Page instance.
        :param seen_post_ids: Set of already processed post IDs.
        :param max_posts: Optional max count for extraction on this pass.
        """
        if seen_post_ids is None:
            seen_post_ids = set()

        posts: List[Dict[str, Any]] = []

        article_selector = "div[role='article']"
        article_handles: List[ElementHandle] = await page.query_selector_all(article_selector)

        for article in article_handles:
            try:
                post = await self._parse_article(article)
                if not post.postId:
                    # Fallback identifier based on content hash
                    base = (post.text or "") + (post.url or "")
                    if not base.strip():
                        continue
                    post.postId = str(abs(hash(base)))
                if post.postId in seen_post_ids:
                    continue
                seen_post_ids.add(post.postId)
                posts.append(post.as_dict())
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("Failed to parse an article: %s", exc, exc_info=True)

            if max_posts is not None and len(posts) >= max_posts:
                break

        return posts

    async def _parse_article(self, article: ElementHandle) -> FacebookPost:
        """
        Parse a single article element into a FacebookPost dataclass.
        """
        post = FacebookPost()
        data_ft_raw = await article.get_attribute("data-ft")

        if data_ft_raw:
            try:
                data_ft = json.loads(data_ft_raw)
            except Exception:  # noqa: BLE001
                data_ft = {}
        else:
            data_ft = {}

        # Extract core IDs where available
        post.pageId = self._safe_get_nested(data_ft, ["page_id", "page_id"])
        post.postId = self._safe_get_nested(data_ft, ["top_level_post_id", "mf_story_key"])

        # Page URL and name
        page_link = await article.query_selector("h3 a[href*='facebook.com/']")
        if page_link:
            post.facebookUrl = await page_link.get_attribute("href")
            post.pageName = (await page_link.inner_text()) or None

        # Post URL (top-level)
        post_link = await article.query_selector("a[href*='/posts/'], a[href*='pfbid']")
        if post_link:
            post.url = await post_link.get_attribute("href")
            post.topLevelUrl = post.url

        # Timestamp
        time_node = await article.query_selector("a[aria-label*=' at '], a[role='link'] time")
        timestamp = None
        time_str = None
        if time_node:
            try:
                time_str = await time_node.get_attribute("datetime")
            except Exception:  # noqa: BLE001
                time_str = None

            if not time_str:
                try:
                    # fallback: visible text
                    time_str = await time_node.inner_text()
                except Exception:  # noqa: BLE001
                    time_str = None

            # Some time nodes embed UNIX timestamp as data-utime
            try:
                time_attr = await time_node.get_attribute("data-utime")
                if time_attr and time_attr.isdigit():
                    timestamp = int(time_attr)
            except Exception:  # noqa: BLE001
                timestamp = None

        if not timestamp:
            timestamp = int(time.time())

        post.time = time_str
        post.timestamp = timestamp

        # Text content
        text_node = await article.query_selector("div[dir='auto'] span, div[dir='auto']")
        post.text = (await text_node.inner_text()) if text_node else None

        # External link
        link_node = await article.query_selector("a[rel~='noopener'], a[rel~='nofollow']")
        post.link = await link_node.get_attribute("href") if link_node else None

        # Thumbnail image
        thumb_node = await article.query_selector("img[src][referrerpolicy]")
        post.thumb = await thumb_node.get_attribute("src") if thumb_node else None

        # Engagement metrics (likes, comments, shares)
        likes, comments, shares = await self._parse_engagement(article)
        post.likes = likes
        post.comments = comments
        post.shares = shares

        return post

    async def _parse_engagement(
        self,
        article: ElementHandle,
    ) -> (Optional[int], Optional[int], Optional[int]):
        """
        Attempt to parse likes, comments and shares from the article footer.
        """
        likes = comments = shares = None

        selector = "span[dir='auto'], div[dir='auto']"
        nodes = await article.query_selector_all(selector)
        for node in nodes:
            try:
                txt = (await node.inner_text()).strip()
            except Exception:  # noqa: BLE001
                continue

            lower = txt.lower()
            if "like" in lower:
                likes_value = self._parse_engagement_number(txt)
                if likes_value is not None:
                    likes = likes_value
            elif "comment" in lower:
                comments_value = self._parse_engagement_number(txt)
                if comments_value is not None:
                    comments = comments_value
            elif "share" in lower:
                shares_value = self._parse_engagement_number(txt)
                if shares_value is not None:
                    shares = shares_value

        return likes, comments, shares

    def _parse_engagement_number(self, text: str) -> Optional[int]:
        """
        Convert engagement label like '2.4K' or '1,234' into an integer.
        """
        match = re.search(r"([\d.,]+)\s*[KkMm]?", text)
        if not match:
            return None

        number_str = match.group(1).replace(",", "")
        try:
            value = float(number_str)
        except ValueError:
            return None

        multiplier = 1
        if re.search(r"[Kk]", text):
            multiplier = 1_000
        elif re.search(r"[Mm]", text):
            multiplier = 1_000_000

        return int(value * multiplier)

    @staticmethod
    def _safe_get_nested(data: Any, keys: List[str]) -> Optional[str]:
        """
        Safely traverse nested JSON-like structures with fallbacks.
        """
        if not isinstance(data, dict):
            return None
        for key in keys:
            if key in data:
                value = data.get(key)
                return str(value) if value is not None else None
        return None