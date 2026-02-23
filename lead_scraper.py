import asyncio
import logging
import os
import random
import re
import time
from functools import wraps
from typing import Any, Callable, TypeVar

import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter to prevent request spam."""

    def __init__(self, min_delay: float = 1.0, max_delay: float = 3.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_request_time = 0.0

    async def wait(self) -> None:
        """Wait appropriate time before next request."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            wait_time = (
                self.min_delay
                - elapsed
                + random.uniform(0, self.max_delay - self.min_delay)
            )
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()


def retry_on_failure(max_retries: int = 3, delay: float = 1.0) -> Callable[[F], F]:
    """Decorator to retry async functions on failure."""

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying..."
                        )
                        await asyncio.sleep(delay * (attempt + 1))
                    else:
                        logger.error(f"All {max_retries} attempts failed")
            raise last_exception  # type: ignore[arg-type]

        return wrapper  # type: ignore[return-value]

    return decorator  # type: ignore[return-value]


SEARCH_CONFIG = {
    "keywords": "",
    "client_type": "",
    "location": "",
    "max_scrolls": 15,
    "results_limit": 100,
    "search_type": "maps",
    "dork_query": "",
}

PHONE_REGEX = r"(\(\d{3}\)\s*\d{3}[-\s]?\d{4}|\+\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})"
EMAIL_REGEX = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DEFAULT_VIEWPORT = {"width": 1920, "height": 1080}  # type: ignore[assignment]

rate_limiter = RateLimiter(min_delay=1.0, max_delay=2.5)


async def human_like_scroll(
    page: Any, scroll_pauses: list[float] | None = None
) -> None:
    """Perform human-like scrolling behavior on a page."""
    if scroll_pauses is None:
        scroll_pauses = [0.3, 0.5, 0.8, 0.4, 0.6, 0.7, 0.3, 0.5, 0.9]

    for i, pause in enumerate(scroll_pauses):
        scroll_amount = 300 + (i * 73) % 500
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await asyncio.sleep(pause)
        if i % 3 == 0:
            await page.evaluate("window.scrollBy(0, -100)")
            await asyncio.sleep(0.2)


async def scrape_listing_details(
    page: Any, context: Any, listing_element: Any
) -> dict[str, str] | None:
    """Open listing in new tab and extract details."""
    business_data: dict[str, str] = {
        "Business Name": "",
        "Phone Number": "",
        "Website": "",
        "Address": "",
    }

    try:
        aria_label = await listing_element.get_attribute("aria-label")
        if aria_label:
            business_data["Business Name"] = aria_label.split(" - ")[0].strip()[:100]
    except Exception:
        logger.debug("Failed to get aria-label from listing element")

    if not business_data["Business Name"]:
        return None

    try:
        href = await listing_element.get_attribute("href")
        if not href:
            return None

        new_page = await context.new_page()
        await new_page.goto(href)
        await asyncio.sleep(2.5)

        try:
            body_text = await new_page.evaluate("() => document.body.innerText")

            phone_match = re.search(PHONE_REGEX, body_text)
            if phone_match:
                business_data["Phone Number"] = phone_match.group(1).strip()

            website_match = re.search(
                r"(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}",
                body_text,
            )
            if website_match:
                website = website_match.group(0)
                if not website.startswith("http"):
                    website = "https://" + website
                business_data["Website"] = website

            address_match = re.search(
                r"\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln)[,\s]+[A-Za-z\s]+,?\s*(?:NY|NJ|CT|PA)?\s*\d{5}",
                body_text,
            )
            if address_match:
                business_data["Address"] = address_match.group(0).strip()[:200]

        except Exception:
            logger.debug("Failed to extract details from listing page")

        await new_page.close()

    except Exception:
        logger.debug("Failed to navigate to listing")

    return business_data


async def _initialize_browser(headless: bool = True) -> tuple[Any, Any, Any]:
    """Initialize Playwright browser with stealth settings."""
    p = await async_playwright().start()
    browser = await p.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    context = await browser.new_context(
        viewport=DEFAULT_VIEWPORT,  # type: ignore[arg-type]
        user_agent=DEFAULT_USER_AGENT,
        locale="en-US",
    )
    page = await context.new_page()

    stealth = Stealth()
    await stealth.apply_stealth_async(page)

    return p, browser, page


async def scrape_google_maps(
    keywords: str,
    location: str,
    max_scrolls: int = 15,
) -> list[dict[str, str]]:
    """Scrape business listings from Google Maps."""
    results: list[dict[str, str]] = []

    p, browser, page = await _initialize_browser(headless=True)
    logger.info("Browser initialized with stealth mode")

    try:
        search_query = f"{keywords} in {location}"
        logger.info(f"Navigating to Google Maps: {search_query}")

        try:
            await page.goto(
                f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            logger.info("Page loaded successfully")
        except Exception as e:
            logger.warning(f"Initial load issue: {e}")
            await page.wait_for_timeout(5000)

        await asyncio.sleep(3)
        logger.info(f"Starting scroll loop (max: {max_scrolls} scrolls)")

        scroll_count = 0
        seen_businesses: set[str] = set()

        while scroll_count < max_scrolls:
            logger.info(
                f"[SCROLL {scroll_count + 1}/{max_scrolls}] Finding listings..."
            )

            await human_like_scroll(page)
            await asyncio.sleep(2)

            try:
                listings = await page.query_selector_all('a[href*="/maps/place"]')
                logger.info(f"Found {len(listings)} listings on this scroll")

                for listing in listings:
                    try:
                        business_data = await scrape_listing_details(
                            page, browser, listing
                        )

                        if business_data and business_data.get("Business Name"):
                            business_key = (
                                business_data["Business Name"].strip().lower()
                            )
                            if business_key not in seen_businesses:
                                seen_businesses.add(business_key)
                                results.append(business_data)
                                logger.info(
                                    f"  + {business_data['Business Name'][:40]:<40} | "
                                    f"Phone: {business_data.get('Phone Number', 'N/A')[:15]}"
                                )
                    except Exception:
                        logger.debug("Failed to process listing")

            except Exception:
                logger.debug("Failed to find listings")

            scroll_count += 1
            logger.info(f"  Total collected so far: {len(results)}")

            if scroll_count < max_scrolls:
                try:
                    show_more = await page.query_selector('button[aria-label="More"]')
                    if not show_more:
                        show_more = await page.query_selector(
                            'button:mmhas-text("More")'
                        )
                    if show_more:
                        await show_more.click()
                        await asyncio.sleep(3)
                        logger.info("Loaded more results")
                except Exception:
                    logger.debug("Failed to click show more button")

            if len(results) >= SEARCH_CONFIG["results_limit"]:
                logger.info(f"Reached results limit: {SEARCH_CONFIG['results_limit']}")
                break

        logger.info(f"Scraping complete! Total results: {len(results)}")

    finally:
        await browser.close()
        await p.stop()

    return results


async def scrape_google_dork(
    keywords: str,
    dork_query: str,
    max_scrolls: int = 15,
) -> list[dict[str, str]]:
    """Scrape using Google Dorking - searches for emails/contacts via Yahoo."""
    results: list[dict[str, str]] = []
    seen_emails: set[str] = set()

    p, browser, page = await _initialize_browser(headless=False)
    logger.info("Browser initialized with stealth mode")

    try:
        search_query = f"{keywords} {dork_query}".strip()
        logger.info(f"Searching Yahoo: {search_query}")

        try:
            await page.goto(
                f"https://search.yahoo.com/search?p={search_query.replace(' ', '+')}",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            logger.info("Search results loaded")
        except Exception as e:
            logger.warning(f"Load issue: {e}")
            await page.wait_for_timeout(10000)

        await asyncio.sleep(3)
        logger.info(f"Scraping results (max: {max_scrolls} scrolls)")

        scroll_count = 0
        page_num = 1

        while scroll_count < max_scrolls:
            logger.info(f"[PAGE {page_num}/{max_scrolls}] Processing results...")

            await human_like_scroll(page)
            await asyncio.sleep(2)

            try:
                result_blocks = await page.query_selector_all("div.algo, li.algo")
                new_count = 0

                for block in result_blocks:
                    try:
                        text = await block.inner_text()

                        emails = re.findall(EMAIL_REGEX, text)
                        for email in emails:
                            email_lower = email.lower()
                            if (
                                email_lower not in seen_emails
                                and not email_lower.endswith("@example.com")
                            ):
                                seen_emails.add(email_lower)

                                result: dict[str, str] = {
                                    "Business Name": "",
                                    "Phone Number": "",
                                    "Website": "",
                                    "Email": email,
                                    "Source": "Yahoo Search",
                                }

                                try:
                                    link = await block.query_selector("a")
                                    if link:
                                        href = await link.get_attribute("href")
                                        if (
                                            href
                                            and href.startswith("http")
                                            and "yahoo" not in href
                                        ):
                                            result["Website"] = href
                                except Exception:
                                    logger.debug(
                                        "Failed to extract website from result"
                                    )

                                try:
                                    title = await block.query_selector("h3")
                                    if title:
                                        result["Business Name"] = (
                                            await title.inner_text()
                                        ).strip()[:100]
                                except Exception:
                                    logger.debug("Failed to extract title from result")

                                phone_match = re.search(PHONE_REGEX, text)
                                if phone_match:
                                    result["Phone Number"] = phone_match.group(
                                        1
                                    ).strip()

                                results.append(result)
                                new_count += 1
                                logger.info(f"  + {email[:40]}")

                    except Exception:
                        logger.debug("Failed to process result block")

                logger.info(f" | New: {new_count} | Total: {len(results)}")

            except Exception:
                logger.debug("Failed to find result blocks")

            next_clicked = False
            if scroll_count < max_scrolls - 1:
                try:
                    next_button = await page.query_selector('a[href*="b="]')
                    if next_button:
                        await next_button.click()
                        await asyncio.sleep(4)
                        page_num += 1
                        next_clicked = True
                        logger.info(f"Moved to page {page_num}")
                except Exception:
                    logger.debug("Failed to click next button (b=)")

                if not next_clicked:
                    try:
                        next_link = await page.query_selector('a[aria-label="Next"]')
                        if next_link:
                            await next_link.click()
                            await asyncio.sleep(4)
                            page_num += 1
                            next_clicked = True
                            logger.info(f"Moved to page {page_num}")
                    except Exception:
                        logger.debug("Failed to click next button (aria-label)")

            scroll_count += 1

            if len(results) >= SEARCH_CONFIG["results_limit"]:
                logger.info(f"Reached results limit: {SEARCH_CONFIG['results_limit']}")
                break

            if not next_clicked and scroll_count >= max_scrolls:
                logger.info("No more pages available")
                break

        logger.info(f"Scraping complete! Total results: {len(results)}")

    finally:
        await browser.close()
        await p.stop()

    return results


def process_and_clean_data(raw_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Process and clean scraped data."""
    if not raw_data:
        return pd.DataFrame(
            columns=["Business Name", "Phone Number", "Website", "Address", "Email"]
        )

    df = pd.DataFrame(raw_data)

    if "Email" in df.columns:
        df = df.drop_duplicates(subset=["Email"], keep="first")
        df["has_contact"] = (
            (df["Phone Number"].fillna("").str.len() > 0)
            | (df["Website"].fillna("").str.len() > 0)
            | (df["Email"].fillna("").str.len() > 0)
        )
    else:
        df = df.drop_duplicates(subset=["Business Name"], keep="first")
        df["has_contact"] = (df["Phone Number"].fillna("").str.len() > 0) | (
            df["Website"].fillna("").str.len() > 0
        )

    df = df[df["has_contact"]].drop(columns=["has_contact"])

    def validate_phone(phone: Any) -> str:
        if pd.isna(phone) or phone == "":
            return str(phone)
        cleaned = re.sub(r"[^\d\+]", "", str(phone))
        if 10 <= len(cleaned) <= 15:
            return str(phone)
        return ""

    df["Phone Number"] = df["Phone Number"].apply(validate_phone)  # type: ignore[union-attr]
    business_names = df["Business Name"].fillna("")  # type: ignore[union-attr]
    df = df[business_names.str.len() > 0]

    return df  # type: ignore[return-value]


def parse_search_prompt(search_prompt: str) -> dict[str, str]:
    """Parse user search prompt to determine search type and parameters."""
    config = {
        "search_type": "maps",
        "keywords": "",
        "location": "",
    }

    dork_indicators = [
        "@gmail.com",
        "@yahoo.com",
        "@hotmail.com",
        "@outlook.com",
        "contact@",
        "site:",
    ]

    if any(indicator in search_prompt.lower() for indicator in dork_indicators):
        config["search_type"] = "dork"
        config["keywords"] = search_prompt
    else:
        config["search_type"] = "maps"
        if " in " in search_prompt.lower():
            parts = search_prompt.split(" in ", 1)
            config["keywords"] = parts[0].strip()
            config["location"] = parts[1].strip()
        elif " near " in search_prompt.lower():
            parts = search_prompt.split(" near ", 1)
            config["keywords"] = parts[0].strip()
            config["location"] = parts[1].strip()
        else:
            config["keywords"] = search_prompt
            config["location"] = os.getenv("DEFAULT_LOCATION", "New York")

    return config


async def main() -> None:
    """Main entry point for the lead scraper."""
    print("=" * 60)
    print("              LEAD SCRAPING AGENT")
    print("=" * 60)

    print("\n[ENTER SEARCH PROMPT]")
    print("Examples:")
    print("  - 'restaurants in New York'     (Google Maps)")
    print("  - 'real estate @gmail.com'       (Google Dorking)")
    print("  - 'plumbers @yahoo.com Los Angeles'")
    print("  - 'hotels contact@*.com'")
    print("  - 'restaurants near Times Square'")

    search_prompt = input("\nEnter search prompt: ").strip()

    if not search_prompt:
        print("[ERROR] No search prompt entered.")
        return

    parsed = parse_search_prompt(search_prompt)
    SEARCH_CONFIG["search_type"] = parsed["search_type"]
    SEARCH_CONFIG["keywords"] = parsed["keywords"]
    SEARCH_CONFIG["location"] = parsed.get("location", "")

    print(
        f"\n[DETECTED] {'Google Dorking' if SEARCH_CONFIG['search_type'] == 'dork' else 'Google Maps'} mode"
    )

    try:
        if SEARCH_CONFIG["search_type"] == "dork":
            SEARCH_CONFIG["max_scrolls"] = int(
                input("Page limit (number of pages to scrape): ").strip() or "5"
            )
        else:
            SEARCH_CONFIG["max_scrolls"] = int(
                input("Max scrolls (default 15): ").strip() or "15"
            )
        SEARCH_CONFIG["results_limit"] = int(
            input("Results limit (default 100): ").strip() or "100"
        )
    except ValueError:
        logger.warning("Invalid numeric input, using defaults")

    print("\n" + "=" * 60)
    print(
        f"  Search Type:   {'Google Dorking' if SEARCH_CONFIG['search_type'] == 'dork' else 'Google Maps'}"
    )
    print(f"  Search Prompt: {search_prompt}")
    if SEARCH_CONFIG["search_type"] == "maps":
        print(f"  Keywords:      {SEARCH_CONFIG['keywords']}")
        print(f"  Location:      {SEARCH_CONFIG['location']}")

    print(f"  Max Scrolls:  {SEARCH_CONFIG['max_scrolls']}")
    print(f"  Results Limit: {SEARCH_CONFIG['results_limit']}")
    print("=" * 60 + "\n")

    logger.info("Initializing browser...")

    raw_results: list[dict[str, str]] = []
    try:
        if SEARCH_CONFIG["search_type"] == "dork":
            raw_results = await scrape_google_dork(
                keywords=SEARCH_CONFIG["keywords"],
                dork_query="",
                max_scrolls=SEARCH_CONFIG["max_scrolls"],
            )
        else:
            raw_results = await scrape_google_maps(
                keywords=SEARCH_CONFIG["keywords"],
                location=SEARCH_CONFIG["location"],
                max_scrolls=SEARCH_CONFIG["max_scrolls"],
            )
    except KeyboardInterrupt:
        print("\n[!] Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Error during scraping: {e}")

    logger.info(f"Scraping finished! Total raw results: {len(raw_results)}")

    if not raw_results:
        logger.warning("No results found. Check your search parameters.")
        return

    df = process_and_clean_data(raw_results)
    logger.info(f"After processing: {len(df)} valid leads")

    output_file = "leads_output.xlsx"
    df.to_excel(output_file, index=False, engine="openpyxl")

    logger.info(f"Results exported to {output_file}")
    print("=" * 60)
    print("\nSample results:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    asyncio.run(main())
