import asyncio
import re
import random
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

SEARCH_CONFIG = {
    "keywords": "",
    "client_type": "",
    "location": "",
    "max_scrolls": 15,
    "results_limit": 100,
    "search_type": "maps",  # maps or dork
    "dork_query": "",  # For Google dorking
}

PHONE_REGEX = r"(\(\d{3}\)\s*\d{3}[-\s]?\d{4}|\+\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})"
EMAIL_REGEX = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"


async def human_like_scroll(page):
    scroll_pauses = [0.3, 0.5, 0.8, 0.4, 0.6, 0.7, 0.3, 0.5, 0.9]
    for i, pause in enumerate(scroll_pauses):
        scroll_amount = random.randint(300, 800)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await asyncio.sleep(pause)
        if i % 3 == 0:
            await page.evaluate("window.scrollBy(0, -100)")
            await asyncio.sleep(0.2)


async def scrape_listing_details(page, context, listing_element):
    """Open listing in new tab and extract details"""
    business_data = {
        "Business Name": "",
        "Phone Number": "",
        "Website": "",
        "Address": "",
    }

    try:
        aria_label = await listing_element.get_attribute("aria-label")
        if aria_label:
            business_data["Business Name"] = aria_label.split(" - ")[0].strip()[:100]
    except:
        pass

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
            body_text = await new_page.evaluate("""() => document.body.innerText""")

            phone_match = re.search(PHONE_REGEX, body_text)
            if phone_match:
                business_data["Phone Number"] = phone_match.group(1).strip()

            website_match = re.search(
                r"(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}", body_text
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

        except Exception as e:
            pass

        await new_page.close()

    except Exception as e:
        pass

    return business_data


async def scrape_google_maps(
    keywords: str, location: str, max_scrolls: int = 15
) -> list:
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = await context.new_page()

        stealth = Stealth()
        await stealth.apply_stealth_async(page)

        print("[OK] Browser initialized with stealth mode")

        search_query = f"{keywords} in {location}"
        print(f"[LOAD] Navigating to Google Maps: {search_query}...")

        try:
            await page.goto(
                f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            print("[OK] Page loaded successfully")
        except Exception as e:
            print(f"[WARN] Initial load issue: {e}")
            await page.wait_for_timeout(5000)

        await asyncio.sleep(3)

        print(f"[START] Starting scroll loop (max: {max_scrolls} scrolls)...\n")

        scroll_count = 0
        seen_businesses = set()

        while scroll_count < max_scrolls:
            print(
                f"[SCROLL {scroll_count + 1}/{max_scrolls}] Finding listings...",
                end=" ",
            )

            await human_like_scroll(page)
            await asyncio.sleep(2)

            try:
                listings = await page.query_selector_all('a[href*="/maps/place"]')
                print(f"Found {len(listings)} listings on this scroll")

                for i, listing in enumerate(listings):
                    try:
                        business_data = await scrape_listing_details(
                            page, context, listing
                        )

                        if business_data and business_data.get("Business Name"):
                            business_key = (
                                business_data["Business Name"].strip().lower()
                            )
                            if business_key not in seen_businesses:
                                seen_businesses.add(business_key)
                                results.append(business_data)
                                print(
                                    f"  [+] {business_data['Business Name'][:40]:<40} | Phone: {business_data.get('Phone Number', 'N/A')[:15]}"
                                )
                    except Exception as e:
                        continue

            except Exception as e:
                pass

            scroll_count += 1
            print(f"  Total collected so far: {len(results)}\n")

            # Try to load more results on Google Maps
            if scroll_count < max_scrolls:
                try:
                    # Look for "Show more" button on Google Maps
                    show_more = await page.query_selector('button[aria-label="More"]')
                    if not show_more:
                        show_more = await page.query_selector(
                            'button:mmhas-text("More")'
                        )
                    if show_more:
                        await show_more.click()
                        await asyncio.sleep(3)
                        print(f"  [>>] Loaded more results")
                except:
                    pass

            if len(results) >= SEARCH_CONFIG["results_limit"]:
                print(
                    f"\n[INFO] Reached results limit: {SEARCH_CONFIG['results_limit']}"
                )
                break

        print(f"\n[OK] Scraping complete! Total results: {len(results)}")

        await browser.close()

    return results


async def scrape_google_dork(
    keywords: str, dork_query: str, max_scrolls: int = 15
) -> list:
    """Scrape using Google Dorking - searches for emails/contacts"""
    results = []
    seen_emails = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # Non-headless for Google
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = await context.new_page()

        stealth = Stealth()
        await stealth.apply_stealth_async(page)

        print("[OK] Browser initialized with stealth mode")

        # Use Yahoo for dorking (Google/Bing block automated requests)
        search_query = f"{keywords} {dork_query}".strip()
        print(f"[LOAD] Searching Yahoo: {search_query}...")

        try:
            await page.goto(
                f"https://search.yahoo.com/search?p={search_query.replace(' ', '+')}",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            print("[OK] Search results loaded")
        except Exception as e:
            print(f"[WARN] Load issue: {e}")
            await page.wait_for_timeout(10000)

        await asyncio.sleep(3)

        print(f"[START] Scraping results (max: {max_scrolls} scrolls)...\n")

        scroll_count = 0
        new_count = 0

        while scroll_count < max_scrolls:
            print(
                f"[SCROLL {scroll_count + 1}/{max_scrolls}] Processing results...",
                end=" ",
            )

            await human_like_scroll(page)
            await asyncio.sleep(2)

            try:
                # Bing result selectors
                # Yahoo result selectors
                result_blocks = await page.query_selector_all("div.algo, li.algo")
                new_count = 0

                for block in result_blocks:
                    try:
                        # Extract text content
                        text = await block.inner_text()

                        # Find emails
                        emails = re.findall(EMAIL_REGEX, text)
                        for email in emails:
                            email_lower = email.lower()
                            if (
                                email_lower not in seen_emails
                                and not email_lower.endswith("@example.com")
                            ):
                                seen_emails.add(email_lower)

                                result = {
                                    "Business Name": "",
                                    "Phone Number": "",
                                    "Website": "",
                                    "Email": email,
                                    "Source": "Google Search",
                                }

                                # Try to extract website from result
                                try:
                                    link = await block.query_selector("a")
                                    if link:
                                        href = await link.get_attribute("href")
                                        if (
                                            href
                                            and href.startswith("http")
                                            and "google" not in href
                                        ):
                                            result["Website"] = href
                                except:
                                    pass

                                # Try to extract business name from title
                                try:
                                    title = await block.query_selector("h3")
                                    if title:
                                        result["Business Name"] = (
                                            await title.inner_text()
                                        ).strip()[:100]
                                except:
                                    pass

                                # Find phones in text
                                phone_match = re.search(PHONE_REGEX, text)
                                if phone_match:
                                    result["Phone Number"] = phone_match.group(
                                        1
                                    ).strip()

                                results.append(result)
                                new_count += 1
                                print(f"  [+] {email[:40]}")

                    except Exception as e:
                        continue

            except Exception as e:
                pass

            print(f" | New: {new_count} | Total: {len(results)}")

            # Try to go to next page if no new results or after each scroll
            if new_count == 0 or scroll_count < max_scrolls - 1:
                try:
                    next_button = await page.query_selector(
                        'a[href*="page="], a.next, a.pagination-next'
                    )
                    if next_button:
                        await next_button.click()
                        await asyncio.sleep(3)
                        print(f"  [>>] Moved to next page")
                        continue

                    # Try Yahoo specific next button
                    next_link = await page.query_selector('a[aria-label="Next"]')
                    if next_link:
                        await next_link.click()
                        await asyncio.sleep(3)
                        print(f"  [>>] Moved to next page")
                        continue
                except:
                    pass

            scroll_count += 1

            if len(results) >= SEARCH_CONFIG["results_limit"]:
                print(
                    f"\n[INFO] Reached results limit: {SEARCH_CONFIG['results_limit']}"
                )
                break

        print(f"\n[OK] Scraping complete! Total results: {len(results)}")

        await browser.close()

    return results


def process_and_clean_data(raw_data: list) -> pd.DataFrame:
    if not raw_data:
        return pd.DataFrame(
            columns=["Business Name", "Phone Number", "Website", "Address", "Email"]
        )

    df = pd.DataFrame(raw_data)

    # Handle email-specific processing
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

    def validate_phone(phone):
        if pd.isna(phone) or phone == "":
            return phone
        cleaned = re.sub(r"[^\d\+]", "", str(phone))
        if len(cleaned) >= 10 and len(cleaned) <= 15:
            return phone
        return ""

    df["Phone Number"] = df["Phone Number"].apply(validate_phone)
    df = df[df["Business Name"].fillna("").str.len() > 0]

    return df


async def main():
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

    # Determine search type based on prompt
    dork_indicators = [
        "@gmail.com",
        "@yahoo.com",
        "@hotmail.com",
        "@outlook.com",
        "contact@",
        "site:",
    ]
    is_dork = any(indicator in search_prompt.lower() for indicator in dork_indicators)

    if is_dork:
        SEARCH_CONFIG["search_type"] = "dork"
        SEARCH_CONFIG["keywords"] = search_prompt
    else:
        SEARCH_CONFIG["search_type"] = "maps"
        # Parse location from prompt (everything after "in" or "near")
        if " in " in search_prompt.lower():
            parts = search_prompt.split(" in ", 1)
            SEARCH_CONFIG["keywords"] = parts[0].strip()
            SEARCH_CONFIG["location"] = parts[1].strip()
        elif " near " in search_prompt.lower():
            parts = search_prompt.split(" near ", 1)
            SEARCH_CONFIG["keywords"] = parts[0].strip()
            SEARCH_CONFIG["location"] = parts[1].strip()
        else:
            SEARCH_CONFIG["keywords"] = search_prompt
            SEARCH_CONFIG["location"] = "New York"  # default

    print(
        f"\n[DETECTED] {'Google Dorking' if SEARCH_CONFIG['search_type'] == 'dork' else 'Google Maps'} mode"
    )

    try:
        SEARCH_CONFIG["max_scrolls"] = int(
            input("Max scrolls (default 15): ").strip() or "15"
        )
        SEARCH_CONFIG["results_limit"] = int(
            input("Results limit (default 100): ").strip() or "100"
        )
    except ValueError:
        pass

    print("\n" + "=" * 60)
    print(
        f"  Search Type:   {'Google Dorking' if SEARCH_CONFIG['search_type'] == 'dork' else 'Google Maps'}"
    )
    print(f"  Search Prompt: {search_prompt}")
    if SEARCH_CONFIG["search_type"] == "maps":
        print(f"  Keywords:      {SEARCH_CONFIG['keywords']}")
        print(f"  Location:     {SEARCH_CONFIG['location']}")
    print(f"  Max Scrolls:  {SEARCH_CONFIG['max_scrolls']}")
    print(f"  Results Limit: {SEARCH_CONFIG['results_limit']}")
    print("=" * 60 + "\n")

    print("[START] Initializing browser...\n")

    raw_results = []
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
        print(f"\n[!] Error during scraping: {e}")

    print(f"\n[COMPLETE] Scraping finished!")
    print(f"[INFO] Total raw results: {len(raw_results)}")

    if not raw_results:
        print("[WARNING] No results found. Check your search parameters.")
        return

    df = process_and_clean_data(raw_results)
    print(f"[INFO] After processing: {len(df)} valid leads")

    output_file = "leads_output.xlsx"
    df.to_excel(output_file, index=False, engine="openpyxl")

    print(f"[SUCCESS] Results exported to {output_file}")
    print("=" * 60)
    print("\nSample results:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    asyncio.run(main())
