import json
import os
from typing import List, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.mcnews import News, HinduNews, ExpressNews, NewsdataNews

from utils.data_utils import is_complete_news, is_duplicate_news


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=True,
    )


def get_llm_strategynew() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    return LLMExtractionStrategy(
        provider="gemini/gemini-2.5-flash",
        api_token=os.getenv("GEMINI_API_KEY"),
        schema=News.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract news articles from the HTML content. "
            "For each article, extract the following fields: "
            "title (article headline), description (brief summary), "
            "url (article link), publishtime (publication date/time), "
            "provider (source/author). "
            "Return a valid JSON array of objects with these exact field names in lowercase."
        ),
        input_format="html",
        verbose=True,
    )

def get_llm_strategy_for_source(model_name: str, api_key: str) -> LLMExtractionStrategy:
    """Get LLM strategy based on source with specific API key"""
    
    model_map = {
        'mcnews': News,
        'thehindu': HinduNews,
        'indianexpress': ExpressNews,
        'newsdata': NewsdataNews
    }
    
    schema_class = model_map.get(model_name, News)
    
    return LLMExtractionStrategy(
        provider="gemini/gemini-2.5-flash",
        api_token=api_key,
        schema=schema_class.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract news articles from the HTML content. "
            "For each article, extract the following fields: "
            "title (article headline), description (brief summary), "
            "url (article link), publishtime (publication date/time), "
            "provider (source/author). "
            "Return a valid JSON array of objects with these exact field names in lowercase."
        ),
        input_format="html",
        verbose=True,
    )

async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if the "No Results Found" message is present on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if "No Results Found" message is found, False otherwise.
    """
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )

    if result.success:
        if "No Results Found" in result.cleaned_html:
            return True
    else:
        print(
            f"Error fetching page for 'No Results Found' check: {result.error_message}"
        )

    return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of venue data.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the venue data.
        seen_names (Set[str]): Set of venue names that have already been seen.

    Returns:
        Tuple[List[dict], bool]:
            - List[dict]: A list of processed news from the page.
            - bool: A flag indicating if the "No Results Found" message was encountered.
    """
    url = f"{base_url}/page-{page_number}/"
    print(f"Loading page {page_number}...")

    # Check if "No Results Found" message is present
    no_results = await check_no_results(crawler, url, session_id)
    if no_results:
        return [], True  # No more results, signal to stop crawling

    # Fetch page content with the extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=llm_strategy,
            css_selector=css_selector,
            session_id=session_id,
        ),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    # Parse extracted content with better error handling
    try:
        extracted_data = json.loads(result.extracted_content)
        
        # Handle if extraction returned an error dict instead of list
        if isinstance(extracted_data, dict):
            if extracted_data.get("error"):
                print(f"LLM extraction error on page {page_number}: {extracted_data.get('content', 'Unknown error')}")
                return [], False
            # If it's a single dict, wrap it in a list
            extracted_data = [extracted_data]
        
        # Handle if it's not a list after conversion
        if not isinstance(extracted_data, list):
            print(f"Unexpected data format on page {page_number}: {type(extracted_data)}")
            print(f"Data: {extracted_data}")
            return [], False
            
        if not extracted_data:
            print(f"No news found on page {page_number}.")
            return [], False
            
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON on page {page_number}: {e}")
        print(f"Raw content: {result.extracted_content[:500]}...")  # Print first 500 chars
        return [], False
    except Exception as e:
        print(f"Unexpected error processing page {page_number}: {e}")
        return [], False

    # Debug output
    print(f"Successfully parsed {len(extracted_data)} items from page {page_number}")
    if extracted_data and len(extracted_data) > 0:
        print(f"Sample item keys: {list(extracted_data[0].keys())}")

    # Process news
    complete_news = []
    for idx, news in enumerate(extracted_data):
        # Skip if not a dict or has error flag
        if not isinstance(news, dict):
            print(f"Item {idx} is not a dict: {type(news)}")
            continue
            
        if news.get("error"):
            print(f"Item {idx} has error flag: {news.get('content', 'No error message')}")
            continue
        
        # Remove error key if present
        news.pop("error", None)
        
        # Check for required keys
        if not is_complete_news(news, required_keys):
            missing_keys = [key for key in required_keys if key not in news]
            print(f"Item {idx} missing required keys: {missing_keys}")
            print(f"Available keys: {list(news.keys())}")
            continue
        
        # Check for duplicates
        try:
            news_title = news.get("title", "")
            if not news_title:
                print(f"Item {idx} has empty title")
                continue
                
            if is_duplicate_news(news_title, seen_names):
                print(f"Duplicate found: {news_title[:50]}...")
                continue

            seen_names.add(news_title)
            complete_news.append(news)
            
        except KeyError as e:
            print(f"KeyError processing item {idx}: {e}")
            print(f"Item data: {news}")
            continue

    if not complete_news:
        print(f"No complete news found on page {page_number} after filtering.")
        return [], False

    print(f"Extracted {len(complete_news)} valid news items from page {page_number}.")
    return complete_news, False  # Continue crawling