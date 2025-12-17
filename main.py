import asyncio
import os
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler

from config import SOURCES, REQUIRED_KEYS
from utils.kafka_producer import send_news_to_kafka
from utils.data_utils import save_news_to_csv
from utils.api_scraper import fetch_newsdata_api, process_newsdata_with_gemini
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy_for_source,
)

load_dotenv()


async def crawl_scraper_source(source_name: str, source_config: dict):
    """Crawl a web scraper source"""
    # Get API key from environment
    api_key = os.getenv(source_config['gemini_key_env'])
    if not api_key:
        print(f"[{source_name}] No Gemini API key found for {source_config['gemini_key_env']}")
        return []
    
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy_for_source(source_config['model'], api_key)
    session_id = f"{source_name}_session"
    
    page_number = 1
    all_news = []
    seen_names = set()
    kafka_topic = os.getenv('KAFKA_TOPIC', 'news-events')

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            news = []
            for attempt in range(3):
                news, no_results_found = await fetch_and_process_page(
                    crawler, page_number, source_config['base_url'], 
                    source_config['css_selector'], llm_strategy, session_id, 
                    REQUIRED_KEYS, seen_names
                )
                
                if news or no_results_found:
                    break
                
                print(f"[{source_name}] Retry {attempt + 1}/3 for page {page_number}")
                await asyncio.sleep(5)

            if no_results_found:
                break

            if not news:
                print(f"[{source_name}] No news from page {page_number} after retries.")
                break

            send_news_to_kafka(news, kafka_topic)
            print(f"[{source_name}] Sent {len(news)} news from page {page_number} to Kafka")
            
            all_news.extend(news)

            if page_number > 20:
                break
            
            page_number += 1
            await asyncio.sleep(2)

    if all_news:
        save_news_to_csv(all_news, f"{source_name}_news.csv")
        print(f"[{source_name}] Saved {len(all_news)} news to CSV")
    
    llm_strategy.show_usage()
    return all_news


async def crawl_api_source(source_name: str, source_config: dict):
    """Fetch news from API source - runs in executor to not block async"""
    api_key = os.getenv('NEWSDATA_API_KEY')
    gemini_key = os.getenv(source_config['gemini_key_env'])
    
    if not api_key:
        print(f"[{source_name}] No API key found")
        return []
    
    if not gemini_key:
        print(f"[{source_name}] No Gemini API key found for {source_config['gemini_key_env']}")
        return []
    
    # Run blocking API call in executor
    loop = asyncio.get_event_loop()
    raw_results = await loop.run_in_executor(None, fetch_newsdata_api, api_key)
    
    # Process with Gemini (also blocking, run in executor)
    news = await loop.run_in_executor(
        None, 
        process_newsdata_with_gemini, 
        raw_results, 
        gemini_key
    )
    
    kafka_topic = os.getenv('KAFKA_TOPIC', 'news-events')
    
    if news:
        send_news_to_kafka(news, kafka_topic)
        save_news_to_csv(news, f"{source_name}_news.csv")
        print(f"[{source_name}] Sent {len(news)} news to Kafka")
    
    return news


async def main():
    """Entry point - crawl all sources concurrently"""
    print(f"\n{'='*50}")
    print(f"Starting concurrent crawl of all sources")
    print(f"{'='*50}\n")
    
    # Create tasks for all sources
    tasks = []
    for source_name, source_config in SOURCES.items():
        if source_config['type'] == 'scraper':
            task = crawl_scraper_source(source_name, source_config)
        elif source_config['type'] == 'api':
            task = crawl_api_source(source_name, source_config)
        else:
            print(f"Unknown source type: {source_config['type']}")
            continue
        
        tasks.append((source_name, task))
    
    # Run all tasks concurrently
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    total_news = 0
    for (source_name, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            print(f"[{source_name}] Error: {result}")
        else:
            total_news += len(result)
            print(f"[{source_name}] Completed: {len(result)} news")
    
    print(f"\n{'='*50}")
    print(f"Total news collected: {total_news}")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())