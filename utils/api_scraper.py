import requests
import os
import json
from crawl4ai import LLMExtractionStrategy
from mcnews import NewsdataNews

def fetch_newsdata_api(api_key: str, country: str = 'in', language: str = 'en'):
    """Fetch news from newsdata.io API"""
    url = 'https://newsdata.io/api/1/news'
    params = {
        'apikey': api_key,
        'country': country,
        'language': language,
        'category': 'top'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == 'success':
            results = data.get('results', [])
            return results
        else:
            print(f"API Error: {data.get('message', 'Unknown error')}")
            return []
            
    except Exception as e:
        print(f"Error fetching from newsdata.io: {e}")
        return []


def process_newsdata_with_gemini(raw_results: list, gemini_key: str):
    """Process newsdata.io results with Gemini for standardization"""
    
    if not raw_results:
        return []
    
    # Create LLM strategy for processing
    llm_strategy = LLMExtractionStrategy(
        provider="gemini/gemini-2.5-flash",
        api_token=gemini_key,
        schema=NewsdataNews.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Convert the news data to the standard format. "
            "Extract: title, description, url (from 'link'), "
            "publishtime (from 'pubDate'), provider (from 'source_id'). "
            "Return a valid JSON array."
        ),
        input_format="json",
        verbose=True,
    )
    
    formatted_news = []
    for item in raw_results:
        # Basic transformation without Gemini if you prefer
        news = {
            'title': item.get('title', ''),
            'description': item.get('description', ''),
            'url': item.get('link', ''),
            'publishtime': item.get('pubDate', ''),
            'provider': item.get('source_id', 'newsdata')
        }
        formatted_news.append(news)
    
    return formatted_news