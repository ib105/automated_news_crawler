# config.py

# BASE_URL = "https://www.moneycontrol.com/news/india"
# CSS_SELECTOR = "[class='fleft']"
SOURCES = {
    'moneycontrol': {
        'base_url': 'https://www.moneycontrol.com/city',
        'css_selector': "[class='topictabpane']",
        'type': 'scraper',
        'model': 'mcnews',
        'gemini_key_env': 'GEMINI_API_KEY_1'
    },
    'thehindu': {
        'base_url': 'https://www.thehindu.com/news/national',
        'css_selector': '[class="story-card"]',
        'type': 'scraper',
        'model': 'thehindu',
        'gemini_key_env': 'GEMINI_API_KEY_2'
    },
    'indianexpress': {
        'base_url': 'https://indianexpress.com/section/india',
        'css_selector': '[class="articles"]',
        'type': 'scraper',
        'model': 'indianexpress',
        'gemini_key_env': 'GEMINI_API_KEY_3'
    },
    'newsdata': {
        'api_url': 'https://newsdata.io/api/1/news',
        'type': 'api',
        'model': 'newsdata',
        'gemini_key_env': 'GEMINI_API_KEY_3'  # Shares key with indianexpress
    }
}

REQUIRED_KEYS = ['title', 'description', 'url', 'publishtime', 'provider']
