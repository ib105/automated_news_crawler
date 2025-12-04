# Automated News Web Scraper

A production-ready web scraping application that automatically extracts news articles from websites and stores them in Azure Blob Storage for analysis in Microsoft Fabric.

## Features

- **Asynchronous Web Scraping**: Uses crawl4ai for efficient, paginated content extraction
- **AI-Powered Extraction**: Leverages Google Gemini AI to intelligently parse and structure news data
- **Cloud Storage Integration**: Automatically saves results to Azure Blob Storage
- **Duplicate Detection**: Prevents re-scraping of already collected articles
- **Dockerized Deployment**: Runs in Azure Container Instances for serverless execution
- **Automated Scheduling**: Daily execution via Azure Logic Apps
- **Fabric Integration**: Seamlessly imports data into Microsoft Fabric Lakehouse

## Prerequisites

- Python 3.12+
- Docker Desktop
- Azure Subscription
- Google Gemini API Key
- Microsoft Fabric workspace (for data analysis)

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Azure Logic    │────▶│  Azure Container │────▶│  Azure Blob     │
│  App (9:00 AM)  │     │  Instance        │     │  Storage        │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                              │                           │
                              │ Scrapes News              │
                              ▼                           ▼
                        ┌──────────────┐          ┌─────────────────┐
                        │  Target      │          │  Fabric Pipeline│
                        │  Website     │          │  (9:30 AM)      │
                        └──────────────┘          └─────────────────┘
                                                          │
                                                          ▼
                                                   ┌─────────────────┐
                                                   │  Fabric         │
                                                   │  Lakehouse      │
                                                   └─────────────────┘
```

## Project Structure

```
paginated_ai_web_crawler/
├── main.py                 # Main application entry point
├── config.py              # Configuration settings (URLs, selectors)
├── pyproject.toml         # Python dependencies
├── Dockerfile             # Container configuration
├── .dockerignore          # Docker build exclusions
├── env-vars.yaml          # Environment variables template
├── models/
│   └── mcnews.py         # Pydantic data models
└── utils/
    ├── data_utils.py     # CSV generation and Azure Storage
    └── scraper_utils.py  # Web scraping logic
```

## Installation

### Local Development

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd paginated_ai_web_crawler
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e .
   playwright install chromium
   ```

4. **Configure environment variables**
   
   Create a `.env` file:
   ```env
   GEMINI_API_KEY=your_gemini_api_key
   AZURE_STORAGE_CONNECTION_STRING=your_connection_string
   AZURE_STORAGE_CONTAINER=news-data
   ```

5. **Run locally**
   ```bash
   python main.py
   ```

## Docker Deployment

### Build and Test Locally

```bash
docker build -t news-scraper:latest .

docker run --env-file .env news-scraper:latest
```

### Deploy to Azure

1. **Create Azure Container Registry**
   ```bash
   az acr create \
     --resource-group Fabric \
     --name webnewsscraper \
     --sku Basic \
     --location centralindia
   ```

2. **Build and push image**
   ```bash
   az acr login --name webnewsscraper
   
   docker build -t webnewsscraper.azurecr.io/news-scraper:latest .
   docker push webnewsscraper.azurecr.io/news-scraper:latest
   ```

3. **Create Azure Storage Account**
   ```bash
   az storage account create \
     --name newsdata \
     --resource-group Fabric \
     --location centralindia \
     --sku Standard_LRS
   
   az storage container create \
     --name news-data \
     --account-name newsdata \
     --auth-mode login
   ```

4. **Test container deployment**
   ```bash
   az container create \
     --resource-group Fabric \
     --name news-scraper-test \
     --image webnewsscraper.azurecr.io/news-scraper:latest \
     --registry-login-server webnewsscraper.azurecr.io \
     --registry-username webnewsscraper \
     --registry-password "<your-password>" \
     --cpu 1 \
     --memory 2 \
     --restart-policy Never \
     --location centralindia \
     --os-type Linux \
     --environment-variables @env-vars.yaml
   ```

## Automated Scheduling

### Azure Logic App Setup

1. **Create Logic App** in Azure Portal
2. **Add Recurrence Trigger**:
   - Frequency: Daily
   - Time: 9:00 AM IST
3. **Add Container Instance Action**:
   - Action: "Create or update a container group"
   - Configure with your ACR image and environment variables
4. **Save and enable** the Logic App

The scraper will now run automatically every day at 9:00 AM.

## Microsoft Fabric Integration

### Fabric Pipeline Setup

1. **Create Data Pipeline** in Fabric workspace
2. **Add Copy Data Activity**:
   - **Source**: Azure Blob Storage
     - Connection: newsdata storage account
     - Container: news-data
     - File: latest_news.csv
   - **Destination**: Lakehouse
     - Root folder: Files
     - Path: news/
3. **Schedule Pipeline**: Daily at 9:30 AM IST
4. **Run and verify** data flows into Lakehouse

## Configuration

### Scraping Configuration (`config.py`)

```python
# Target website URL
BASE_URL = "https://www.moneycontrol.com/city"

# CSS selector for content extraction
CSS_SELECTOR = "[class='topictabpane']"

# Required fields for each news article
REQUIRED_KEYS = [
    "Title",
    "description",
    "url",    
    "publishtime",
    "provider"
]
```

### Data Model (`models/mcnews.py`)

```python
class News(BaseModel):
    Title: str
    description: str
    publishtime: str
    url: str
    provider: str
```

## Troubleshooting

### Common Issues

**Issue**: `BrowserType.launch: Executable doesn't exist`
- **Solution**: Install Playwright browsers: `playwright install chromium`

**Issue**: `Bad data detected at line X`
- **Solution**: The CSV cleaning logic handles special characters. Ensure you're using the updated `data_utils.py`

**Issue**: Docker build fails
- **Solution**: Ensure Docker Desktop is running and you have sufficient disk space

**Issue**: Container exits with code 1
- **Solution**: Check logs: `az container logs --resource-group Fabric --name <container-name>`

**Issue**: Azure region not allowed
- **Solution**: Use an allowed region like `centralindia` instead of `eastus`

## Monitoring

### View Container Logs

```bash
az container logs \
  --resource-group Fabric \
  --name news-scraper-test \
  --follow
```

### Check Blob Storage

```bash
az storage blob list \
  --container-name news-data \
  --account-name newsdata \
  --auth-mode login \
  --output table
```




## 🔗 Resources

- [crawl4ai Documentation](https://docs.crawl4ai.com/)
- [Azure Container Instances](https://docs.microsoft.com/azure/container-instances/)
- [Microsoft Fabric](https://learn.microsoft.com/fabric/)
- [Playwright Documentation](https://playwright.dev/python/)

---

**Last Updated**: December 2024