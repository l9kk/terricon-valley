# EOZ Procurement-Risk Dataset Builder

🛰️ **AI-powered corruption detection system for Kazakhstan's public procurement data**

## Overview

This project automates the collection and analysis of procurement data from Kazakhstan's EOZ portal to detect corruption patterns including:

- **Price Inflation**: Abnormal pricing using statistical analysis
- **Single Bidder Awards**: Non-competitive procurement detection  
- **Repeated Winners**: Potential collusion patterns
- **Contract Splitting**: Threshold circumvention detection
- **Underpayment**: Payment irregularity detection

## Quick Start

### 1. Installation

```bash
# Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### 2. Configuration

The `.env` file contains all configuration parameters. Key settings:

- `EOZ_BASE_URL`: API endpoint URL
- `EOZ_COOKIES`: Required session cookies for Plan entity
- `MAX_CONCURRENT_REQUESTS`: Rate limiting (default: 50)
- `ISOLATION_CONTAMINATION`: Anomaly detection threshold (default: 0.05)

### 3. Run Data Collection

```bash
# Complete data scraping and processing pipeline
python -m src scrape
```

This will:
1. Scrape Plan, _Lot, and OrderDetail entities from EOZ API
2. Normalize and save raw data to Parquet format
3. Join datasets using DuckDB
4. Engineer corruption detection features
5. Train Isolation Forest anomaly detection model

### 4. Launch Dashboard

```bash
# Start interactive Streamlit dashboard
python -m src dashboard
```

Access the dashboard at `http://localhost:8501`

## Architecture

### Data Pipeline

```
EOZ API → Raw JSON → Normalized Parquet → Joined Dataset → Feature Engineering → ML Model → Dashboard
```

### Project Structure

```
src/
├── eoz_downloader.py    # Async API scraping with rate limiting
├── normalizer.py        # JSON flattening and column selection
├── joiner.py           # DuckDB-based dataset joining
├── features.py         # Corruption flag calculations
├── risk_model.py       # Isolation Forest training
├── dashboard.py        # Streamlit visualization
└── __main__.py         # CLI interface

raw/                    # Raw JSON data storage
├── pages/             # API list responses
└── objects/           # Full object details

bronze/                # Normalized Parquet files
dataset/              # Final joined datasets
models/              # Trained ML models
logs/               # Application logs
```

## Data Model

The system processes three main entity types from EOZ:

- **Plan**: Procurement plans (`plan_id`, `plan_price`, `plan_customer_bin`)
- **_Lot**: Tender lots (`lot_id`, `lot_amount`, `title_ru`, `lot_method_id`)  
- **OrderDetail**: Signed contracts (`contract_sum`, `provider_bin`, `paid_sum`)

*Note: Tender entity is skipped as it's not accessible via the EOZ API*

## Risk Detection Features

### Statistical Features
- **Price Z-Score**: MAD-based outlier detection for similar procurement items
- **Payment Ratio**: Actual payment vs contracted amount
- **Processing Time**: Days between lot start and contract acceptance

### Corruption Indicators
- **Single Bidder Flag**: `method_id == 6` or single participant
- **Repeated Winner Flag**: Provider wins >60% with same customer (≥5 contracts)
- **Contract Splitting Flag**: ≥3 lots ≤100K KZT within 30 days  
- **Underpayment Flag**: Payment <90% of contract value
- **Price Inflation Flag**: Price Z-score >3 standard deviations

### Risk Scoring
```
risk_score = 2×price_flag + 1.5×single_flag + 1.5×repeat_flag + split_flag + underpaid_flag
```

Categories: Low (<1), Medium (1-3), High (≥3)

## Dashboard Features

### 📊 Interactive Visualizations
- Risk distribution pie charts
- Temporal trend analysis
- Corruption indicator frequency
- Market concentration analysis

### 🔍 Advanced Filtering
- Date range selection
- Risk level filtering
- Contract value ranges
- Customer/provider selection
- Corruption flag filtering

### 📋 Risk Management
- Traffic-light risk table
- High-risk contract identification
- CSV export functionality
- Real-time statistics

### 🔬 Analytics
- Customer analysis (top spenders, risk patterns)
- Provider analysis (market share, win rates)
- Market concentration metrics

## Performance & Scalability

### Rate Limiting
- Maximum 50 concurrent requests
- 5 requests per second limit
- Exponential backoff (1-3-10 seconds) for errors

### Data Processing
- Chunked processing for large datasets
- ZSTD compression for Parquet files
- DuckDB for efficient SQL joins
- Async I/O throughout pipeline

### Expected Scale
- **Time Period**: 2018-2025 (7+ years)
- **Data Volume**: ~5GB raw JSON, ~1GB processed
- **Records**: Hundreds of thousands of contracts
- **Processing Time**: 2-4 hours for complete pipeline

## Machine Learning

### Isolation Forest Model
- **Algorithm**: Unsupervised anomaly detection
- **Contamination**: 5% (configurable)
- **Features**: Contract values, ratios, temporal patterns
- **Output**: Anomaly scores + binary classifications

### Model Persistence
- Trained models saved as pickle files
- Feature scalers preserved
- Column mappings maintained
- Easy model reloading for inference

## API Integration

### EOZ Endpoints Used
- `POST /api/uicommand/get/page` - Entity listings
- `POST /api/uicommand/get/object` - Full object details

### Special Requirements
- **Cookies**: Plan entity requires specific session cookies
- **Rate Limits**: Respect ~5 req/s limit
- **Error Handling**: Robust retry logic for 502/504 errors

## Development

### Code Standards
- **Python 3.11** with type hints
- **Black** code formatting
- **Poetry** dependency management
- **Structured logging** with contextual information

### Testing
```bash
# Run tests
poetry run pytest

# Code formatting
poetry run black src/

# Type checking
poetry run mypy src/
```

### Dependencies
- **Core**: httpx, pandas, duckdb, scikit-learn
- **Async**: asyncio, tqdm
- **Visualization**: streamlit, plotly
- **Utilities**: structlog, python-dotenv

## Monitoring & Logging

### Structured Logging
- JSON-formatted logs with context
- Error tracking with stack traces
- Performance metrics logging
- Progress tracking for long operations

### Agent Logs
All actions are logged to `agent-logs.md` for session continuity and debugging.

## Security & Compliance

### Data Privacy
- No authentication credentials stored
- Public procurement data only
- Local processing (no external API calls)

### Error Handling
- Graceful degradation for missing data
- Comprehensive exception handling
- Data validation at each pipeline stage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Follow the coding standards
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## License

This project is designed for transparency and anti-corruption research. Please use responsibly and in accordance with local laws and regulations.

---

**Built for the Kazakhstan EOZ Hackathon 2025**  
*Empowering transparency through AI-driven procurement analysis*
