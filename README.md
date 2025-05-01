# Football Predictions System

An automated system for generating and publishing football (soccer) match previews and predictions.

## Overview

This project automatically collects data about upcoming football matches, generates predictions and analyses using multiple statistical models, and publishes articles to a WordPress site. Articles are published 12 hours before each match and automatically removed 8 hours after the match ends.

## Features

- **Automated Data Collection**: Gathers match data from multiple sources including official APIs and web scraping
- **Advanced Statistical Analysis**: Uses multiple models including Expected Goals (xG), Poisson, and form-based analysis
- **Value Bet Detection**: Identifies betting opportunities with positive expected value
- **Multi-language Support**: Currently supports English and Italian article generation
- **Automated WordPress Publishing**: Handles the full lifecycle of articles including scheduling and expiration
- **Comprehensive Monitoring**: Health checks and metrics tracking
- **Completely Free**: Uses only free resources for operation

## Structure

The project is organized into several modules:

```
football-predictions-system/
├── .github/workflows/      # GitHub Actions workflows
├── src/                    # Source code
│   ├── analytics/          # Statistical models and analysis
│   ├── content/            # Content generation
│   ├── data/               # Data collection and processing
│   ├── monitoring/         # System monitoring and logging
│   ├── publishing/         # WordPress integration
│   ├── translation/        # Translation services (optional)
│   └── utils/              # Shared utilities
├── scripts/                # Utility scripts
├── tests/                  # Test suite
├── cache/                  # Local cache directory
├── logs/                   # Log files
└── requirements.txt        # Python dependencies
```

## Setup

### Prerequisites

- Python 3.10+
- Firebase Realtime Database
- WordPress site with REST API access
- GitHub account (for automation)
- PythonAnywhere account (for hosting)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/football-predictions-system.git
   cd football-predictions-system
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   Create a `.env` file with:
   ```
   FIREBASE_CREDENTIALS=path/to/firebase_credentials.json
   FIREBASE_DB_URL=https://your-firebase-project.firebaseio.com
   FOOTBALL_API_KEY=your_api_key
   WP_URL=https://your-wordpress-site.com/wp-json/wp/v2/posts
   WP_USER=username
   WP_APP_PASSWORD=app_password
   ```

4. Initialize the system:
   ```bash
   python scripts/setup.py
   ```

### GitHub Actions Setup

1. Add the following secrets to your GitHub repository:
   - `FIREBASE_CREDENTIALS`: Base64-encoded Firebase credentials JSON
   - `FIREBASE_DB_URL`: Firebase database URL
   - `FOOTBALL_API_KEY`: Football-data.org API key
   - `WP_URL`: WordPress API URL
   - `WP_USER`: WordPress username
   - `WP_APP_PASSWORD`: WordPress application password

2. The workflows will automatically run according to the defined schedule.

## Usage

### Manual Execution

You can run the system manually with:

```bash
# Run the complete process
python scripts/daily_coordinator.py --all

# Run individual steps
python scripts/daily_coordinator.py --collect-data
python scripts/daily_coordinator.py --generate-content
python scripts/daily_coordinator.py --publish-articles
python scripts/daily_coordinator.py --cleanup-expired
```

### Customization

- Modify prediction models in `src/analytics/models/`
- Customize article templates in `src/content/templates/`
- Configure data sources in `src/config/sources.py`
- Add or modify leagues in `src/config/leagues.py`

## Testing

Run the test suite:

```bash
pytest
```

Run tests with coverage:

```bash
pytest --cov=src
```

## Monitoring

Check system health:

```bash
python src/monitoring/health_checker.py --check
```

Generate a detailed health report:

```bash
python src/monitoring/health_checker.py --report
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgements

- Data provided by [Football-Data.org](https://www.football-data.org/)
- Stats from [FBref](https://fbref.com/), [Understat](https://understat.com/), and other public sources
- Built with Python, Firebase, and WordPress
