# Company GTM Enricher

`company-gtm-enricher` is a lightweight local tool for uploading a CSV of company names, enriching each row with public go-to-market data, and downloading the refreshed CSV.

## Status

Repository scaffold is in place. The next iteration adds the enrichment app, provider integrations, and tests.

## Planned Capabilities

- Upload a CSV with a company-name column
- Enrich each company with:
  - HQ city
  - HQ state or region
  - HQ country
  - approximate annual revenue
  - current total funding
- Download a new CSV with the added columns
- Keep the enrichment provider configurable so the app can evolve without changing the UI

## Tech Stack

- Python 3.9+
- Streamlit for the local web UI
- Pandas for CSV handling
- OpenAI API for web-grounded enrichment

## Getting Started

1. Create a virtual environment.
2. Install the project dependencies.
3. Copy `.env.example` to `.env` and add your API credentials.
4. Run the local app.

Detailed setup instructions will be expanded in the feature branch.

## Repository Layout

```text
.
├── examples/
├── src/company_gtm_enricher/
├── tests/
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

## License

MIT
