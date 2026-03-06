# Company GTM Enricher

`company-gtm-enricher` is a local CSV enrichment tool for lightweight go-to-market research. Upload a CSV that contains company names, enrich each row with public business data, and download a refreshed CSV in one pass.

## What It Adds

For each company, the tool appends:

- `HQ City`
- `HQ State`
- `Country`
- `Approximate Annual Revenue`
- `Current Total Funding`

It can also add audit columns:

- `Enrichment Confidence`
- `Source URLs`
- `Enrichment Status`
- `Research Notes`

## How It Works

- Streamlit provides the local web UI for upload, preview, processing, and download.
- Pandas handles CSV parsing and output generation.
- A provider layer keeps enrichment logic separate from the UI.
- The default live provider uses the OpenAI API to do public-web research and return a structured JSON payload.
- OpenAI requests are batched by unique company names to reduce request count.
- A mock provider is included for dry runs and UI testing without an API key.
- The UI shows elapsed runtime and writes an interim backup CSV every 10 minutes during long runs.

## Project Structure

```text
.
├── app.py
├── examples/
│   └── companies.csv
├── src/company_gtm_enricher/
│   ├── cli.py
│   ├── config.py
│   ├── csv_tools.py
│   ├── enrichment_service.py
│   ├── models.py
│   └── providers/
├── tests/
├── .env.example
├── .gitignore
├── LICENSE
└── pyproject.toml
```

## Quick Start

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -e '.[dev]'
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Set `OPENAI_API_KEY` if you want live enrichment. If you only want to test the flow, use the `mock` provider in the UI or CLI.

### 4. Run the app

```bash
python -m streamlit run app.py
```

The app will open locally and let you:

- upload a CSV
- select the company-name column
- choose the provider
- tune the request batch size
- run enrichment
- pause, resume, or stop an in-flight run
- see elapsed runtime while the job is active
- download the latest interim backup if a 10-minute flush has occurred
- download the enriched CSV

## CLI Usage

You can also run enrichment without the web UI:

```bash
python -m company_gtm_enricher.cli --input examples/companies.csv --output enriched.csv --provider mock
```

Example with a live provider:

```bash
python -m company_gtm_enricher.cli --input companies.csv --output companies_enriched.csv --provider openai
```

Useful flags:

- `--company-column` to explicitly choose the company-name column
- `--model` to override the OpenAI model
- `--batch-size` to choose how many unique companies go into each request
- `--no-audit-columns` to keep the output narrower

## Input Requirements

- CSV format only
- At least one column containing company names
- If the file has a single column, the app uses it automatically
- If the file has multiple columns, the app lets you select the company-name column

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPENAI_API_KEY` | empty | Required for live enrichment |
| `OPENAI_MODEL` | `gpt-4o-mini-search-preview` | Search-capable model used by the OpenAI provider |
| `OPENAI_TIMEOUT_SECONDS` | `45` | Request timeout for the OpenAI client |
| `OPENAI_BATCH_SIZE` | `10` | Default number of unique companies per OpenAI request |
| `MAX_COMPANIES_PER_RUN` | empty | Optional cap for UI batch size. Empty or `0` means unlimited |

## Notes on Data Quality

- Revenue and funding are intentionally approximate.
- Public company data is often easier to verify than private-company data.
- The audit columns are useful when you need to review low-confidence rows manually.
- Duplicate company names in the same upload are cached during a run to avoid duplicate lookups.
- Pause or stop actions take effect between request batches, so a large batch may need to finish before the UI reflects the action.
- Interim backups are written to `backups/` every 10 minutes and again on stop/failure when partial progress exists.

## Tests

Run the test suite with:

```bash
python -m pytest
```

## Future Improvements

- Add provider retries and rate-limit backoff
- Add optional source weighting or domain allowlists
- Add background job processing for larger CSV files
- Export a second review-only CSV for low-confidence rows

## License

MIT
