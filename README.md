# immoclaude-bot

Playwright automation for the Immocloud DATEV export flow.

## Setup

```bash
npm i
npx playwright install chromium
cp .env.example .env
```

Fill in `.env` with your credentials and, if needed, the export period.

## Run

```bash
npm run datev
```

To send files directly to a webhook without any parsing or DATEV processing:

```bash
npm run webhook -- ./downloads/some-export
```

By default, `.csv` files are converted to `.xlsx` before upload so they open directly in Excel. Other file types are sent unchanged.

You can also pass individual files:

```bash
npm run webhook -- ./file1.csv ./file2.pdf
```

To import CSV files into Google Sheets:

```bash
npm run google-sheets -- ./downloads/some-export
```

For personal `My Drive`, prefer Google OAuth user credentials instead of a service account. On first run the script opens a local callback flow, asks you to approve access, and caches the refresh token locally.
After each successful import, the script can also POST the created Google Sheet ids and links to a webhook.

## Environment

- `IMMO_EMAIL` and `IMMO_PASSWORD` are required.
- `HEADLESS=false` opens a visible browser window.
- `DATEV_EXPORT_START` and `DATEV_EXPORT_END` accept `YYYY-MM-DD` or `DD.MM.YYYY`.
- `DATEV_EXPORT_RANGE` accepts `start:end` in `YYYY-MM-DD` form.
- If no export dates are set, the script uses the previous calendar month.
- If no owner is specified, the script defaults to `Arona GmbH`.
- Downloaded files are saved to `downloads/` unless `DOWNLOAD_DIR` is set.
- Login persists between runs through `.auth/immocloud-storage-state.json` unless `PERSIST_AUTH=false`.
- `DATEV_PROCESS_FILES=false` disables local DATEV CSV transformations.
- If you need `KOST1` enrichment, set `DATEV_KOST1_MAP_FILE` to a CSV or JSON file with mappings from Immo data to `DatevKostenStelle`.
- The script copies the unpacked files into `downloads/<export>/processed/` and rewrites the `EXTF_Buchungsstapel` CSV there.
- If `WEBHOOK_UPLOAD=true`, original CSV files from the downloaded DATEV ZIP are sent to `WEBHOOK_URL` as `multipart/form-data`.
- `WEBHOOK_FILE_FIELD` controls the multipart file field name and defaults to `files`.
- If `WEBHOOK_TOKEN` is set, it is sent as a `Bearer` authorization header.
- `WEBHOOK_SOURCE_DIR` is an optional fallback path for `npm run webhook` when no CLI path arguments are passed.
- `WEBHOOK_CONVERT_CSV_TO_XLSX=false` disables the automatic CSV to Excel conversion in `npm run webhook`.
- `GOOGLE_SHEETS_IMPORT=true` imports the original downloaded CSV files into Google Sheets as part of `npm run datev`.
- `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` enable Google user OAuth for the integrated Google Sheets import.
- `GOOGLE_OAUTH_REDIRECT_URI` defaults to `http://127.0.0.1:3000/oauth2callback`.
- `GOOGLE_OAUTH_TOKEN_FILE` stores the cached OAuth token and defaults to `.auth/google-oauth-token.json`.
- `GOOGLE_SHEETS_WEBHOOK_URL` sets the webhook endpoint for created Google Sheet metadata.
- `GOOGLE_SHEETS_WEBHOOK_ENABLED=false` disables the webhook call after import.
- `GOOGLE_SHEETS_WEBHOOK_TOKEN` adds an optional Bearer token to that webhook call.
- If `GOOGLE_DRIVE_UPLOAD=true`, processed files are uploaded to Google Drive folder `GOOGLE_DRIVE_FOLDER_ID`.
- For service-account uploads, the target folder must be in a Shared Drive with the service account added to it. Uploading to a regular personal `My Drive` folder with a plain service account will fail with `Service Accounts do not have storage quota`.
- Drive auth is read from `GOOGLE_DRIVE_CREDENTIALS_FILE` (or `GOOGLE_APPLICATION_CREDENTIALS`) or `GOOGLE_DRIVE_CREDENTIALS_JSON`.

## `KOST1` map format

CSV example:

```csv
match;datevKostenStelle
Kleyerstr;8301
Some street 1;1201
```

JSON example:

```json
[
  { "match": "Kleyerstr", "datevKostenStelle": "8301" },
  { "match": "Some street 1", "datevKostenStelle": "1201" }
]
```

## Flow

1. Open the Immocloud login page.
2. Sign in with the credentials from `.env`.
3. Go to `/settings/datev`.
4. Fill the export period and fiscal year start.
5. Select the owner, defaulting to `Arona GmbH`.
6. Create the DATEV export.
7. Wait for the new history row and download the generated file.
8. Unpack the ZIP.
9. Save auth state for the next run.
10. Optionally transform DATEV files locally if `DATEV_PROCESS_FILES=true`.
11. Optionally upload original CSV files to a webhook.
12. Optionally import original CSV files into Google Sheets / Google Drive.
13. Optionally POST created Google Sheet metadata to a webhook.
14. Optionally upload processed files to Google Drive.
