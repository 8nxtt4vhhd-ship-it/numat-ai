# Numat AI Backend

FastAPI backend for analysing customer order data and identifying customers needing attention.

## Local Setup

From the project root:

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create your local environment file:

```bash
cp .env.example .env
```

Edit `.env` and add your local values:

```bash
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1-mini
APP_BASIC_AUTH_USERNAME=
APP_BASIC_AUTH_PASSWORD=

ORDER_DATA_SOURCE=mock
SAMPLE_ORDERS_CSV_PATH=/path/to/sample-data.csv
CRM_SAMPLE_CSV_PATH=/path/to/sample-data-crm.csv
CRM_DATA_SOURCE=sample_csv
CRM_INTERNAL_DOMAINS=numatsystems.com,nufox.com
ANALYSIS_TODAY=2026-04-22
FILEMAKER_ORDER_CACHE_SECONDS=120
CRM_CACHE_SECONDS=120
FILEMAKER_ORDER_LIMIT=5000
FILEMAKER_CRM_LIMIT=5000
FILEMAKER_CRM_USE_SYNC_CACHE=true
FILEMAKER_CRM_FETCH_ALL=false
FILEMAKER_CRM_BATCH_SIZE=5000
FILEMAKER_CRM_SORT_FIELD=Date Created

FILEMAKER_URL=https://your-filemaker-server
FILEMAKER_DATABASE=your-database-name
FILEMAKER_USERNAME=your-username
FILEMAKER_PASSWORD=your-password
FILEMAKER_VERIFY_SSL=true
FILEMAKER_ORDERS_LAYOUT=numat_ai_Orders
FILEMAKER_EMAILS_LAYOUT=numat_ai_EmailTablev2
FILEMAKER_CUSTOMER_FIELD=Orders::Customer Name
FILEMAKER_ORDER_DATE_FIELD=Orders::CreationTimestamp
FILEMAKER_AMOUNT_FIELD=Orders::Repair Value
FILEMAKER_DATE_ORDER=mdy
FILEMAKER_LAST_ACTIVITY_CONTENT_FIELD=Companies 4::Last Activity Act Content
FILEMAKER_EXTRA_FIELDS=Companies 4::Price List,Orders::First Order,Orders::Order No,Orders::Invoice Number,Orders::No of Mats,Orders::Status,Companies 4::State,Companies 4::Territory,Companies 4::ZIP Code,Companies 4::First Order Date,Companies 4::Last Order Date,Companies 4::Last Activity Act,Companies 4::Last Activity Act Content,Companies 4::PrimaryKey
```

Run the API locally:

```bash
uvicorn main:app --reload
```

If port `8000` is already in use:

```bash
uvicorn main:app --reload --port 8001
```

Useful local URLs:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/api`
- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/filemaker-health`
- `http://127.0.0.1:8000/orders`
- `http://127.0.0.1:8000/crm-activities`
- `http://127.0.0.1:8000/crm-activities-view`
- `http://127.0.0.1:8000/sample-data`
- `http://127.0.0.1:8000/filemaker-orders`
- `http://127.0.0.1:8000/customers-needing-attention`
- `http://127.0.0.1:8000/customers-needing-attention-view`
- `http://127.0.0.1:8000/late-customers`
- `http://127.0.0.1:8000/customer-view?customer=Cintas%20%28Columbus%2C%20OH%29`

## Deployment

Install dependencies on the server:

```bash
cd /path/to/numat-ai/backend
pip install -r requirements.txt
```

Set environment variables on the server using your hosting platform, process manager, or secret manager:

```bash
OPENAI_API_KEY
OPENAI_MODEL
APP_BASIC_AUTH_USERNAME
APP_BASIC_AUTH_PASSWORD
ORDER_DATA_SOURCE
SAMPLE_ORDERS_CSV_PATH
CRM_SAMPLE_CSV_PATH
CRM_DATA_SOURCE
CRM_INTERNAL_DOMAINS
ANALYSIS_TODAY
FILEMAKER_ORDER_CACHE_SECONDS
CRM_CACHE_SECONDS
FILEMAKER_ORDER_LIMIT
FILEMAKER_CRM_LIMIT
FILEMAKER_CRM_USE_SYNC_CACHE
FILEMAKER_CRM_FETCH_ALL
FILEMAKER_CRM_BATCH_SIZE
FILEMAKER_CRM_SORT_FIELD
FILEMAKER_URL
FILEMAKER_DATABASE
FILEMAKER_USERNAME
FILEMAKER_PASSWORD
FILEMAKER_VERIFY_SSL
FILEMAKER_ORDERS_LAYOUT
FILEMAKER_EMAILS_LAYOUT
FILEMAKER_CUSTOMER_FIELD
FILEMAKER_ORDER_DATE_FIELD
FILEMAKER_AMOUNT_FIELD
FILEMAKER_DATE_ORDER
FILEMAKER_LAST_ACTIVITY_CONTENT_FIELD
FILEMAKER_EXTRA_FIELDS
```

Start the API without `--reload`:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Password Rotation

Do not hardcode FileMaker credentials in Python files.

To rotate the FileMaker password:

1. Change the password in FileMaker.
2. Update `FILEMAKER_PASSWORD` in the server environment or secret manager.
3. Restart the FastAPI process.
4. Check `/health` to confirm FileMaker configuration is present.
5. Check `/filemaker-health` to confirm FileMaker login succeeds.

If this repository has ever been pushed or shared with an old password committed, rotate that FileMaker password immediately.

The `/filemaker-health` endpoint returns only connection state, not passwords, tokens, or FileMaker response bodies.

For local FileMaker servers using a self-signed certificate or an internal IP address, set this locally:

```env
FILEMAKER_VERIFY_SSL=false
```

Use `FILEMAKER_VERIFY_SSL=true` in production with a valid certificate.

## FileMaker Data Pull

The `/filemaker-orders` endpoint logs in to FileMaker, fetches records from `FILEMAKER_ORDERS_LAYOUT`, maps configured FileMaker fields into the backend order shape, and logs out again.

Expected order shape:

```json
{
  "filemaker_record_id": "1",
  "customer": "ABC Ltd",
  "order_date": "2024-03-01",
  "amount": 210,
  "extra": {
    "Orders::Order No": "12345",
    "Orders::Status": "Complete"
  }
}
```

Use query parameters while testing:

```text
http://127.0.0.1:8000/filemaker-orders?limit=10&offset=1
```

## Data Sources

The `/customers-needing-attention` endpoint reads orders from the source configured by `ORDER_DATA_SOURCE`.

Available values:

- `mock`: use the hardcoded test records in `analysis.py`
- `sample_csv`: use a local CSV file from `SAMPLE_ORDERS_CSV_PATH`
- `filemaker`: fetch live records from FileMaker

CRM data uses `CRM_DATA_SOURCE`:

- `sample_csv`: use a local CSV file from `CRM_SAMPLE_CSV_PATH`
- `filemaker`: fetch live emails from `FILEMAKER_EMAILS_LAYOUT`

For large live CRM tables, you can fetch the whole table in batches and sort it locally:

- `FILEMAKER_CRM_USE_SYNC_CACHE=true`
- `FILEMAKER_CRM_FETCH_ALL=true`
- `FILEMAKER_CRM_BATCH_SIZE=5000`

That makes the first load heavier, but keeps FileMaker happier than asking it to sort a very large email table server-side.

For local CSV testing:

```env
ORDER_DATA_SOURCE=sample_csv
SAMPLE_ORDERS_CSV_PATH=/Users/kellybainbridge/Documents/sample data.csv
CRM_SAMPLE_CSV_PATH=/Users/kellybainbridge/Documents/sample data crm.csv
CRM_INTERNAL_DOMAINS=numatsystems.com,nufox.com
ANALYSIS_TODAY=2026-04-22
```

For live FileMaker:

```env
ORDER_DATA_SOURCE=filemaker
```

Use `/orders` to inspect the currently selected source before running `/customers-needing-attention`.

The home page at `/` includes navigation, summary tiles, an order trend chart, an attention breakdown chart, an orders-by-state chart, and highest-priority customers.

The browser views include these conveniences:

- `/sample-data`: upload and validate a replacement sample CSV for testing.
- `/orders-view`: filter by customer/status and sort by date, amount, order number, status, territory, or last activity.
- `/crm-activities-view`: inspect normalized CRM email rows, filter by customer or direction, and verify the customer primary key mapping.
- `/customers-needing-attention-view`: filter by customer/action, sort by priority or overdue timing, see last activity, and read the priority score guide.
- `/customer-view?customer=...`: inspect one customer's order history and totals.

Older `/late-customers` URLs still work as aliases for compatibility.

## Hosting Preview Access

For a hosted preview environment, set:

```env
APP_BASIC_AUTH_USERNAME=your-preview-username
APP_BASIC_AUTH_PASSWORD=your-preview-password
```

When both are set, the app requires browser basic auth on the main app routes. Health checks stay open.

For a first hosted preview on Render, see:

- [deploy-checklist.md](/Users/kellybainbridge/projects/numat-ai/deploy-checklist.md)
- [render.yaml](/Users/kellybainbridge/projects/numat-ai/render.yaml)
