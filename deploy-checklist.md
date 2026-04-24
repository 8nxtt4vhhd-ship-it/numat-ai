# Deployment Checklist

## Recommended First Host

Use Render for the first shared preview with sales reps.

Why:

- easy FastAPI deployment
- simple environment variable management
- public URL for testing
- low setup overhead

## What To Prepare First

Before deployment, make sure you have:

- the code pushed to GitHub
- the exact FileMaker environment values
- a preview username and password for the rep login wall

## Render Setup

### 1. Create the service

In Render:

1. Connect the GitHub repository
2. Create a new Web Service
3. Render should detect the included `render.yaml`

If you enter the settings manually, use:

- Root directory: `backend`
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## Environment Variables To Set

### Security

- `APP_BASIC_AUTH_USERNAME`
- `APP_BASIC_AUTH_PASSWORD`

### OpenAI

- `OPENAI_API_KEY`
- `OPENAI_MODEL`

### App data source

- `ORDER_DATA_SOURCE=filemaker`
- `CRM_DATA_SOURCE=filemaker`
- `ANALYSIS_TODAY`

### FileMaker

- `FILEMAKER_URL`
- `FILEMAKER_DATABASE`
- `FILEMAKER_USERNAME`
- `FILEMAKER_PASSWORD`
- `FILEMAKER_VERIFY_SSL=true`
- `FILEMAKER_ORDERS_LAYOUT`
- `FILEMAKER_EMAILS_LAYOUT`
- `FILEMAKER_CUSTOMER_FIELD`
- `FILEMAKER_ORDER_DATE_FIELD`
- `FILEMAKER_AMOUNT_FIELD`
- `FILEMAKER_DATE_ORDER`
- `FILEMAKER_EXTRA_FIELDS`

### CRM / caching

- `FILEMAKER_ORDER_CACHE_SECONDS`
- `CRM_CACHE_SECONDS`
- `FILEMAKER_ORDER_LIMIT`
- `FILEMAKER_CRM_LIMIT`
- `FILEMAKER_CRM_USE_SYNC_CACHE=true`
- `FILEMAKER_CRM_FETCH_ALL=false`
- `FILEMAKER_CRM_BATCH_SIZE`
- `FILEMAKER_CRM_SORT_FIELD`

## First Deployment Checks

After the site is live:

1. Open `/health`
2. Open `/filemaker-health`
3. Log in through the basic auth prompt
4. Open the home page
5. Open `/crm-data`
6. Run `Sync Full CRM from FileMaker`
7. Check:
   - home page action plan
   - hold / recently contacted
   - customer pages
   - CRM Activities

## Recommended Sharing Approach

For the first rep review:

- keep the site behind the basic auth prompt
- share only with the sales reps involved
- use the hosted URL for a live walkthrough

## Practical Notes

- first CRM sync may take a while
- normal app use should be much faster after sync/cache is in place
- FileMaker connectivity is the main thing to verify after deployment

## Suggested Next Step After Deploy

Once the hosted preview is working:

1. test it yourself end to end
2. run the sales rep demo
3. collect the team’s feature requests
4. decide the first AI workflow to build from their feedback
