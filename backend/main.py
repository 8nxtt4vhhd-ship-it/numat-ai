from collections import Counter, defaultdict
import base64
from datetime import datetime, timedelta
from html import escape
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import secrets
from threading import Lock, Thread
import time
from urllib.parse import quote, urlencode

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from ai import add_ai_explanations
from ai import build_outreach_prep_fallback
from ai import generate_outreach_prep
from analysis import (
    calculate_average_gap,
    get_analysis_today,
    get_last_activity,
    get_last_activity_info,
    group_by_customer,
    find_late_customers,
)
from data_sources import (
    MAX_SAMPLE_ROWS,
    get_orders_for_analysis,
    get_sample_csv_path,
    validate_sample_csv_content,
    validate_sample_csv_path,
)
from crm import (
    MAX_CRM_SAMPLE_ROWS,
    extract_emails,
    fetch_crm_activities,
    get_crm_data_source,
    get_filemaker_crm_cache_path,
    is_customer_service_activity,
    get_crm_sample_csv_path,
    get_uploaded_crm_csv_path,
    sync_filemaker_crm_cache,
    validate_crm_csv_content,
    validate_crm_csv_path,
)
from filemaker import (
    check_filemaker_connection,
    fetch_filemaker_master_data,
    fetch_order_records,
    has_filemaker_config,
)

app = FastAPI()

CRM_SYNC_STATUS_LOCK = Lock()
CRM_SYNC_STATUS = {
    "running": False,
    "started_at": "",
    "finished_at": "",
    "status": "",
    "saved": False,
    "message": "",
}

_ATTENTION_RESPONSE_CACHE = {
    "key": None,
    "expires_at": 0,
    "result": None,
}

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ACTION_PLAN_DISMISSALS_PATH = BASE_DIR / "data" / "action_plan_dismissals.json"


PREVIEW_AUTH_EXEMPT_PATHS = {
    "/health",
    "/filemaker-health",
}


@app.on_event("startup")
def startup_prewarm():
    if not should_enable_startup_prewarm():
        return

    Thread(target=prewarm_app_caches, daemon=True).start()


def get_preview_auth_credentials():
    username = os.getenv("APP_BASIC_AUTH_USERNAME", "").strip()
    password = os.getenv("APP_BASIC_AUTH_PASSWORD", "").strip()
    return username, password


def is_preview_auth_enabled():
    username, password = get_preview_auth_credentials()
    return bool(username and password)


def is_preview_auth_exempt(path):
    if path in PREVIEW_AUTH_EXEMPT_PATHS:
        return True

    return path.startswith("/docs") or path.startswith("/openapi")


def unauthorized_preview_response():
    return PlainTextResponse(
        "Authentication required.",
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Numat AI Preview"'},
    )


def get_crm_sync_status():
    with CRM_SYNC_STATUS_LOCK:
        return dict(CRM_SYNC_STATUS)


def update_crm_sync_status(**updates):
    with CRM_SYNC_STATUS_LOCK:
        CRM_SYNC_STATUS.update(updates)


def run_crm_sync_in_background():
    started_at = format_optional_datetime(datetime.now())
    update_crm_sync_status(
        running=True,
        started_at=started_at,
        finished_at="",
        status="running",
        saved=False,
        message="Full CRM sync started. You can keep using the app while it runs.",
    )

    try:
        result = sync_filemaker_crm_cache()
        finished_at = format_optional_datetime(datetime.now())

        if result.get("status") == "ok":
            update_crm_sync_status(
                running=False,
                finished_at=finished_at,
                status="ok",
                saved=True,
                message="Full CRM sync completed and the hosted cache was refreshed.",
            )
        else:
            update_crm_sync_status(
                running=False,
                finished_at=finished_at,
                status=result.get("status", "error"),
                saved=False,
                message=(
                    "Full CRM sync did not complete. "
                    f"Latest status: {result.get('status', 'error')}."
                ),
            )
    except Exception as exc:
        finished_at = format_optional_datetime(datetime.now())
        update_crm_sync_status(
            running=False,
            finished_at=finished_at,
            status="error",
            saved=False,
            message=f"Full CRM sync failed: {str(exc)}",
        )


@app.middleware("http")
async def preview_basic_auth(request: Request, call_next):
    if not is_preview_auth_enabled() or is_preview_auth_exempt(request.url.path):
        return await call_next(request)

    authorization = request.headers.get("Authorization", "")

    if not authorization.startswith("Basic "):
        return unauthorized_preview_response()

    try:
        encoded_credentials = authorization.split(" ", 1)[1]
        decoded_credentials = base64.b64decode(encoded_credentials).decode("utf-8")
        supplied_username, supplied_password = decoded_credentials.split(":", 1)
    except (ValueError, UnicodeDecodeError, base64.binascii.Error):
        return unauthorized_preview_response()

    expected_username, expected_password = get_preview_auth_credentials()

    if not (
        secrets.compare_digest(supplied_username, expected_username)
        and secrets.compare_digest(supplied_password, expected_password)
    ):
        return unauthorized_preview_response()

    return await call_next(request)


@app.get("/", response_class=HTMLResponse)
def read_root(selected_customer: str = "", view: str = "focus", dismiss_customer: str = ""):
    return render_home_page(
        selected_customer=selected_customer,
        view=view,
        dismiss_customer=dismiss_customer,
    )


@app.get("/api")
def read_api_root():
    return {
        "message": "API is working",
        "orders_json": "/orders",
        "orders_view": "/orders-view",
        "crm_activities_json": "/crm-activities",
        "crm_activities_view": "/crm-activities-view",
        "crm_data": "/crm-data",
        "customers_view": "/customers-view",
        "sample_data": "/sample-data",
        "customers_needing_attention_json": "/customers-needing-attention",
        "customers_needing_attention_view": "/customers-needing-attention-view",
        "late_customers_json": "/late-customers",
        "late_customers_view": "/late-customers-view"
    }


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "filemaker_configured": has_filemaker_config()
    }


@app.get("/filemaker-health")
def filemaker_health_check():
    return check_filemaker_connection()


@app.get("/filemaker-orders")
def get_filemaker_orders(limit: int = 100, offset: int = 1):
    return fetch_order_records(limit=limit, offset=offset)


@app.get("/orders")
def get_orders():
    return get_orders_for_analysis()


@app.get("/crm-activities")
def get_crm_activities():
    return fetch_crm_activities()


@app.get("/crm-activities-view", response_class=HTMLResponse)
def get_crm_activities_view(
    customer: str = "",
    direction: str = "",
    category: str = "",
    subject: str = "",
    date_from: str = "",
    date_to: str = "",
    range_key: str = "all",
    page: int = 1,
    page_size: int = 100,
):
    result = fetch_crm_activities()

    if result["status"] != "ok":
        return render_page(
            title="CRM Activities",
            body=(
                f"<p class='status error'>Could not load CRM data from "
                f"{escape(result['path'])}: {escape(result['status'])}</p>"
            )
        )

    filtered_activities = filter_crm_activities(
        result["activities"],
        customer=customer,
        direction=direction,
        category=category,
        subject=subject,
        date_from=date_from,
        date_to=date_to,
        range_key=range_key,
    )
    page_size = max(25, min(page_size, 250))
    total_filtered = len(filtered_activities)
    total_pages = max(1, (total_filtered + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    paged_activities = filtered_activities[start_index:end_index]
    rows = "".join(
        render_crm_activity_row(activity)
        for activity in paged_activities
    )
    has_active_filters = any([
        customer.strip(),
        direction.strip(),
        category.strip(),
        subject.strip(),
        date_from.strip(),
        date_to.strip(),
        str(range_key or "").strip().lower() not in ["", "all"],
    ])

    if not rows:
        empty_message = (
            "No CRM activity matches the current filters."
            if has_active_filters else
            "No CRM activity found."
        )
        rows = (
            "<tr>"
            f"<td colspan='2' class='empty'>{empty_message}</td>"
            "</tr>"
        )

    counts = result["counts"]
    filter_summary = render_crm_filter_summary(
        total_filtered=total_filtered,
        customer=customer,
        direction=direction,
        category=category,
        subject=subject,
        date_from=date_from,
        date_to=date_to,
        range_key=range_key,
    )
    body = f"""
        {render_data_availability_banner(result)}

        <div class="summary crm-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(result["source"])}</strong>
            </div>
            <div>
                <span class="label">Total Rows</span>
                <strong>{counts["total_rows"]}</strong>
            </div>
            <div>
                <span class="label">Usable Rows</span>
                <strong>{counts["kept_rows"]}</strong>
            </div>
            <div>
                <span class="label">Internal Only Removed</span>
                <strong>{counts["excluded_internal_only"]}</strong>
            </div>
            <div>
                <span class="label">Showing</span>
                <strong>{len(paged_activities)} of {total_filtered}</strong>
            </div>
        </div>

        {render_crm_filter_form(customer, direction, category, subject, page_size, date_from, date_to, range_key)}
        {filter_summary}

        <div class="table-wrap tall-table crm-activities-wrap">
        <table class="crm-activities-table">
            <thead>
                <tr>
                    <th>Details</th>
                    <th>Preview</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </div>

        {render_crm_pagination(customer, direction, category, subject, date_from, date_to, range_key, page, total_pages, page_size, total_filtered)}
    """

    return render_page(title="CRM Activities", body=body)


@app.get("/crm-data", response_class=HTMLResponse)
def get_crm_data_page():
    return render_crm_data_page()


@app.post("/crm-data", response_class=HTMLResponse)
async def upload_crm_data(file: UploadFile = File(...)):
    filename = file.filename or ""

    if not filename.lower().endswith(".csv"):
        return render_crm_data_page(
            upload_result={
                "valid": False,
                "status": "invalid_file_type",
                "warnings": [],
                "errors": ["Please upload a .csv file."],
                "row_count": 0,
                "customer_count": 0,
                "usable_count": 0,
                "excluded_internal_only": 0,
            }
        )

    content = await file.read()
    validation = validate_crm_csv_content(content)

    if validation["valid"]:
        crm_path = get_uploaded_crm_csv_path()
        crm_path.parent.mkdir(parents=True, exist_ok=True)
        crm_path.write_bytes(content)
        validation["path"] = str(crm_path)
        validation["saved"] = True
    else:
        validation["saved"] = False

    return render_crm_data_page(upload_result=validation)


@app.post("/crm-sync-full", response_class=HTMLResponse)
def post_crm_sync_full():
    sync_status = get_crm_sync_status()

    if not sync_status.get("running"):
        worker = Thread(target=run_crm_sync_in_background, daemon=True)
        worker.start()
        sync_result = {
            "status": "started",
            "saved": False,
            "message": "Full CRM sync started in the background.",
        }
    else:
        sync_result = {
            "status": "running",
            "saved": False,
            "message": "A full CRM sync is already running.",
        }

    return render_crm_data_page(sync_result=sync_result)


@app.post("/action-plan-dismiss")
def post_action_plan_dismiss(
    customer: str = Form(...),
    last_order: str = Form(""),
    reason: str = Form(""),
    reason_preset: str = Form(""),
    return_to: str = Form("/#action-plan"),
):
    effective_reason = str(reason or "").strip() or str(reason_preset or "").strip()
    dismiss_action_plan_customer(customer, last_order, effective_reason)
    return RedirectResponse(url=return_to or "/#action-plan", status_code=303)


@app.post("/action-plan-restore")
def post_action_plan_restore(
    customer: str = Form(...),
    return_to: str = Form("/#action-plan"),
):
    restore_action_plan_customer(customer)
    return RedirectResponse(url=return_to or "/#action-plan", status_code=303)


def render_home_page(selected_customer="", view="focus", dismiss_customer=""):
    order_result = get_orders_for_analysis()
    crm_result = fetch_crm_activities()
    attention_result = build_customers_needing_attention_response(
        order_result=order_result,
        crm_result=crm_result,
    )

    if order_result["status"] != "ok":
        return render_page(
            title="Numat AI Sales Assistant",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(order_result['source'])}: "
                f"{escape(order_result['status'])}</p>"
            )
        )

    orders = order_result["orders"]
    attention_customers = attention_result["late_customers"]
    grouped_orders = group_by_customer(orders)
    action_plan = build_home_action_plan(
        attention_customers,
        grouped_orders,
        attention_result.get("dismissed_customers", []),
    )
    crm_activity_map = (
        crm_result.get("activity_map", {})
        if crm_result.get("status") == "ok" else {}
    )
    queue_summaries = build_home_queue_summaries(
        action_plan.get("due_today", []),
        grouped_orders,
        crm_activity_map,
    )
    selected_customer = get_home_selected_customer_name(
        queue_summaries,
        selected_customer=selected_customer,
    )
    selected_preview = build_home_preview_payload(
        selected_customer,
        queue_summaries,
        grouped_orders,
        attention_customers,
        crm_result,
    )
    home_view = "list" if str(view).strip().lower() == "list" else "focus"

    body = f"""
        {render_data_availability_banner(order_result, crm_result)}
        {render_home_workflow_layout(action_plan, queue_summaries, selected_preview, home_view, dismiss_customer=dismiss_customer)}
    """

    return render_page(
        title="Numat AI Sales Assistant",
        body=body,
        show_title=False,
    )


@app.get("/outreach-prep", response_class=HTMLResponse)
def get_outreach_prep(customer: str):
    order_result = get_orders_for_analysis()
    crm_result = fetch_crm_activities()

    if order_result["status"] != "ok":
        return render_page(
            title="Outreach Prep",
            body=(
                f"<p class='status error'>Could not load orders from "
                f"{escape(order_result['source'])}: {escape(order_result['status'])}</p>"
            ),
        )

    customer_orders = [
        order
        for order in order_result["orders"]
        if str(order.get("customer", "")).lower() == str(customer).lower()
    ]

    if not customer_orders:
        return render_page(
            title="Outreach Prep",
            body=f"<p class='status'>No orders found for {escape(customer)}.</p>",
        )

    attention = build_customers_needing_attention_response_map().get(customer)
    outreach_context = build_outreach_context(customer, customer_orders, attention, crm_result)
    outreach_result = generate_outreach_prep(outreach_context)
    body = render_outreach_prep_page(
        customer=customer,
        context=outreach_context,
        result=outreach_result,
        data_results=[order_result, crm_result],
    )
    return render_page(title=f"Outreach Prep: {customer}", body=body)


@app.get("/sample-data", response_class=HTMLResponse)
def get_sample_data_page():
    return render_sample_data_page()


@app.post("/sample-data", response_class=HTMLResponse)
async def upload_sample_data(file: UploadFile = File(...)):
    filename = file.filename or ""

    if not filename.lower().endswith(".csv"):
        return render_sample_data_page(
            upload_result={
                "valid": False,
                "status": "invalid_file_type",
                "warnings": [],
                "errors": ["Please upload a .csv file."],
            }
        )

    content = await file.read()
    validation = validate_sample_csv_content(content)

    if validation["valid"]:
        sample_path = get_sample_csv_path()
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        sample_path.write_bytes(content)
        validation["path"] = str(sample_path)
        validation["saved"] = True
    else:
        validation["saved"] = False

    return render_sample_data_page(upload_result=validation)


@app.get("/orders-view", response_class=HTMLResponse)
def get_orders_view(
    customer: str = "",
    status: str = "",
    sort: str = "order_date",
    direction: str = "desc",
):
    result = get_orders_for_analysis()
    orders = result["orders"]

    if result["status"] != "ok":
        return render_page(
            title="Orders",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(result['source'])}: {escape(result['status'])}</p>"
            )
        )

    filtered_orders = filter_orders(orders, customer=customer, status=status)
    sorted_orders = sort_orders(
        filtered_orders,
        sort_key=sort,
        direction=direction
    )

    rows = "".join(
        render_customer_order_row(order)
        for order in sorted_orders
    )

    if not rows:
        rows = (
            "<tr>"
            "<td colspan='9' class='empty'>No orders match the current filters.</td>"
            "</tr>"
        )

    body = f"""
        {render_data_availability_banner(result)}

        <div class="summary customer-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(result["source"])}</strong>
            </div>
            <div>
                <span class="label">Status</span>
                <strong>{escape(result["status"])}</strong>
            </div>
            <div>
                <span class="label">Orders</span>
                <strong>{len(orders)}</strong>
            </div>
            <div>
                <span class="label">Showing</span>
                <strong>{len(sorted_orders)}</strong>
            </div>
        </div>

        {render_orders_filter_form(customer, status, sort, direction)}

        <div class="table-wrap tall-table">
        <table>
            <thead>
                <tr>
                    <th>Record</th>
                    <th>Customer</th>
                    <th>Order Date</th>
                    <th>Amount</th>
                    <th>Order No</th>
                    <th>Status</th>
                    <th>Price List</th>
                    <th>State</th>
                    <th>Territory</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </div>
    """

    return render_page(title="Orders", body=body)


@app.get("/insights-view", response_class=HTMLResponse)
def get_insights_view():
    order_result = get_orders_for_analysis()

    if order_result["status"] != "ok":
        return render_page(
            title="Insights",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(order_result['source'])}: "
                f"{escape(order_result['status'])}</p>"
            )
        )

    orders = order_result["orders"]
    crm_result = fetch_crm_activities()
    attention_result = build_customers_needing_attention_response()
    attention_customers = (
        attention_result.get("late_customers", [])
        if attention_result["status"] == "ok"
        else []
    )
    grouped_orders = group_by_customer(orders)
    master_data_result = fetch_filemaker_master_data()
    master_customers = (
        master_data_result.get("customers_by_key", {})
        if master_data_result.get("status") == "ok"
        else {}
    )

    active_customer_count = 0
    lost_lapsed_count = 0
    prospect_count = 0

    for customer_record in master_customers.values():
        activity_status = str(customer_record.get("activity_status") or "").strip().lower()

        if activity_status == "active":
            active_customer_count += 1
        elif activity_status in {"l&l", "lost & lapsed", "lost and lapsed"}:
            lost_lapsed_count += 1
        elif activity_status == "prospect":
            prospect_count += 1

    sales_activities = []
    inbound_sales = []
    outbound_sales = []
    if crm_result.get("status") == "ok":
        for activity in crm_result.get("activities", []):
            if get_activity_category(activity) != "sales_outreach":
                continue

            sales_activities.append(activity)
            direction = str(activity.get("direction") or "").strip().lower()

            if direction == "inbound":
                inbound_sales.append(activity)
            elif direction == "outbound":
                outbound_sales.append(activity)

    approx_response_rate = (
        f"{round((len(inbound_sales) / len(outbound_sales)) * 100)}%"
        if outbound_sales else "n/a"
    )

    body = f"""
        {render_data_availability_banner(order_result, crm_result, attention_result)}

        <div class="dashboard-grid chart-grid insights-grid">
            <div class="insights-span-2">
                {render_monthly_orders_chart(orders)}
            </div>
            {render_attention_chart(attention_customers)}
            {render_monthly_sales_outreach_chart(sales_activities)}
            {render_sales_response_chart(outbound_sales, inbound_sales)}
            {render_sales_activity_type_chart(sales_activities)}
        </div>
    """

    return render_page(title="Insights", body=body)


@app.get("/late-customers")
def get_late_customers():
    return build_customers_needing_attention_response()


@app.get("/customers-needing-attention")
def get_customers_needing_attention():
    return build_customers_needing_attention_response()


@app.get("/late-customers-view", response_class=HTMLResponse)
def get_late_customers_view(
    customer: str = "",
    action: str = "",
    sort: str = "priority_score",
    direction: str = "desc",
):
    return get_customers_needing_attention_view(
        customer=customer,
        action=action,
        sort=sort,
        direction=direction
    )


@app.get("/customers-needing-attention-view", response_class=HTMLResponse)
def get_customers_needing_attention_view(
    customer: str = "",
    action: str = "",
    sort: str = "priority_score",
    direction: str = "desc",
):
    result = build_customers_needing_attention_response()
    late_customers = result["late_customers"]
    dismissed_customers = result.get("dismissed_customers", [])
    dismissed_customers = result.get("dismissed_customers", [])

    if result["status"] != "ok":
        return render_page(
            title="Customers Needing Attention",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(result['source'])}: {escape(result['status'])}</p>"
            )
        )

    filtered_late_customers = filter_late_customers(
        late_customers,
        customer=customer,
        action=action
    )
    sorted_late_customers = sort_late_customers(
        filtered_late_customers,
        sort_key=sort,
        direction=direction
    )

    rows = "".join(
        render_late_customer_row(customer)
        for customer in sorted_late_customers
    )

    if not rows:
        rows = (
            "<tr>"
            "<td colspan='7' class='empty'>No customers currently need attention.</td>"
            "</tr>"
        )

    body = f"""
        {render_data_availability_banner(result)}

        <div class="summary customer-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(result["source"])}</strong>
            </div>
            <div>
                <span class="label">Status</span>
                <strong>{escape(result["status"])}</strong>
            </div>
            <div>
                <span class="label">Need Attention</span>
                <strong>{len(late_customers)}</strong>
            </div>
            <div>
                <span class="label">Showing</span>
                <strong>{len(sorted_late_customers)}</strong>
            </div>
        </div>

        <section class="attention-top">
            {render_late_customers_filter_form(customer, action, sort, direction)}

            <section class="note compact-note">
                <h2>Priority Score Guide</h2>
                <p>
                    Score compares days since last order with the customer's
                    usual average gap.
                </p>
                <ul>
                    <li><strong>Over 2.0:</strong> urgent</li>
                    <li><strong>1.5 to 2.0:</strong> follow up soon</li>
                    <li><strong>1.25 to 1.5:</strong> consider follow-up</li>
                    <li><strong>1.0 to 1.25:</strong> watch</li>
                </ul>
            </section>
        </section>

        <div class="table-wrap tall-table">
        <table class="attention-table">
            <thead>
                <tr>
                    <th>Customer</th>
                    <th>Avg Gap</th>
                    <th>Days Since Last</th>
                    <th>Priority</th>
                    <th>Action</th>
                    <th>Last Activity</th>
                    <th>Explanation</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </div>

        {render_dismissed_action_plan_items(
            dismissed_customers,
            return_to="/customers-needing-attention-view#dismissed-urgency",
            section_id="dismissed-urgency",
            summary_label="Dismissed urgency items",
        )}
    """

    return render_page(title="Customers Needing Attention", body=body)


@app.get("/customers-view", response_class=HTMLResponse)
def get_customers_view(
    customer: str = "",
    state: str = "",
    sort: str = "last_order",
    direction: str = "desc",
):
    order_result = get_orders_for_analysis()

    if order_result["status"] != "ok":
        return render_page(
            title="Customers",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(order_result['source'])}: "
                f"{escape(order_result['status'])}</p>"
            )
        )

    attention_result = build_customers_needing_attention_response()
    attention_by_customer = {
        item["customer"]: item
        for item in attention_result["late_customers"]
    }
    crm_result = fetch_crm_activities()
    crm_activity_map = (
        crm_result.get("activity_map", {})
        if crm_result["status"] == "ok" else {}
    )
    summaries = build_customer_summaries(
        order_result["orders"],
        attention_by_customer,
        crm_activity_map,
    )
    filtered_summaries = filter_customer_summaries(
        summaries,
        customer=customer,
        state=state
    )
    sorted_summaries = sort_customer_summaries(
        filtered_summaries,
        sort_key=sort,
        direction=direction
    )
    rows = "".join(
        render_customer_summary_row(summary)
        for summary in sorted_summaries
    )

    if not rows:
        rows = (
            "<tr>"
            "<td colspan='9' class='empty'>No customers match the current filters.</td>"
            "</tr>"
        )

    body = f"""
        {render_data_availability_banner(order_result, crm_result)}

        <div class="summary customer-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(order_result["source"])}</strong>
            </div>
            <div>
                <span class="label">Customers</span>
                <strong>{len(summaries)}</strong>
            </div>
            <div>
                <span class="label">Showing</span>
                <strong>{len(sorted_summaries)}</strong>
            </div>
            <div>
                <span class="label">Need Attention</span>
                <strong>{len(attention_by_customer)}</strong>
            </div>
        </div>

        {render_customers_filter_form(customer, state, sort, direction)}

        <div class="table-wrap tall-table">
        <table class="customers-table">
            <thead>
                <tr>
                    <th>Customer</th>
                    <th>Orders</th>
                    <th>CRM</th>
                    <th>State</th>
                    <th>Last Order</th>
                    <th>Avg Cycle</th>
                    <th>Total Value</th>
                    <th>Avg Value / Order</th>
                    <th>Last Activity</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </div>
    """

    return render_page(title="Customers", body=body)


@app.get("/customer-view", response_class=HTMLResponse)
def get_customer_view(
    customer: str,
    sort: str = "order_date",
    direction: str = "desc",
    crm_limit: int = 12,
    crm_page: int = 1,
    crm_direction: str = "",
    crm_category: str = "",
):
    result = get_orders_for_analysis()
    crm_result = fetch_crm_activities()
    crm_limit = max(12, min(crm_limit, 120))
    crm_page = max(1, crm_page)

    if result["status"] != "ok":
        return render_page(
            title="Customer Orders",
            body=(
                f"<p class='status error'>Could not load data from "
                f"{escape(result['source'])}: {escape(result['status'])}</p>"
            )
        )

    customer_orders = [
        order
        for order in result["orders"]
        if str(order.get("customer", "")).lower() == customer.lower()
    ]
    sorted_orders = sort_orders(
        customer_orders,
        sort_key=sort,
        direction=direction
    )

    if not customer_orders:
        return render_page(
            title="Customer Orders",
            body=(
                f"<p class='status'>No orders found for "
                f"{escape(customer)}.</p>"
            )
        )

    rows = "".join(
        render_customer_order_row(order)
        for order in sorted_orders
    )
    total_value = sum_order_amounts(customer_orders)
    first_order = min(order["order_date"] for order in customer_orders)
    last_order = max(order["order_date"] for order in customer_orders)
    average_gap = calculate_average_gap(customer_orders)
    average_value = average_order_amount(customer_orders)
    last_activity = get_last_activity(customer_orders)
    last_activity_info = get_last_activity_info(customer_orders)
    customer_primary_key = get_customer_primary_key(customer_orders)
    crm_activities = (
        crm_result.get("activity_map", {}).get(customer_primary_key, [])
        if crm_result["status"] == "ok" else []
    )
    sales_crm_activities = get_sales_outreach_activities(crm_activities)
    filtered_crm_activities = filter_customer_crm_activities(
        crm_activities,
        direction=crm_direction,
        category=crm_category,
    )
    latest_crm_activity = crm_activities[0] if crm_activities else None
    latest_sales_crm_activity = sales_crm_activities[0] if sales_crm_activities else None
    latest_sales_crm_activity_date = parse_crm_datetime(
        latest_sales_crm_activity.get("date_created", "")
    ) if latest_sales_crm_activity else None
    display_last_activity = last_activity or latest_sales_crm_activity_date

    body = f"""
        {render_data_availability_banner(result, crm_result)}

        <div class="summary customer-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(result["source"])}</strong>
            </div>
            <div>
                <span class="label">Orders</span>
                <strong>{len(customer_orders)}</strong>
            </div>
            <div>
                <span class="label">First Order</span>
                <strong>{escape(first_order)}</strong>
            </div>
            <div>
                <span class="label">Last Order</span>
                <strong>{escape(last_order)}</strong>
            </div>
            <div>
                <span class="label">Average Cycle</span>
                <strong>{format_average_gap(average_gap)}</strong>
            </div>
            <div>
                <span class="label">Last Activity</span>
                <strong>{format_optional_date(display_last_activity)}</strong>
                {render_recent_activity_badge_for_date(display_last_activity)}
            </div>
            <div>
                <span class="label">CRM Emails</span>
                <strong>{len(crm_activities)}</strong>
            </div>
            <div>
                <span class="label">Latest CRM Email</span>
                <strong>{escape(format_optional_datetime(latest_crm_activity.get("date_created") if latest_crm_activity else ""))}</strong>
            </div>
            <div>
                <span class="label">Total Value</span>
                <strong>{format_currency(total_value)}</strong>
            </div>
            <div>
                <span class="label">Avg Value / Order</span>
                <strong>{format_currency(average_value)}</strong>
            </div>
        </div>

        {render_contact_ready_panel(
            customer=customer,
            attention=build_customers_needing_attention_response_map().get(customer),
            latest_crm_activity=latest_crm_activity,
            crm_activities=crm_activities,
            last_order=last_order,
            average_gap=average_gap,
            last_activity=last_activity,
        )}

        {render_customer_crm_timeline(
            customer,
            filtered_crm_activities,
            crm_limit,
            crm_page,
            crm_direction,
            crm_category,
            sort,
            direction,
        )}

        {render_customer_sort_form(customer, sort, direction, crm_limit, crm_page, crm_direction, crm_category)}

        <div class="table-wrap tall-table">
        <table>
            <thead>
                <tr>
                    <th>Record</th>
                    <th>Customer</th>
                    <th>Order Date</th>
                    <th>Amount</th>
                    <th>Order No</th>
                    <th>Status</th>
                    <th>Price List</th>
                    <th>State</th>
                    <th>Territory</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </div>
    """

    return render_page(title=f"Customer: {customer}", body=body)


def build_late_customers_response():
    return build_customers_needing_attention_response()


def should_add_ai_explanations():
    return os.getenv("ENABLE_AI_EXPLANATIONS", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def should_enable_full_crm_sync():
    return os.getenv("ENABLE_FULL_CRM_SYNC", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def get_attention_cache_seconds():
    raw_seconds = os.getenv("ATTENTION_CACHE_SECONDS", "60").strip()

    try:
        return max(0, int(raw_seconds))
    except ValueError:
        return 60


def build_attention_cache_key(order_result, crm_result):
    return (
        order_result.get("source"),
        order_result.get("status"),
        order_result.get("cache_updated_at", ""),
        len(order_result.get("orders", [])),
        crm_result.get("source"),
        crm_result.get("status"),
        crm_result.get("synced_at", ""),
        crm_result.get("cache_updated_at", ""),
        crm_result.get("counts", {}).get("kept_rows", 0),
        should_add_ai_explanations(),
    )


def should_enable_startup_prewarm():
    return os.getenv("ENABLE_STARTUP_PREWARM", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def prewarm_app_caches():
    try:
        order_result = get_orders_for_analysis()
        crm_result = fetch_crm_activities()
        fetch_filemaker_master_data()

        if order_result.get("status") == "ok":
            build_customers_needing_attention_response(
                order_result=order_result,
                crm_result=crm_result,
            )
    except Exception as error:
        print(f"Startup prewarm skipped after error: {error.__class__.__name__}")


def build_attention_explanation(customer):
    explanation = str(customer.get("explanation", "") or "").strip()

    if explanation:
        return explanation

    return (
        f"{customer.get('customer', 'This customer')} needs attention because it has been "
        f"{customer.get('days_since_last', '0')} days since the last order, compared with "
        f"the usual average gap of {customer.get('avg_gap', '0')} days. "
        f"Recommended action: {customer.get('action', 'Review account')}."
    )


def build_customers_needing_attention_response(order_result=None, crm_result=None):
    order_result = order_result or get_orders_for_analysis()

    if order_result["status"] != "ok":
        return {
            "source": order_result["source"],
            "status": order_result["status"],
            "late_customers": []
        }

    crm_result = crm_result or fetch_crm_activities()
    cache_seconds = get_attention_cache_seconds()
    cache_key = build_attention_cache_key(order_result, crm_result)
    now = time.time()

    if (
        cache_seconds
        and _ATTENTION_RESPONSE_CACHE["key"] == cache_key
        and _ATTENTION_RESPONSE_CACHE["result"] is not None
        and _ATTENTION_RESPONSE_CACHE["expires_at"] > now
    ):
        return _ATTENTION_RESPONSE_CACHE["result"]

    customers = group_by_customer(order_result["orders"])
    crm_activity_map = (
        crm_result.get("activity_map", {})
        if crm_result["status"] == "ok" else {}
    )
    late = find_late_customers(customers)
    dismissals = read_action_plan_dismissals()
    dismissed_customers = []
    active_customers = []

    for customer in late:
        customer_orders = customers.get(customer["customer"], [])
        customer["last_order"] = (
            max(order.get("order_date", "") for order in customer_orders)
            if customer_orders else ""
        )
        customer_primary_key = get_customer_primary_key(customer_orders)
        crm_matches = crm_activity_map.get(customer_primary_key, [])
        sales_crm_matches = get_sales_outreach_activities(crm_matches)
        latest_crm_activity = crm_matches[0] if crm_matches else None
        latest_sales_crm_activity = sales_crm_matches[0] if sales_crm_matches else None
        crm_recent_days = get_crm_days_since_latest_activity(latest_sales_crm_activity)
        display_last_contact, _last_contact_source = build_last_contact_display(
            customer.get("last_activity_content", ""),
            latest_crm_activity,
            crm_activities=crm_matches,
        )
        customer["display_last_contact"] = display_last_contact
        if customer.get("days_since_last_activity") is None and crm_recent_days is not None:
            customer["days_since_last_activity"] = crm_recent_days
            customer["last_activity_date"] = (
                latest_sales_crm_activity.get("date_created", "")[:10]
                if latest_sales_crm_activity else None
            )

        dismissal = get_action_plan_dismissal(
            customer.get("customer", ""),
            customer.get("last_order", ""),
            dismissals=dismissals,
        )
        if dismissal:
            customer["dismiss_reason"] = dismissal.get("reason", "")
            customer["dismissed_at"] = dismissal.get("dismissed_at", "")
            dismissed_customers.append(customer)
            continue

        active_customers.append(customer)

    late_with_explanations = (
        add_ai_explanations(active_customers)
        if should_add_ai_explanations()
        else active_customers
    )
    result = {
        "source": order_result["source"],
        "status": "ok",
        "late_customers": late_with_explanations,
        "dismissed_customers": sort_late_customers(
            dismissed_customers,
            sort_key="priority_score",
            direction="desc",
        ),
    }

    if cache_seconds:
        _ATTENTION_RESPONSE_CACHE["key"] = cache_key
        _ATTENTION_RESPONSE_CACHE["expires_at"] = now + cache_seconds
        _ATTENTION_RESPONSE_CACHE["result"] = result

    return result


def render_late_customer_row(customer):
    customer_name = str(customer["customer"])
    customer_url = f"/customer-view?customer={quote(customer_name)}"
    dismiss_reason = str(customer.get("dismiss_reason") or "").strip()

    return f"""
        <tr>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td>{escape(str(customer["avg_gap"]))} days</td>
            <td>{escape(str(customer["days_since_last"]))} days</td>
            <td><span class="score">{escape(str(customer["priority_score"]))}</span></td>
            <td>
                <div class="attention-action-cell">
                    <span>{escape(str(customer["action"]))}</span>
                    <form method="post" action="/action-plan-dismiss" class="attention-dismiss-form">
                        <input type="hidden" name="customer" value="{escape(customer_name)}">
                        <input type="hidden" name="last_order" value="{escape(str(customer.get('last_order', '')))}">
                        <input type="hidden" name="return_to" value="/customers-needing-attention-view#dismissed-urgency">
                        <button type="submit" class="button secondary small-button action-dismiss-button">Dismiss</button>
                        <input
                            type="text"
                            name="reason"
                            class="dismiss-reason-input"
                            placeholder="Reason for dismissing urgency"
                            value="{escape(dismiss_reason)}"
                        >
                    </form>
                </div>
            </td>
            <td>
                <div class="activity-cell">
                    <span class="activity-summary">{escape(format_activity_summary(customer))}</span>
                    {render_recent_activity_badge(customer)}
                    {render_activity_content_note(customer.get("display_last_contact", ""))}
                </div>
            </td>
            <td>{escape(build_attention_explanation(customer))}</td>
        </tr>
    """


def render_order_row(order):
    extra = order.get("extra", {})
    customer_name = str(order.get("customer", ""))
    customer_url = f"/customer-view?customer={quote(customer_name)}"

    return f"""
        <tr>
            <td>{escape(str(order.get("filemaker_record_id", "")))}</td>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td>{escape(str(order.get("order_date", "")))}</td>
            <td>{escape(str(order.get("amount", "")))}</td>
            <td>{escape(str(extra.get("Orders::Order No", "")))}</td>
            <td>{escape(str(extra.get("Orders::Status", "")))}</td>
            <td>{escape(str(get_order_price_list(order)))}</td>
            <td>{escape(str(get_order_state(order)))}</td>
            <td>{escape(str(get_order_territory(order)))}</td>
        </tr>
    """


def render_customer_order_row(order):
    extra = order.get("extra", {})
    customer_name = str(order.get("customer", ""))
    customer_url = f"/customer-view?customer={quote(customer_name)}"

    return f"""
        <tr>
            <td>{escape(str(order.get("filemaker_record_id", "")))}</td>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td>{escape(str(order.get("order_date", "")))}</td>
            <td>{escape(str(order.get("amount", "")))}</td>
            <td>{escape(str(extra.get("Orders::Order No", "")))}</td>
            <td>{escape(str(extra.get("Orders::Status", "")))}</td>
            <td>{escape(str(get_order_price_list(order)))}</td>
            <td>{escape(str(get_order_state(order)))}</td>
            <td>{escape(str(get_order_territory(order)))}</td>
        </tr>
    """


def build_customer_summaries(orders, attention_by_customer, crm_activity_map=None):
    grouped_customers = group_by_customer(orders)
    crm_activity_map = crm_activity_map or {}
    summaries = []

    for customer_name, customer_orders in grouped_customers.items():
        state_counts = Counter(
            get_order_state(order)
            for order in customer_orders
            if get_order_state(order)
        )
        last_activity = get_last_activity(customer_orders)
        attention = attention_by_customer.get(customer_name)
        customer_primary_key = get_customer_primary_key(customer_orders)
        crm_activities = crm_activity_map.get(customer_primary_key, [])
        sales_crm_activities = get_sales_outreach_activities(crm_activities)
        latest_sales_crm_activity = sales_crm_activities[0] if sales_crm_activities else None
        latest_sales_crm_activity_date = parse_crm_datetime(
            latest_sales_crm_activity.get("date_created", "")
        ) if latest_sales_crm_activity else None
        display_last_activity = last_activity or latest_sales_crm_activity_date

        summaries.append({
            "customer": customer_name,
            "order_count": len(customer_orders),
            "crm_count": len(crm_activities),
            "state": state_counts.most_common(1)[0][0] if state_counts else "",
            "last_order": max(order["order_date"] for order in customer_orders),
            "avg_cycle": calculate_average_gap(customer_orders),
            "total_value": sum_order_amounts(customer_orders),
            "avg_value": average_order_amount(customer_orders),
            "last_activity": last_activity,
            "display_last_activity": display_last_activity,
            "latest_crm_activity": (
                latest_sales_crm_activity.get("date_created", "")
                if latest_sales_crm_activity else ""
            ),
            "attention": attention,
        })

    return summaries


def render_customer_summary_row(summary):
    customer_name = str(summary["customer"])
    customer_url = f"/customer-view?customer={quote(customer_name)}"

    return f"""
        <tr>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td>{summary["order_count"]}</td>
            <td>{summary["crm_count"]}</td>
            <td>{escape(str(summary["state"]))}</td>
            <td>{escape(str(summary["last_order"]))}</td>
            <td>{format_average_gap(summary["avg_cycle"])}</td>
            <td>{format_currency(summary["total_value"])}</td>
            <td>{format_currency(summary["avg_value"])}</td>
            <td>
                <div class="customers-last-activity">
                    <span>{format_optional_date(summary.get("display_last_activity"))}</span>
                    {render_recent_activity_badge_for_date(summary.get("display_last_activity"))}
                </div>
            </td>
        </tr>
    """


def render_customer_crm_timeline(customer, activities, crm_limit, crm_page, crm_direction, crm_category, sort, direction):
    total_pages = max(1, (len(activities) + crm_limit - 1) // crm_limit)
    crm_page = max(1, min(crm_page, total_pages))
    start_index = (crm_page - 1) * crm_limit
    shown_activities = activities[start_index:start_index + crm_limit]
    shown_count = len(shown_activities)
    summary_html = render_customer_crm_timeline_summary(
        activities=activities,
        crm_direction=crm_direction,
        crm_category=crm_category,
        start_index=start_index,
        shown_count=shown_count,
    )
    rows = "".join(
        render_customer_crm_activity(activity)
        for activity in shown_activities
    )

    if not rows:
        rows = (
            "<tr>"
            "<td colspan='5' class='empty'>No CRM email history linked to this customer key yet.</td>"
            "</tr>"
        )

    pagination_html = render_customer_crm_pagination(
        customer=customer,
        crm_page=crm_page,
        total_pages=total_pages,
        crm_limit=crm_limit,
        crm_direction=crm_direction,
        crm_category=crm_category,
        sort=sort,
        direction=direction,
    )

    return f"""
        <section class="panel" id="crm-timeline">
            <h2>CRM Timeline</h2>
            {summary_html}
            {render_customer_crm_timeline_controls(customer, crm_direction, crm_category, crm_limit, sort, direction)}
            <div class="table-wrap tall-table crm-timeline-wrap">
            <table class="crm-timeline-table">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Direction</th>
                        <th>Subject</th>
                        <th>From / To</th>
                        <th>Preview</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            </div>
            {pagination_html}
        </section>
    """


def render_customer_crm_timeline_summary(activities, crm_direction, crm_category, start_index, shown_count):
    parts = [
        (
            f"Showing {start_index + 1 if activities else 0} to "
            f"{start_index + shown_count} of {len(activities)} matched customer emails"
        )
    ]
    direction_value = str(crm_direction or "").strip().lower()

    if direction_value:
        parts.append(f"direction: {direction_value.title()}")
    else:
        direction_counts = Counter(
            str(activity.get("direction") or "").strip().lower()
            for activity in activities
            if str(activity.get("direction") or "").strip()
        )
        inbound_count = direction_counts.get("inbound", 0)
        outbound_count = direction_counts.get("outbound", 0)
        unknown_count = direction_counts.get("unknown", 0)
        parts.append(f"inbound: {inbound_count}")
        parts.append(f"outbound: {outbound_count}")

        if unknown_count:
            parts.append(f"unknown: {unknown_count}")

    category_value = str(crm_category or "").strip().lower()
    if category_value:
        parts.append(f"category: {category_value.replace('_', ' ').title()}")

    return (
        "<p class=\"filter-summary\">"
        + " | ".join(escape(part) for part in parts)
        + "</p>"
    )


def render_customer_crm_timeline_controls(customer, crm_direction, crm_category, crm_limit, sort, direction):
    direction_options = {
        "": "All",
        "inbound": "Inbound",
        "outbound": "Outbound",
        "unknown": "Unknown",
    }
    category_options = {
        "": "All",
        "sales_outreach": "Sales Outreach",
        "customer_services": "Customer Services",
    }
    limit_options = {
        "12": "12 rows",
        "24": "24 rows",
        "48": "48 rows",
    }

    return f"""
        <form class="controls compact crm-timeline-controls" method="get" action="/customer-view#crm-timeline">
            <input type="hidden" name="customer" value="{escape(customer)}">
            <input type="hidden" name="sort" value="{escape(sort)}">
            <input type="hidden" name="direction" value="{escape(direction)}">
            <input type="hidden" name="crm_page" value="1">

            <label>
                <span>CRM Direction</span>
                <select name="crm_direction">
                    {render_select_options(direction_options, crm_direction)}
                </select>
            </label>

            <label>
                <span>CRM Category</span>
                <select name="crm_category">
                    {render_select_options(category_options, crm_category)}
                </select>
            </label>

            <label>
                <span>Rows</span>
                <select name="crm_limit">
                    {render_select_options(limit_options, str(crm_limit))}
                </select>
            </label>

            <button type="submit">Apply</button>
        </form>
    """


def render_customer_crm_pagination(customer, crm_page, total_pages, crm_limit, crm_direction, crm_category, sort, direction):
    if total_pages <= 1:
        return ""

    previous_link = ""
    next_link = ""

    base_query = (
        f"customer={quote(customer)}&sort={quote(sort)}&direction={quote(direction)}"
        f"&crm_limit={crm_limit}&crm_direction={quote(crm_direction)}"
        f"&crm_category={quote(crm_category)}"
    )

    if crm_page > 1:
        previous_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/customer-view?{base_query}&crm_page={crm_page - 1}#crm-timeline">Previous</a>'
        )

    if crm_page < total_pages:
        next_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/customer-view?{base_query}&crm_page={crm_page + 1}#crm-timeline">Next</a>'
        )

    return f"""
        <div class="pager">
            <span class="muted">Page {crm_page} of {total_pages}</span>
            <div class="pager-actions">
                {previous_link}
                {next_link}
            </div>
        </div>
    """


def render_customer_crm_activity(activity):
    direction_class = "crm-direction"

    if activity.get("direction") == "inbound":
        direction_class = "crm-direction inbound"
    elif activity.get("direction") == "outbound":
        direction_class = "crm-direction outbound"

    category_value = get_activity_category(activity)
    category_label = (
        "Customer Services"
        if category_value == "customer_services"
        else "Sales Outreach"
    )
    category_class = f"crm-category {category_value.replace('_', '-')}"

    full_text = clean_activity_content(activity.get("body", ""))
    preview = truncate_text(full_text, 260)

    return f"""
        <tr>
            <td>{escape(format_optional_datetime(activity.get("date_created", "")))}</td>
            <td>
                <div class="crm-pill-stack">
                    <span class="{category_class}">{escape(category_label)}</span>
                    <span class="{direction_class}">{escape(activity.get("direction", "unknown"))}</span>
                </div>
            </td>
            <td>{escape(activity.get("subject", ""))}</td>
            <td>
                <div><strong>From:</strong> {escape(activity.get("sender_email", ""))}</div>
                <div><strong>To:</strong> {escape(activity.get("to", ""))}</div>
            </td>
            <td>{render_expandable_activity(preview, full_text)}</td>
        </tr>
    """


def render_expandable_activity(preview, full_text):
    if not full_text:
        return ""

    if len(full_text) <= len(preview):
        return f"<span class=\"activity-note\">{escape(full_text)}</span>"

    return f"""
        <details class="activity-expand">
            <summary>{escape(preview)}</summary>
            <div class="activity-note">{escape(full_text)}</div>
        </details>
    """


def render_contact_ready_panel(
    customer,
    attention,
    latest_crm_activity,
    crm_activities,
    last_order,
    average_gap,
    last_activity,
):
    recommendation = build_contact_recommendation(
        customer=customer,
        attention=attention,
        latest_crm_activity=latest_crm_activity,
        crm_activities=crm_activities,
        last_order=last_order,
        average_gap=average_gap,
        last_activity=last_activity,
    )

    return f"""
        <section class="panel contact-panel">
            <h2>Contact Ready</h2>
            <p class="muted">A quick read on whether this customer looks ready for follow-up and what context a rep should keep in mind.</p>
            <div class="summary compact-summary contact-summary">
                <div>
                    <span class="label">Current Priority</span>
                    <strong>{escape(recommendation["priority_label"])}</strong>
                </div>
                <div>
                    <span class="label">Suggested Next Step</span>
                    <strong>{escape(recommendation["next_step"])}</strong>
                </div>
                <div>
                    <span class="label">CRM History</span>
                    <strong>{escape(recommendation["crm_summary"])}</strong>
                </div>
            </div>
            <p class="contact-guidance">{escape(recommendation["guidance"])}</p>
            <p class="contact-guidance subtle">{escape(recommendation["draft_prompt"])}</p>
        </section>
    """


def build_contact_recommendation(
    customer,
    attention,
    latest_crm_activity,
    crm_activities,
    last_order,
    average_gap,
    last_activity,
):
    priority_label = "Monitor"
    next_step = "Review account before contacting"
    guidance = "No special contact cue yet."

    if attention:
        priority_label = str(attention.get("action") or "Needs attention")

    if latest_crm_activity:
        crm_summary = (
            f"{len(crm_activities)} email(s), latest on "
            f"{format_optional_datetime(latest_crm_activity.get('date_created', ''))}"
        )
    else:
        crm_summary = "No linked CRM emails yet"

    if attention and has_recent_activity(attention.get("days_since_last_activity")):
        next_step = "Check the recent contact before sending another message"
        guidance = (
            "The customer already shows recent activity, so the safest move is to"
            " read the latest exchange first and avoid duplicate outreach."
        )
    elif attention and float(attention.get("priority_score") or 0) >= 2:
        next_step = "Reach out now with a specific operational reason"
        guidance = (
            "This account is well beyond its normal order cycle. Lead with a short,"
            " concrete reason for contact and reference the most recent CRM thread."
        )
    elif attention:
        next_step = "Follow up with a light check-in"
        guidance = (
            "This customer is beyond its average cycle, but not deeply overdue."
            " A short message referencing the last conversation should be enough."
        )
    elif latest_crm_activity:
        next_step = "Use the CRM thread for context before any outreach"
        guidance = (
            "The order pattern is not currently urgent, but there is useful email"
            " history you can use to match tone and avoid repeating questions."
        )

    draft_prompt = (
        f"Draft angle for {customer}: mention the last order on {last_order}, note the usual cycle"
        f" of {round(average_gap, 1) if average_gap else 'n/a'} days, and tie the"
        f" message back to the latest CRM conversation."
    )

    if last_activity:
        draft_prompt += (
            f" Internal last activity was recorded on"
            f" {format_optional_date(last_activity)}."
        )

    return {
        "priority_label": priority_label,
        "next_step": next_step,
        "crm_summary": crm_summary,
        "guidance": guidance,
        "draft_prompt": draft_prompt,
    }


def render_crm_filter_form(customer, direction, category, subject, page_size, date_from, date_to, range_key):
    direction_options = {
        "": "All",
        "inbound": "Inbound",
        "outbound": "Outbound",
        "unknown": "Unknown",
    }
    category_options = {
        "": "All",
        "sales_outreach": "Sales Outreach",
        "customer_services": "Customer Services",
    }
    page_size_options = {
        "50": "50 rows",
        "100": "100 rows",
        "200": "200 rows",
        "250": "250 rows",
    }
    range_options = {
        "90d": "Last 90 days",
        "all": "All dates",
    }

    effective_date_from, effective_date_to = get_crm_effective_date_range(
        date_from,
        date_to,
        range_key,
    )
    recent_link = (
        f'/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}'
        f'&category={quote(category)}&subject={quote(subject)}'
        f'&range_key=90d&page_size={page_size}'
    )
    all_link = (
        f'/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}'
        f'&category={quote(category)}&subject={quote(subject)}'
        f'&range_key=all&page_size={page_size}'
    )
    recent_class = "toggle-chip"
    all_class = "toggle-chip"

    if str(range_key or "").strip().lower() == "90d":
        recent_class += " active"
    else:
        all_class += " active"

    return f"""
        <form class="controls crm-filter-grid" method="get" action="/crm-activities-view">
            <div class="crm-filter-toggle">
                <span class="toggle-label">Quick Range</span>
                <div class="toggle-chips">
                    <a class="{recent_class}" href="{recent_link}">Recent</a>
                    <a class="{all_class}" href="{all_link}">All history</a>
                </div>
            </div>

            <label class="crm-field customer">
                <span>Customer</span>
                <input
                    type="search"
                    name="customer"
                    value="{escape(customer)}"
                    placeholder="Search company"
                >
            </label>

            <label class="crm-field direction">
                <span>Direction</span>
                <select name="direction">
                    {render_select_options(direction_options, direction)}
                </select>
            </label>

            <label class="crm-field category">
                <span>Category</span>
                <select name="category">
                    {render_select_options(category_options, category)}
                </select>
            </label>

            <label class="crm-field subject">
                <span>Subject</span>
                <input
                    type="search"
                    name="subject"
                    value="{escape(subject)}"
                    placeholder="Search subject"
                >
            </label>

            <label class="crm-field rows">
                <span>Rows</span>
                <select name="page_size">
                    {render_select_options(page_size_options, str(page_size))}
                </select>
            </label>

            <label class="crm-field range">
                <span>Range</span>
                <select name="range_key">
                    {render_select_options(range_options, range_key)}
                </select>
            </label>

            <label class="crm-field date-from">
                <span>Date From</span>
                <input type="date" name="date_from" value="{escape(effective_date_from)}">
            </label>

            <label class="crm-field date-to">
                <span>Date To</span>
                <input type="date" name="date_to" value="{escape(effective_date_to)}">
            </label>

            <input type="hidden" name="page" value="1">

            <div class="crm-filter-actions">
                <button class="crm-filter-action apply" type="submit">Apply</button>
                <a class="button secondary crm-filter-action reset" href="/crm-activities-view">Reset</a>
            </div>
        </form>
    """


def render_crm_filter_summary(total_filtered, customer="", direction="", category="", subject="", date_from="", date_to="", range_key="all"):
    parts = [f"{total_filtered:,} matches"]
    effective_date_from, effective_date_to = get_crm_effective_date_range(
        date_from,
        date_to,
        range_key,
    )

    if customer.strip():
        parts.append(f"customer: {customer.strip()}")

    if direction.strip():
        parts.append(f"direction: {direction.strip().title()}")

    if category.strip():
        parts.append(f"category: {category.strip().replace('_', ' ').title()}")

    if subject.strip():
        parts.append(f"subject: {subject.strip()}")

    if str(range_key or "").strip().lower() == "90d" and not date_from.strip() and not date_to.strip():
        parts.append("range: last 90 days")
    elif effective_date_from or effective_date_to:
        range_text = "range: "
        if effective_date_from:
            range_text += effective_date_from
        else:
            range_text += "..."

        range_text += " to "

        if effective_date_to:
            range_text += effective_date_to
        else:
            range_text += "..."

        parts.append(range_text)

    return (
        "<p class=\"filter-summary\">"
        + " | ".join(escape(part) for part in parts)
        + "</p>"
    )


def render_crm_pagination(customer, direction, category, subject, date_from, date_to, range_key, page, total_pages, page_size, total_filtered):
    if total_filtered <= page_size:
        return ""

    previous_link = ""
    next_link = ""

    if page > 1:
        previous_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}&category={quote(category)}&subject={quote(subject)}&date_from={quote(date_from)}&date_to={quote(date_to)}&range_key={quote(range_key)}&page={page - 1}&page_size={page_size}">Previous</a>'
        )

    if page < total_pages:
        next_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}&category={quote(category)}&subject={quote(subject)}&date_from={quote(date_from)}&date_to={quote(date_to)}&range_key={quote(range_key)}&page={page + 1}&page_size={page_size}">Next</a>'
        )

    return f"""
        <div class="pager">
            <span class="muted">Page {page} of {total_pages}</span>
            <div class="pager-actions">
                {previous_link}
                {next_link}
            </div>
        </div>
    """


def render_crm_activity_row(activity):
    preview = clean_activity_content(activity.get("body", ""))
    preview = truncate_text(preview, 320)
    direction_class = "crm-direction"

    if activity.get("direction") == "inbound":
        direction_class = "crm-direction inbound"
    elif activity.get("direction") == "outbound":
        direction_class = "crm-direction outbound"

    return f"""
        <tr>
            <td>
                <div class="crm-activity-meta">
                    <div><strong>Date:</strong> {escape(activity.get("date_created", ""))}</div>
                    <div><strong>Direction:</strong> <span class="{direction_class}">{escape(activity.get("direction", ""))}</span></div>
                    <div><strong>Customer:</strong> {escape(activity.get("customer_company") or activity.get("customer_label") or "")}</div>
                    <div><strong>Subject:</strong> {escape(activity.get("subject", ""))}</div>
                    <div><strong>From:</strong> {escape(activity.get("sender_email", ""))}</div>
                    <div><strong>To:</strong> {escape(activity.get("to", ""))}</div>
                </div>
            </td>
            <td><span class="activity-note">{escape(preview)}</span></td>
        </tr>
    """


def filter_crm_activities(activities, customer="", direction="", category="", subject="", date_from="", date_to="", range_key="all"):
    customer_filter = customer.strip().lower()
    direction_filter = direction.strip().lower()
    category_filter = category.strip().lower()
    subject_filter = subject.strip().lower()
    effective_date_from, effective_date_to = get_crm_effective_date_range(
        date_from,
        date_to,
        range_key,
    )
    date_from_value = parse_iso_date(effective_date_from)
    date_to_value = parse_iso_date(effective_date_to)
    filtered_activities = []

    for activity in activities:
        customer_value = str(
            activity.get("customer_company")
            or activity.get("customer_label")
            or ""
        ).lower()
        subject_value = str(activity.get("subject") or "").lower()
        direction_value = str(activity.get("direction") or "").lower()
        category_value = get_activity_category(activity)

        if customer_filter and customer_filter not in customer_value:
            continue

        if direction_filter and direction_filter != direction_value:
            continue

        if category_filter and category_filter != category_value:
            continue

        if subject_filter and subject_filter not in subject_value:
            continue

        activity_date = parse_crm_datetime(activity.get("date_created", ""))

        if date_from_value and activity_date and activity_date.date() < date_from_value.date():
            continue

        if date_to_value and activity_date and activity_date.date() > date_to_value.date():
            continue

        filtered_activities.append(activity)

    return filtered_activities


def get_crm_effective_date_range(date_from, date_to, range_key):
    raw_from = str(date_from or "").strip()
    raw_to = str(date_to or "").strip()
    range_value = str(range_key or "all").strip().lower()

    if range_value == "all":
        return raw_from, raw_to

    if raw_from or raw_to:
        return raw_from, raw_to

    today = get_analysis_today().date()
    default_from = today - timedelta(days=90)

    return default_from.isoformat(), today.isoformat()


def parse_crm_datetime(value):
    if not value:
        return None

    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def filter_customer_crm_activities(activities, direction="", category=""):
    direction_filter = str(direction or "").strip().lower()
    category_filter = str(category or "").strip().lower()

    filtered = []

    for activity in activities:
        if direction_filter and str(activity.get("direction") or "").strip().lower() != direction_filter:
            continue

        if category_filter and get_activity_category(activity) != category_filter:
            continue

        filtered.append(activity)

    return filtered


def build_crm_activity_map(activities):
    activity_map = defaultdict(list)

    for activity in activities:
        key = str(activity.get("customer_primary_key") or "").strip()

        if not key:
            continue

        activity_map[key].append(activity)

    for key, items in activity_map.items():
        activity_map[key] = sorted(
            items,
            key=lambda activity: activity.get("date_created", ""),
            reverse=True,
        )

    return activity_map


def build_customers_needing_attention_response_map():
    response = build_customers_needing_attention_response()

    if response["status"] != "ok":
        return {}

    return {
        item["customer"]: item
        for item in response["late_customers"]
    }


def get_customer_primary_key(customer_orders):
    for order in customer_orders:
        key = str(get_order_primary_key(order) or "").strip()

        if key:
            return key

    return ""


def get_customer_crm_activities(activities, customer_primary_key):
    if not customer_primary_key:
        return []

    matching_activities = [
        activity
        for activity in activities
        if str(activity.get("customer_primary_key") or "").strip()
        == customer_primary_key
    ]

    return sorted(
        matching_activities,
        key=lambda activity: activity.get("date_created", ""),
        reverse=True,
    )


def filter_customer_summaries(summaries, customer="", state=""):
    customer_filter = customer.strip().lower()
    state_filter = state.strip().lower()
    filtered_summaries = []

    for summary in summaries:
        if (
            customer_filter
            and customer_filter not in str(summary["customer"]).lower()
        ):
            continue

        if state_filter and state_filter != str(summary["state"]).lower():
            continue

        filtered_summaries.append(summary)

    return filtered_summaries


def sort_customer_summaries(summaries, sort_key="last_order", direction="desc"):
    valid_sort_keys = {
        "customer",
        "order_count",
        "crm_count",
        "state",
        "last_order",
        "avg_cycle",
        "total_value",
        "avg_value",
        "last_activity",
        "attention",
    }

    if sort_key not in valid_sort_keys:
        sort_key = "last_order"

    reverse = direction != "asc"

    return sorted(
        summaries,
        key=lambda summary: get_customer_summary_sort_value(summary, sort_key),
        reverse=reverse
    )


def get_customer_summary_sort_value(summary, sort_key):
    if sort_key in {"order_count", "crm_count", "total_value", "avg_value"}:
        return float(summary.get(sort_key) or 0)

    if sort_key == "avg_cycle":
        return float(summary.get("avg_cycle") or 0)

    if sort_key == "last_activity":
        activity = summary.get("display_last_activity") or summary.get("last_activity")
        return activity.strftime("%Y-%m-%d") if activity else ""

    if sort_key == "attention":
        attention = summary.get("attention")
        return float(attention.get("priority_score", 0)) if attention else 0

    return str(summary.get(sort_key, "")).lower()


def filter_orders(orders, customer="", status=""):
    customer_filter = customer.strip().lower()
    status_filter = status.strip().lower()

    filtered_orders = []

    for order in orders:
        extra = order.get("extra", {})
        order_customer = str(order.get("customer", "")).lower()
        order_status = str(extra.get("Orders::Status", "")).lower()

        if customer_filter and customer_filter not in order_customer:
            continue

        if status_filter and status_filter != order_status:
            continue

        filtered_orders.append(order)

    return filtered_orders


def filter_late_customers(customers, customer="", action=""):
    customer_filter = customer.strip().lower()
    action_filter = action.strip().lower()
    filtered_customers = []

    for late_customer in customers:
        customer_name = str(late_customer.get("customer", "")).lower()
        customer_action = str(late_customer.get("action", "")).lower()

        if customer_filter and customer_filter not in customer_name:
            continue

        if action_filter and action_filter not in customer_action:
            continue

        filtered_customers.append(late_customer)

    return filtered_customers


def sort_orders(orders, sort_key="order_date", direction="desc"):
    valid_sort_keys = {
        "customer",
        "order_date",
        "amount",
        "order_no",
        "status",
        "state",
        "territory",
        "last_activity",
    }

    if sort_key not in valid_sort_keys:
        sort_key = "order_date"

    reverse = direction != "asc"

    return sorted(
        orders,
        key=lambda order: get_order_sort_value(order, sort_key),
        reverse=reverse
    )


def get_order_sort_value(order, sort_key):
    extra = order.get("extra", {})

    if sort_key == "amount":
        try:
            return float(order.get("amount") or 0)
        except ValueError:
            return 0

    if sort_key == "order_no":
        try:
            return int(extra.get("Orders::Order No") or 0)
        except ValueError:
            return 0

    if sort_key == "status":
        return str(extra.get("Orders::Status", "")).lower()

    if sort_key == "state":
        return str(get_order_state(order)).lower()

    if sort_key == "territory":
        return str(get_order_territory(order)).lower()

    return str(order.get(sort_key, "")).lower()


def sort_late_customers(customers, sort_key="priority_score", direction="desc"):
    valid_sort_keys = {
        "customer",
        "avg_gap",
        "days_since_last",
        "priority_score",
        "action",
        "days_since_last_activity",
    }

    if sort_key not in valid_sort_keys:
        sort_key = "priority_score"

    reverse = direction != "asc"

    return sorted(
        customers,
        key=lambda customer: get_late_customer_sort_value(customer, sort_key),
        reverse=reverse
    )


def get_late_customer_sort_value(customer, sort_key):
    if sort_key in {
        "avg_gap",
        "days_since_last",
        "priority_score",
        "days_since_last_activity",
    }:
        try:
            return float(customer.get(sort_key) or 0)
        except ValueError:
            return 0

    return str(customer.get(sort_key, "")).lower()


def sum_order_amounts(orders):
    total = 0

    for order in orders:
        try:
            total += float(order.get("amount") or 0)
        except ValueError:
            continue

    return total


def average_order_amount(orders):
    if not orders:
        return 0

    total = 0

    for order in orders:
        try:
            total += float(order.get("amount") or 0)
        except ValueError:
            continue

    return total / len(orders)


def format_currency(value):
    return f"${value:,.2f}"


def truncate_text(value, limit=240):
    value = str(value or "").strip()

    if len(value) <= limit:
        return value

    return f"{value[:limit - 1].rstrip()}…"


def format_average_gap(value):
    if value is None:
        return "Not enough orders"

    return f"{round(value, 1)} days"


def parse_display_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()

    for pattern in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue

    return None


def format_optional_date(value):
    if not value:
        return "No recent activity"

    parsed_value = parse_display_datetime(value)

    if parsed_value:
        return parsed_value.strftime("%m/%d/%Y")

    return str(value)


def format_optional_datetime(value):
    if not value:
        return "Not available"

    parsed_value = parse_display_datetime(value)

    if parsed_value:
        return parsed_value.strftime("%m/%d/%Y %H:%M")

    return str(value)


def format_activity_summary(customer):
    activity_date = customer.get("last_activity_date")
    days_since_activity = customer.get("days_since_last_activity")

    if not activity_date:
        return "No recent activity"

    if days_since_activity is None:
        return format_optional_date(activity_date)

    return f"{format_optional_date(activity_date)} ({days_since_activity} days ago)"


def get_last_activity_content(order):
    extra = order.get("extra", {})
    configured_field = os.getenv("FILEMAKER_LAST_ACTIVITY_CONTENT_FIELD", "")
    candidate_fields = [
        configured_field,
        "Companies 4::Last Activity Act Content",
        "Companies 4::Last Activity Content",
        "Companies 4::Last Activity Note",
        "Companies 4::Last Activity Notes",
        "Companies 4::Last Activity Detail",
        "Companies 4::Last Activity Details",
        "Companies 4::Last Activity Description",
        "Companies 4::Last Activity",
    ]

    for field_name in candidate_fields:
        if field_name and extra.get(field_name):
            return str(extra.get(field_name))

    return ""


def render_activity_content_note(content):
    if not content:
        return ""

    return (
        "<span class=\"activity-note\">"
        f"{render_activity_content_text(content)}"
        "</span>"
    )


def render_activity_content_text(content):
    text = clean_activity_content(content)

    if not text:
        return ""

    return escape(text)


def get_best_crm_contact_text(latest_crm_activity=None, crm_activities=None):
    candidates = []

    if crm_activities:
        candidates.extend(crm_activities)
    elif latest_crm_activity:
        candidates.append(latest_crm_activity)

    for activity in candidates:
        body_text = clean_activity_content(activity.get("body", ""))

        if body_text:
            return body_text

    for activity in candidates:
        subject_text = clean_activity_content(activity.get("subject", ""))

        if subject_text:
            return f"Subject: {subject_text}"

    return ""


def build_last_contact_display(last_activity_content, latest_crm_activity, crm_activities=None):
    order_text = clean_activity_content(last_activity_content)

    if order_text:
        return order_text, "Orders"

    crm_text = get_best_crm_contact_text(
        latest_crm_activity=latest_crm_activity,
        crm_activities=crm_activities,
    )

    if crm_text:
        return crm_text, "CRM"

    return "", ""


def build_outreach_context(customer, customer_orders, attention, crm_result):
    customer_primary_key = get_customer_primary_key(customer_orders)
    master_data_result = fetch_filemaker_master_data()
    customer_master = (
        master_data_result.get("customers_by_key", {}).get(customer_primary_key, {})
        if master_data_result.get("status") == "ok"
        else {}
    )
    customer_contacts = (
        master_data_result.get("contacts_by_customer_key", {}).get(customer_primary_key, [])
        if master_data_result.get("status") == "ok"
        else []
    )
    contacts_by_email = (
        master_data_result.get("contacts_by_email", {})
        if master_data_result.get("status") == "ok"
        else {}
    )
    crm_activities = (
        crm_result.get("activity_map", {}).get(customer_primary_key, [])
        if crm_result.get("status") == "ok" else []
    )
    sales_activities = get_sales_outreach_activities(crm_activities)
    customer_service_activities = [
        activity for activity in crm_activities
        if get_activity_category(activity) == "customer_services"
    ]
    outbound_sales = [
        activity for activity in sales_activities
        if str(activity.get("direction") or "").strip().lower() == "outbound"
    ]
    inbound_sales = [
        activity for activity in sales_activities
        if str(activity.get("direction") or "").strip().lower() == "inbound"
    ]
    replied_sales_context = []
    latest_replied_outreach = inbound_sales[0] if inbound_sales else None
    recent_sales_context = []
    activity_type_summary = summarize_sales_activity_types(sales_activities)
    activity_type_counts = activity_type_summary["counts"]

    for activity in sales_activities[:5]:
        recent_sales_context.append({
            "date": format_optional_datetime(activity.get("date_created", "")),
            "direction": str(activity.get("direction") or "").strip().title() or "Unknown",
            "crm_type": format_sales_activity_type(activity.get("crm_type")),
            "subject": str(activity.get("subject") or "").strip(),
            "preview": truncate_text(clean_activity_content(activity.get("body", "")) or activity.get("subject", ""), 180),
        })

    last_sales_activity = sales_activities[0] if sales_activities else None
    last_sales_activity_date = (
        format_optional_datetime(last_sales_activity.get("date_created", ""))
        if last_sales_activity else "Not available"
    )
    latest_sales_subject = (
        str(last_sales_activity.get("subject") or "").strip()
        if last_sales_activity else ""
    )
    latest_sales_preview = (
        truncate_text(
            clean_activity_content(last_sales_activity.get("body", "")) or latest_sales_subject,
            220,
        )
        if last_sales_activity else ""
    )
    latest_sales_combined = " ".join(
        part for part in [latest_sales_subject, latest_sales_preview] if part
    ).lower()
    last_reply_date = (
        format_optional_datetime(inbound_sales[0].get("date_created", ""))
        if inbound_sales else "No reply recorded"
    )
    latest_replied_subject = (
        str(latest_replied_outreach.get("subject") or "").strip()
        if latest_replied_outreach else ""
    )
    latest_replied_preview = (
        truncate_text(
            clean_activity_content(latest_replied_outreach.get("body", "")) or latest_replied_subject,
            180,
        )
        if latest_replied_outreach else ""
    )
    latest_replied_context = (
        {
            "date": format_optional_datetime(latest_replied_outreach.get("date_created", "")),
            "direction": str(latest_replied_outreach.get("direction") or "").strip().title() or "Unknown",
            "crm_type": format_sales_activity_type(latest_replied_outreach.get("crm_type")),
            "subject": latest_replied_subject,
            "preview": latest_replied_preview,
        }
        if latest_replied_outreach else None
    )
    approx_response_rate = 0
    if outbound_sales:
        approx_response_rate = round((len(inbound_sales) / len(outbound_sales)) * 100)

    has_recent_sales_activity = False
    days_since_last_sales_activity = None
    if last_sales_activity:
        recent_days = get_crm_days_since_latest_activity(last_sales_activity)
        days_since_last_sales_activity = recent_days
        has_recent_sales_activity = recent_days is not None and recent_days <= 14

    stale_logistics_signal = bool(
        days_since_last_sales_activity is not None
        and days_since_last_sales_activity > 90
        and any(
            phrase in latest_sales_combined
            for phrase in [
                "pickup",
                "pick up",
                "dock",
                "pallet",
                "cage",
                "schedule",
                "shipping",
                "ship",
                "repair request",
            ]
        )
    )

    primary_activity_type = activity_type_summary["primary_type"]
    likely_preferred_mode = "Email"
    if primary_activity_type == "call":
        likely_preferred_mode = "Call"
    elif primary_activity_type == "meeting":
        likely_preferred_mode = "Visit"
    elif primary_activity_type == "linkedin":
        likely_preferred_mode = "LinkedIn"
    elif inbound_sales:
        likely_preferred_mode = "Email"
    elif len(outbound_sales) >= 3:
        likely_preferred_mode = "Call"
    top_contacts = build_outreach_contact_signals(
        sales_activities,
        contacts_by_email=contacts_by_email,
        customer_contacts=customer_contacts,
    )
    primary_contact = top_contacts[0] if top_contacts else None
    order_count = len(customer_orders)
    days_since_last_order = attention.get("days_since_last") if attention else ""

    is_cold_outreach = bool(
        (order_count == 0 and not sales_activities)
        or (
            order_count > 0
            and isinstance(days_since_last_order, (int, float))
            and days_since_last_order >= 180
            and not has_recent_sales_activity
        )
    )

    return {
        "customer": customer,
        "priority_score": attention.get("priority_score") if attention else "",
        "action": attention.get("action") if attention else "",
        "days_since_last_order": days_since_last_order,
        "average_cycle": round(calculate_average_gap(customer_orders), 1) if calculate_average_gap(customer_orders) is not None else "Not enough orders",
        "last_order_date": max(order.get("order_date", "") for order in customer_orders),
        "first_order_date": min(order.get("order_date", "") for order in customer_orders),
        "order_count": order_count,
        "total_value": format_currency(sum_order_amounts(customer_orders)),
        "average_value_per_order": format_currency(average_order_amount(customer_orders)),
        "territory": get_customer_territory(customer_orders),
        "state": get_order_state(customer_orders[-1]) if customer_orders else customer_master.get("state", ""),
        "customer_activity_status": customer_master.get("activity_status", ""),
        "customer_primary_key": customer_primary_key,
        "sales_outreach_sent_count": len(outbound_sales),
        "sales_reply_count": len(inbound_sales),
        "approx_response_rate": f"{approx_response_rate}%" if outbound_sales else "n/a",
        "last_reply_date": last_reply_date,
        "likely_preferred_mode": likely_preferred_mode,
        "activity_type_counts": activity_type_counts,
        "activity_type_pattern": activity_type_summary["pattern"],
        "primary_activity_type": format_sales_activity_type(primary_activity_type),
        "customer_services_present": bool(customer_service_activities),
        "customer_service_activity_count": len(customer_service_activities),
        "sales_activity_count": len(sales_activities),
        "latest_sales_outreach": last_sales_activity_date,
        "days_since_latest_sales_outreach": days_since_last_sales_activity,
        "stale_logistics_signal": stale_logistics_signal,
        "latest_sales_outreach_subject": latest_sales_subject,
        "latest_sales_outreach_preview": latest_sales_preview,
        "latest_replied_outreach_date": last_reply_date,
        "latest_replied_outreach_subject": latest_replied_subject,
        "latest_replied_outreach_preview": latest_replied_preview,
        "latest_replied_context": latest_replied_context,
        "has_recent_sales_activity": has_recent_sales_activity,
        "is_cold_outreach": is_cold_outreach,
        "recent_sales_context": recent_sales_context,
        "top_contacts": top_contacts,
        "primary_contact": primary_contact,
    }


def render_outreach_prep_page(customer, context, result, data_results):
    rationale_items = "".join(
        f"<li>{escape(str(item))}</li>"
        for item in result.get("rationale_bullets", [])
        if str(item).strip()
    ) or "<li>No rationale available.</li>"
    call_points = "".join(
        f"<li>{escape(str(item))}</li>"
        for item in result.get("call_talking_points", [])
        if str(item).strip()
    ) or "<li>No call outline available.</li>"
    recent_sales_rows = "".join(
        render_outreach_sales_context_row(item)
        for item in context.get("recent_sales_context", [])
    ) or "<li class='empty-action'>No recent sales outreach history available.</li>"
    latest_replied_row = (
        render_outreach_sales_context_row(context.get("latest_replied_context"))
        if context.get("latest_replied_context")
        else "<li class='empty-action'>No reply-backed outreach context available yet.</li>"
    )
    contact_rows = "".join(
        render_outreach_contact_row(contact)
        for contact in context.get("top_contacts", [])
    ) or "<li class='empty-action'>No clear contact signals available yet.</li>"
    target_contact = "Not available"
    if result.get("recommended_contact_name") or result.get("recommended_contact_email"):
        target_contact = " - ".join(
            part for part in [
                str(result.get("recommended_contact_name") or "").strip(),
                str(result.get("recommended_contact_email") or "").strip(),
            ]
            if part
        ) or "Not available"

    body = f"""
        {render_data_availability_banner(*data_results)}

        <div class="panel-head outreach-head">
            <div>
                <h1>Outreach Prep</h1>
                <p class="muted">{escape(customer)}</p>
            </div>
            <div class="outreach-links">
                <a class="button secondary small-button" href="/#action-plan">
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path d="M10 6 4 12l6 6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                        <path d="M5 12h15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    </svg>
                    <span>Back to Action Plan</span>
                </a>
                <a class="button secondary small-button" href="/customer-view?customer={quote(customer)}">
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                        <circle cx="12" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/>
                        <path d="M5.5 19c1.8-3 4-4.5 6.5-4.5s4.7 1.5 6.5 4.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    </svg>
                    <span>Open Customer Record</span>
                </a>
            </div>
        </div>

        <div class="summary outreach-summary">
            <div><span class="label">Priority</span><strong>{escape(str(context.get("priority_score", "")) or "n/a")}</strong></div>
            <div><span class="label">Days Since Last Order</span><strong>{escape(str(context.get("days_since_last_order", "")) or "n/a")}</strong></div>
            <div><span class="label">Average Cycle</span><strong>{escape(str(context.get("average_cycle", "")) or "n/a")}</strong></div>
            <div><span class="label">Last Order</span><strong>{escape(str(context.get("last_order_date", "")) or "n/a")}</strong></div>
            <div><span class="label">Last Sales Outreach</span><strong>{escape(str(context.get("latest_sales_outreach", "")) or "n/a")}</strong></div>
            <div><span class="label">Sales Outreach History</span><strong>{escape(str(context.get("sales_activity_count", 0)))}</strong></div>
        </div>

        <div class="outreach-grid">
            <section class="panel outreach-panel">
                <h2>Recommended Next Move</h2>
                <div class="summary compact-summary outreach-recommendation-summary">
                    <div><span class="label">Suggested Mode</span><strong>{escape(str(result.get("suggested_mode", "")) or "Not available")}</strong></div>
                    <div><span class="label">Tone</span><strong>{escape(str(result.get("tone", "")) or "Not available")}</strong></div>
                    <div><span class="label">Confidence</span><strong>{escape(str(result.get("confidence", "")) or "Not available")}</strong></div>
                    <div><span class="label">Target Contact</span><strong>{escape(target_contact)}</strong></div>
                </div>
                <p class="outreach-pattern outreach-note-box"><strong>Targeting note:</strong> {escape(str(result.get("targeting_note", "No specific targeting note available.")))}</p>
                <h3>Why This Was Suggested</h3>
                <ul class="outreach-rationale-list outreach-note-box">{rationale_items}</ul>
            </section>

            <section class="panel outreach-panel">
                <h2>Why This Was Suggested</h2>
                <div class="summary compact-summary outreach-evidence-summary">
                    <div><span class="label">Sales Outreach Sent</span><strong>{escape(str(result.get("sales_outreach_count", 0)))}</strong></div>
                    <div><span class="label">Observed Replies</span><strong>{escape(str(result.get("observed_reply_count", 0)))}</strong></div>
                    <div><span class="label">Approx. Response Rate</span><strong>{escape(str(result.get("approx_response_rate", "n/a")))}</strong></div>
                    <div><span class="label">Last Reply</span><strong>{escape(str(result.get("last_reply_date", "Not available")))}</strong></div>
                    <div><span class="label">Likely Preferred Mode</span><strong>{escape(str(result.get("likely_preferred_mode", "Not available")))}</strong></div>
                    <div><span class="label">Evidence Strength</span><strong>{escape(str(result.get("evidence_strength", "Not available")))}</strong></div>
                </div>
                <p class="outreach-pattern outreach-note-box"><strong>Observed pattern:</strong> {escape(str(result.get("observed_pattern", "No pattern noted.")))}</p>
                <p class="outreach-pattern outreach-note-box"><strong>Customer service traffic present:</strong> {"Yes" if result.get("customer_services_present") else "No"}</p>
            </section>
        </div>

        <section class="panel outreach-panel">
            <h2>Draft Outreach</h2>
            <div class="outreach-draft-block">
                <span class="label">Subject</span>
                <div class="outreach-draft-subject">{escape(str(result.get("email_subject", "")) or "Not available")}</div>
            </div>
            <div class="outreach-draft-block">
                <span class="label">Draft Message</span>
                <div class="outreach-draft-body">{escape(str(result.get("email_body", "")) or "Not available")}</div>
            </div>
        </section>

        <section class="panel outreach-panel">
            <h2>Likely Sales Contacts</h2>
            <ul class="dismissed-list">{contact_rows}</ul>
        </section>

        <div class="outreach-grid">
            <section class="panel outreach-panel">
                <h2>Call Version</h2>
                <p class="outreach-pattern outreach-note-box"><strong>Call objective:</strong> {escape(str(result.get("call_objective", "")) or "Not available")}</p>
                <ul class="outreach-rationale-list outreach-note-box">{call_points}</ul>
                <div class="outreach-draft-block">
                    <span class="label">Suggested Voicemail</span>
                    <div class="outreach-draft-body">{escape(str(result.get("voicemail_draft", "")) or "Not available")}</div>
                </div>
                <div class="outreach-draft-block">
                    <span class="label">Suggested Text Message</span>
                    <div class="outreach-draft-body">{escape(str(result.get("suggested_text_message", "")) or "Not available")}</div>
                </div>
            </section>

            <section class="panel outreach-panel">
                <h2>Recent Sales Outreach Context</h2>
                <ul class="dismissed-list">{recent_sales_rows}</ul>
            </section>
        </div>

        <section class="panel outreach-panel">
            <h2>Latest Replied Outreach</h2>
            <ul class="dismissed-list">{latest_replied_row}</ul>
        </section>
    """
    return body


def render_outreach_sales_context_row(item):
    if not item:
        return ""
    meta_parts = [
        str(item.get("date") or "").strip(),
        str(item.get("direction") or "").strip(),
        str(item.get("crm_type") or "").strip(),
    ]
    meta_text = " • ".join(part for part in meta_parts if part)
    return f"""
        <li>
            <div class="action-item outreach-context-item">
                <div class="action-main">
                    <strong>{escape(str(item.get("subject") or "No subject"))}</strong>
                    <span class="action-meta">{escape(meta_text)}</span>
                    <span class="action-cue">{escape(str(item.get("preview") or ""))}</span>
                </div>
            </div>
        </li>
    """


def render_outreach_contact_row(contact):
    title = str(contact.get("title") or "").strip()
    role_label = str(contact.get("role") or "Unknown role")
    role_text = title if title else role_label
    if title and role_label and role_label.lower() not in title.lower():
        role_text = f"{title} • {role_label}"
    return f"""
        <li>
            <div class="action-item outreach-context-item">
                <div class="action-main">
                    <strong>{escape(str(contact.get("name") or "Unknown contact"))}</strong>
                    <span class="action-meta">{escape(str(contact.get("email") or ""))}</span>
                    <span class="action-meta">{escape(role_text)} • Influence {escape(str(contact.get("influence") or "Low"))} • Last active {escape(str(contact.get("last_active") or "Not available"))}</span>
                    <span class="action-cue">{escape(str(contact.get("note") or ""))}</span>
                    <span class="action-cue">Replies {escape(str(contact.get("inbound_count", 0)))} • Outbound touches {escape(str(contact.get("outbound_count", 0)))}</span>
                </div>
            </div>
        </li>
    """


def clean_activity_content(content):
    content = str(content or "").strip()

    if not content:
        return ""

    if looks_like_html(content):
        content = ActivityHTMLToText.convert(content)

    content = tidy_activity_text(content)
    return strip_activity_noise(content)


def get_internal_email_domains():
    raw_domains = os.getenv(
        "CRM_INTERNAL_DOMAINS",
        "numatsystems.com,nufox.com",
    )
    return {
        domain.strip().lower()
        for domain in raw_domains.split(",")
        if domain.strip()
    }


def is_external_email_address(email):
    email = str(email or "").strip().lower()

    if "@" not in email:
        return False

    domain = email.split("@", 1)[1].strip().lower()
    return bool(domain) and domain not in get_internal_email_domains()


def infer_contact_display_name(email):
    email = str(email or "").strip().lower()

    if "@" not in email:
        return "Unknown contact"

    local_part = email.split("@", 1)[0].strip()

    if not local_part:
        return "Unknown contact"

    if any(separator in local_part for separator in [".", "_", "-"]):
        parts = [
            part.capitalize()
            for part in re.split(r"[._-]+", local_part)
            if part
        ]
        return " ".join(parts) or email

    if len(local_part) >= 5 and local_part[-1].isalpha() and local_part[:-1].isalpha():
        return f"{local_part[:-1].capitalize()} {local_part[-1].upper()}"

    return local_part.capitalize()


def get_master_contact_role(contact):
    title = str(contact.get("position") or "").strip()
    title_lower = title.lower()

    if not title:
        return "", "", ""

    if any(keyword in title_lower for keyword in ["owner", "president", "general manager", "director", "vice president", "vp"]):
        return "Decision path / blocker", "High", "Title suggests this contact may influence or make repair decisions."

    if any(keyword in title_lower for keyword in ["manager", "merchandise control", "operations", "buyer", "purchasing", "plant", "production", "supervisor"]):
        return "Operational contact", "Medium", "Title suggests a hands-on operational or plant-level contact."

    if any(keyword in title_lower for keyword in ["accounts payable", "accounting", "finance", "billing", "admin", "administrator"]):
        return "Finance / admin", "Low", "Title suggests finance or admin responsibility."

    return "", "", ""


def infer_contact_role_signal(contact):
    title_role, title_influence, title_note = get_master_contact_role(contact)
    if title_role:
        return title_role, title_influence, title_note

    combined_text = " ".join(contact.get("context_snippets", [])).lower()
    finance_hits = sum(
        keyword in combined_text
        for keyword in [
            "invoice",
            "purchase order",
            "accounts payable",
            "payment",
            "paid",
            "shipping document",
            "shipping documents",
            "po number",
        ]
    )
    decision_hits = sum(
        keyword in combined_text
        for keyword in [
            "gm",
            "general manager",
            "plant manager",
            "manager doesn't want",
            "don't want",
            "do not want",
            "out of my control",
            "out of my hands",
            "approval",
            "approved",
            "president",
            "director",
            "owner",
        ]
    )
    operational_hits = sum(
        keyword in combined_text
        for keyword in [
            "plant",
            "site",
            "visit",
            "tour",
            "repair",
            "damaged mats",
            "mat repair",
            "processing",
            "facility",
            "buyer",
            "purchasing",
            "merchandise control",
            "operations",
        ]
    )

    if finance_hits and finance_hits >= max(decision_hits, operational_hits):
        return "Finance / admin", "Low", "Mainly finance or admin traffic."
    if decision_hits:
        return "Decision path / blocker", "High", "Replies reference decision-makers or approval blockers."
    if operational_hits:
        return "Operational contact", "Medium", "Recent history is mostly operational or plant-level discussion."
    if contact.get("inbound_count", 0) >= 2:
        return "Active responder", "Medium", "This contact has replied directly more than once."
    return "Unknown role", "Low", "Limited signal on role from recent sales history."


def build_outreach_contact_signals(sales_activities, contacts_by_email=None, customer_contacts=None):
    contacts_by_email = contacts_by_email or {}
    customer_contacts = customer_contacts or []
    contacts = {}

    def get_contact_key(email, master_contact):
        if email:
            return email

        primary_key = str(master_contact.get("primary_key") or "").strip()
        if primary_key:
            return f"contact:{primary_key}"

        return f"name:{str(master_contact.get('name') or '').strip().lower()}"

    def seed_contact(email="", master_contact=None):
        master_contact = master_contact or {}
        key = get_contact_key(email, master_contact)
        seeded_email = str(email or master_contact.get("email") or "").strip().lower()
        seeded_name = str(master_contact.get("name") or "").strip() or (
            infer_contact_display_name(seeded_email) if seeded_email else "Unknown contact"
        )
        contact = contacts.setdefault(key, {
            "name": seeded_name,
            "email": seeded_email,
            "title": str(master_contact.get("position") or "").strip(),
            "phone": str(master_contact.get("phone") or "").strip(),
            "cell": str(master_contact.get("cell") or "").strip(),
            "inbound_count": 0,
            "outbound_count": 0,
            "unknown_count": 0,
            "last_active_raw": "",
            "last_active": "Not available",
            "latest_subject": "",
            "latest_preview": "",
            "context_snippets": [],
        })

        if master_contact:
            if str(master_contact.get("name") or "").strip():
                contact["name"] = str(master_contact.get("name") or "").strip()
            if str(master_contact.get("position") or "").strip():
                contact["title"] = str(master_contact.get("position") or "").strip()
            if str(master_contact.get("phone") or "").strip():
                contact["phone"] = str(master_contact.get("phone") or "").strip()
            if str(master_contact.get("cell") or "").strip():
                contact["cell"] = str(master_contact.get("cell") or "").strip()
            if seeded_email and not contact.get("email"):
                contact["email"] = seeded_email

        return contact

    for activity in sales_activities:
        direction = str(activity.get("direction") or "").strip().lower()
        subject = str(activity.get("subject") or "").strip()
        preview = truncate_text(
            clean_activity_content(activity.get("body", "")) or subject,
            140,
        )
        activity_date_raw = str(activity.get("date_created") or "").strip()
        activity_date = parse_crm_datetime(activity_date_raw)

        contact_emails = []
        if direction == "inbound":
            sender_email = str(activity.get("sender_email") or "").strip().lower()
            if is_external_email_address(sender_email):
                contact_emails.append(sender_email)
        else:
            for email in extract_emails(str(activity.get("to") or "")):
                normalized_email = str(email or "").strip().lower()
                if is_external_email_address(normalized_email):
                    contact_emails.append(normalized_email)

        for email in dict.fromkeys(contact_emails):
            master_contact = contacts_by_email.get(email, {})
            contact = seed_contact(email=email, master_contact=master_contact)

            if direction == "inbound":
                contact["inbound_count"] += 1
            elif direction == "outbound":
                contact["outbound_count"] += 1
            else:
                contact["unknown_count"] += 1

            if activity_date and (
                not contact["last_active_raw"]
                or activity_date > parse_crm_datetime(contact["last_active_raw"])
            ):
                contact["last_active_raw"] = activity_date_raw
                contact["last_active"] = format_optional_datetime(activity_date_raw)
                contact["latest_subject"] = subject
                contact["latest_preview"] = preview

            snippet = " ".join(
                part for part in [subject, preview] if part
            ).strip()
            if snippet and len(contact["context_snippets"]) < 6:
                contact["context_snippets"].append(snippet)

    for master_contact in customer_contacts:
        seed_contact(
            email=str(master_contact.get("email") or "").strip().lower(),
            master_contact=master_contact,
        )

    enriched_contacts = []
    today = get_analysis_today()

    for contact in contacts.values():
        role, influence, note = infer_contact_role_signal(contact)
        last_active_dt = parse_crm_datetime(contact["last_active_raw"])
        recency_score = 0
        if last_active_dt:
            days_old = max(0, (today - last_active_dt).days)
            recency_score = max(0, 60 - min(days_old, 60))

        contact_score = (
            contact["inbound_count"] * 5
            + contact["outbound_count"] * 3
            + recency_score
            + (25 if influence == "High" else 12 if influence == "Medium" else 0)
        )
        enriched_contacts.append({
            "name": contact["name"],
            "email": contact["email"],
            "title": contact.get("title", ""),
            "role": role,
            "influence": influence,
            "note": note,
            "last_active": contact["last_active"],
            "inbound_count": contact["inbound_count"],
            "outbound_count": contact["outbound_count"],
            "latest_subject": contact["latest_subject"],
            "latest_preview": contact["latest_preview"],
            "score": contact_score,
        })

    enriched_contacts.sort(
        key=lambda item: (
            -item["score"],
            item["name"].lower(),
        )
    )
    return enriched_contacts[:3]


def normalize_sales_activity_type(value):
    activity_type = str(value or "").strip().lower()

    if not activity_type:
        return "unknown"

    if activity_type in {"email", "outlook", "mail"}:
        return "email"

    if activity_type in {"call", "phone", "telephone"}:
        return "call"

    if activity_type in {"meeting", "visit", "tour", "onsite", "on site"}:
        return "meeting"

    if activity_type == "linkedin":
        return "linkedin"

    return activity_type


def format_sales_activity_type(value):
    normalized = normalize_sales_activity_type(value)

    labels = {
        "email": "Email",
        "call": "Call",
        "meeting": "Meeting",
        "linkedin": "LinkedIn",
        "unknown": "Unknown",
    }
    return labels.get(normalized, normalized.replace("_", " ").title())


def summarize_sales_activity_types(sales_activities):
    normalized_types = [
        normalize_sales_activity_type(activity.get("crm_type"))
        for activity in sales_activities
        if normalize_sales_activity_type(activity.get("crm_type")) != "unknown"
    ]
    counts = Counter(normalized_types)

    if not counts:
        return {
            "counts": {},
            "primary_type": "",
            "pattern": "The outreach history does not yet show a clear contact mode pattern.",
        }

    primary_type, primary_count = counts.most_common(1)[0]
    total_known = sum(counts.values())
    primary_share = primary_count / total_known if total_known else 0

    if primary_type == "meeting":
        if counts.get("email", 0):
            pattern = "The relationship looks meeting-led with email used to coordinate and follow up."
        else:
            pattern = "The relationship looks meeting-led rather than driven mainly by email."
    elif primary_type == "call":
        if counts.get("email", 0):
            pattern = "The history shows a mix of calls and email, with calls playing the stronger role."
        else:
            pattern = "The history looks call-led rather than email-led."
    elif primary_type == "linkedin":
        pattern = "The history suggests lighter-touch outreach, including LinkedIn contact."
    else:
        if counts.get("meeting", 0):
            pattern = "Email is the main channel, with some meeting or visit context in the history."
        elif counts.get("call", 0):
            pattern = "Email is the main channel, with some supporting call activity."
        else:
            pattern = "Email is the main channel in the sales history."

    if primary_share < 0.5 and len(counts) > 1:
        pattern = "The history is mixed across contact modes, without one channel dominating strongly."

    return {
        "counts": dict(counts),
        "primary_type": primary_type,
        "pattern": pattern,
    }


def looks_like_html(content):
    lowered_content = content.lower()
    html_markers = [
        "<html",
        "<body",
        "<div",
        "<p",
        "<br",
        "<table",
        "<span",
        "<style",
        "&nbsp;",
    ]

    return any(marker in lowered_content for marker in html_markers)


def tidy_activity_text(content):
    lines = [
        " ".join(line.split())
        for line in content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    tidy_lines = []

    for line in lines:
        if line or (tidy_lines and tidy_lines[-1]):
            tidy_lines.append(line)

    return "\n".join(tidy_lines).strip()


def strip_activity_noise(content):
    lines = content.split("\n")
    lines = remove_signature_sections(lines)
    lines = remove_disclaimer_lines(lines)
    return tidy_activity_text("\n".join(lines))


def remove_signature_sections(lines):
    kept_lines = []
    in_signature = False

    for index, line in enumerate(lines):
        lowered = line.strip().lower()

        if is_reply_header_line(lowered):
            in_signature = False
            kept_lines.append(line)
            continue

        if in_signature:
            continue

        if should_start_signature(lines, index):
            trim_signature_prefix(kept_lines)
            in_signature = True
            continue

        kept_lines.append(line)

    return kept_lines


def is_reply_header_line(lowered_line):
    return lowered_line.startswith(("from:", "sent:", "to:", "subject:", "cc:", "bcc:"))


def should_start_signature(lines, index):
    line = lines[index].strip()
    lowered = line.lower()

    if not lowered:
        return False

    direct_markers = (
        "sent from my iphone",
        "sent from my ipad",
        "sent from my mobile",
        "cell:",
        "tel:",
        "fax:",
        "email:",
        "web:",
        "www.",
        "linkedin",
        "instagram",
        "facebook",
    )

    if any(marker in lowered for marker in direct_markers):
        return True

    if "|" in line and any(marker in lowered for marker in (" p:", " e:", " m:", " o:", "@")):
        return True

    lookahead = [
        lines[next_index].strip().lower()
        for next_index in range(index + 1, min(len(lines), index + 4))
        if lines[next_index].strip()
    ]

    title_markers = (
        "manager",
        "director",
        "owner",
        "sales",
        "service",
        "systems",
        "llc",
        "inc",
        "corp",
        "corporation",
        "coordinator",
        "president",
        "vice president",
    )

    if (
        any(marker in lowered for marker in title_markers)
        and any(
            (
                "@" in next_line
                or "www." in next_line
                or next_line.startswith(("cell:", "tel:", "fax:", "email:", "web:"))
            )
            for next_line in lookahead
        )
    ):
        return True

    return False


def trim_signature_prefix(kept_lines):
    removable_signoffs = {"thanks,", "thanks", "regards,", "regards", "best,", "best"}

    while kept_lines:
        previous = kept_lines[-1].strip()
        previous_lower = previous.lower()

        if not previous:
            kept_lines.pop()
            continue

        if previous_lower in removable_signoffs:
            kept_lines.pop()
            continue

        if len(previous.split()) <= 5 and previous == previous.title():
            kept_lines.pop()
            continue

        break


def remove_disclaimer_lines(lines):
    disclaimer_markers = (
        "all rights reserved",
        "exclusive use of addressee",
        "intended recipient",
        "proprietary, confidential",
        "any use, copying, disclosure",
        "notify the sender immediately",
        "delete this communication",
        "destroy all copies",
    )

    cleaned_lines = []

    for line in lines:
        lowered = line.strip().lower()

        if any(marker in lowered for marker in disclaimer_markers):
            continue

        cleaned_lines.append(line)

    return cleaned_lines


class ActivityHTMLToText(HTMLParser):
    block_tags = {
        "address",
        "blockquote",
        "br",
        "div",
        "li",
        "p",
        "table",
        "td",
        "th",
        "tr",
    }

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.skip_depth = 0

    @classmethod
    def convert(cls, content):
        parser = cls()
        parser.feed(content)
        parser.close()
        return "".join(parser.parts)

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.skip_depth += 1
            return

        if self.skip_depth:
            return

        if tag in self.block_tags:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style"} and self.skip_depth:
            self.skip_depth -= 1
            return

        if self.skip_depth:
            return

        if tag in self.block_tags:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.skip_depth:
            self.parts.append(data)


def has_recent_activity(days_since_activity):
    return (
        days_since_activity is not None
        and days_since_activity <= 14
        and days_since_activity >= 0
    )


def render_recent_activity_badge(customer):
    if not has_recent_activity(customer.get("days_since_last_activity")):
        return ""

    return '<span class="badge">Already contacted recently</span>'


def render_recent_activity_badge_for_date(activity_date):
    if not activity_date:
        return ""

    from analysis import get_analysis_today

    days_since_activity = (
        get_analysis_today().date() - activity_date.date()
    ).days

    if not has_recent_activity(days_since_activity):
        return ""

    return '<span class="badge">Already contacted recently</span>'


def count_recent_attention_activity(customers):
    return sum(
        1 for customer in customers
        if has_recent_activity(customer.get("days_since_last_activity"))
    )


def render_monthly_orders_chart(orders):
    monthly_counts = defaultdict(int)
    monthly_price_lists = defaultdict(Counter)
    palette = {
        "A": "#245cff",
        "B": "#06b6d4",
        "C": "#16a34a",
        "D": "#f59e0b",
        "Other": "#94a3b8",
    }

    for order in orders:
        order_date = parse_iso_date(order.get("order_date"))

        if not order_date:
            continue

        month_key = order_date.strftime("%Y-%m")
        monthly_counts[month_key] += 1
        price_list = str(get_order_price_list(order) or "").strip().upper()
        if price_list not in {"A", "B", "C", "D"}:
            price_list = "Other"
        monthly_price_lists[month_key][price_list] += 1

    months = sorted(monthly_counts.keys())[-12:]
    max_count = max([monthly_counts[month] for month in months], default=1)
    ordered_buckets = ["A", "B", "C", "D", "Other"]

    legend_items = []
    for bucket in ordered_buckets:
        if any(monthly_price_lists[month].get(bucket, 0) for month in months):
            legend_items.append(
                f"""
                    <li class="stacked-legend-item">
                        <span class="stacked-legend-swatch" style="background:{palette[bucket]}"></span>
                        <span>{escape(bucket)}</span>
                    </li>
                """
            )

    bars = "".join(
        render_stacked_bar(
            label=datetime.strptime(month, "%Y-%m").strftime("%b %Y"),
            segments=[
                {
                    "label": bucket,
                    "value": monthly_price_lists[month].get(bucket, 0),
                    "color": palette[bucket],
                }
                for bucket in ordered_buckets
                if monthly_price_lists[month].get(bucket, 0)
            ],
            total=monthly_counts[month],
            max_value=max_count,
        )
        for month in months
    )

    if not bars:
        bars = "<p class='empty'>No dated orders available.</p>"

    return f"""
        <section class="panel">
            <h2>Order Trend</h2>
            <p class="muted">Order count by month, split by price list.</p>
            <ul class="stacked-legend">
                {''.join(legend_items)}
            </ul>
            <div class="bars">{bars}</div>
        </section>
    """


def render_attention_chart(customers):
    action_counts = Counter(customer["action"] for customer in customers)
    if not action_counts:
        return """
            <section class="panel">
                <h2>Attention Breakdown</h2>
                <p class="muted">How urgent the current follow-up queue is.</p>
                <p class='empty'>No customers currently need attention.</p>
            </section>
        """

    palette = ["#245cff", "#16a34a", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"]
    items = [
        {
            "label": action,
            "value": count,
            "color": palette[index % len(palette)],
        }
        for index, (action, count) in enumerate(action_counts.most_common())
    ]

    return render_donut_chart(
        title="Attention Breakdown",
        subtitle="How urgent the current follow-up queue is.",
        items=items,
        center_value=str(sum(action_counts.values())),
        center_label="Accounts",
    )


def render_state_orders_chart(orders):
    state_counts = Counter()
    state_values = defaultdict(float)

    for order in orders:
        extra = order.get("extra", {})
        state = str(get_order_state(order) or "Unknown").strip()

        if not state:
            state = "Unknown"

        state_counts[state] += 1

        try:
            state_values[state] += float(order.get("amount") or 0)
        except ValueError:
            continue

    top_states = state_counts.most_common(10)
    max_count = max([count for _, count in top_states], default=1)
    bars = "".join(
        render_bar(
            label=f"{state} ({format_currency(state_values[state])})",
            value=count,
            max_value=max_count,
            color="#f97316",
        )
        for state, count in top_states
    )

    if not bars:
        bars = "<p class='empty'>No state data available.</p>"

    return f"""
        <section class="panel">
            <h2>Orders by State</h2>
            <p class="muted">Top states by order count, with recorded repair value in brackets.</p>
            <div class="bars wide-labels">{bars}</div>
        </section>
    """


def render_monthly_sales_outreach_chart(sales_activities):
    monthly_counts = defaultdict(int)

    for activity in sales_activities:
        if str(activity.get("direction") or "").strip().lower() != "outbound":
            continue

        activity_date = parse_crm_datetime(activity.get("date_created", ""))

        if not activity_date:
            continue

        monthly_counts[activity_date.strftime("%Y-%m")] += 1

    months = sorted(monthly_counts.keys())[-12:]
    max_count = max([monthly_counts[month] for month in months], default=1)
    bars = "".join(
        render_bar(
            label=datetime.strptime(month, "%Y-%m").strftime("%b %Y"),
            value=monthly_counts[month],
            max_value=max_count,
            color="#06b6d4",
        )
        for month in months
    )

    if not bars:
        bars = "<p class='empty'>No outbound sales outreach with usable dates yet.</p>"

    return f"""
        <section class="panel">
            <h2>Sales Outreach Trend</h2>
            <p class="muted">Outbound sales outreach by month across the recent history.</p>
            <div class="bars">{bars}</div>
        </section>
    """


def render_sales_response_chart(outbound_sales, inbound_sales):
    outbound_count = len(outbound_sales)
    inbound_count = len(inbound_sales)
    response_rate = round((inbound_count / outbound_count) * 100) if outbound_count else 0
    max_value = max(outbound_count, inbound_count, response_rate or 1)

    bars = "".join([
        render_bar("Outbound", outbound_count, max_value, color="#245cff"),
        render_bar("Replies", inbound_count, max_value, color="#16a34a"),
        render_bar("Reply Rate", response_rate, max_value, color="#8b5cf6"),
    ])

    return f"""
        <section class="panel">
            <h2>Sales Response Snapshot</h2>
            <p class="muted">A quick view of outbound volume, observed replies, and approximate response rate.</p>
            <div class="bars">{bars}</div>
        </section>
    """


def render_sales_activity_type_chart(sales_activities):
    counts = Counter()

    for activity in sales_activities:
        normalized_type = normalize_sales_activity_type(activity.get("crm_type"))

        counts[format_sales_activity_type(normalized_type)] += 1

    if not counts:
        return """
            <section class="panel">
                <h2>Mode Mix</h2>
                <p class="muted">How sales outreach has been logged across email, calls, meetings, and other activity types.</p>
                <p class='empty'>No typed sales activity records available yet.</p>
            </section>
        """

    palette = ["#245cff", "#06b6d4", "#8b5cf6", "#16a34a", "#f59e0b", "#ef4444", "#14b8a6"]
    items = [
        {
            "label": label,
            "value": count,
            "color": palette[index % len(palette)],
        }
        for index, (label, count) in enumerate(counts.most_common())
    ]

    return render_donut_chart(
        title="Mode Mix",
        subtitle="How sales outreach has been logged across email, calls, meetings, and other activity types.",
        items=items,
        center_value=str(sum(counts.values())),
        center_label="Touches",
    )


def render_donut_chart(title, subtitle, items, center_value="", center_label=""):
    total = sum(int(item.get("value", 0) or 0) for item in items)

    if total <= 0:
        return f"""
            <section class="panel">
                <h2>{escape(title)}</h2>
                <p class="muted">{escape(subtitle)}</p>
                <p class='empty'>No data available.</p>
            </section>
        """

    stops = []
    legend_rows = []
    current = 0.0

    for item in items:
        value = int(item.get("value", 0) or 0)
        color = str(item.get("color") or "#245cff")
        label = str(item.get("label") or "")
        slice_size = (value / total) * 360
        stops.append(f"{color} {current:.2f}deg {current + slice_size:.2f}deg")
        current += slice_size
        percentage = round((value / total) * 100)
        legend_rows.append(
            f"""
                <li class="donut-legend-row">
                    <span class="donut-legend-label">
                        <span class="donut-swatch" style="background:{escape(color)}"></span>
                        {escape(label)}
                    </span>
                    <span class="donut-legend-value">{escape(str(value))} <span class="muted-soft">({percentage}%)</span></span>
                </li>
            """
        )

    donut_style = f"background: conic-gradient({', '.join(stops)});"

    return f"""
        <section class="panel">
            <h2>{escape(title)}</h2>
            <p class="muted">{escape(subtitle)}</p>
            <div class="donut-layout">
                <div class="donut-chart" style="{donut_style}">
                    <div class="donut-hole">
                        <strong>{escape(center_value or str(total))}</strong>
                        <span>{escape(center_label or 'Total')}</span>
                    </div>
                </div>
                <ul class="donut-legend">
                    {''.join(legend_rows)}
                </ul>
            </div>
        </section>
    """


def render_bar(label, value, max_value, color="#245cff"):
    width = 0

    if max_value:
        width = max(6, round((value / max_value) * 100))

    return f"""
        <div class="bar-row">
            <span>{escape(str(label))}</span>
            <div class="bar-track">
                <div class="bar-fill" style="width: {width}%; background: {escape(str(color))}"></div>
            </div>
            <strong>{escape(str(value))}</strong>
        </div>
    """


def render_stacked_bar(label, segments, total, max_value):
    if not total or not max_value:
        return ""

    segment_markup = []
    for segment in segments:
        value = segment.get("value", 0)
        if not value:
            continue
        width = max(2, (value / max_value) * 100)
        segment_markup.append(
            f"<span class='stacked-segment' style='width:{width:.2f}%; background:{escape(str(segment.get('color') or '#245cff'))}'></span>"
        )

    return f"""
        <div class="bar-row stacked-bar-row">
            <span>{escape(str(label))}</span>
            <div class="bar-track stacked-track">
                {''.join(segment_markup)}
            </div>
            <strong>{escape(str(total))}</strong>
        </div>
    """


def render_top_attention_table(customers):
    top_customers = sort_late_customers(
        customers,
        sort_key="priority_score",
        direction="desc"
    )[:5]

    rows = "".join(
        render_top_attention_row(customer)
        for customer in top_customers
    )

    if not rows:
        rows = (
            "<tr>"
            "<td colspan='5' class='empty'>No customers currently need attention.</td>"
            "</tr>"
        )

    return f"""
        <section class="panel">
            <h2>Highest Priority Customers</h2>
            <table>
                <thead>
                    <tr>
                        <th>Customer</th>
                        <th>Priority</th>
                        <th>Days Since Last</th>
                        <th>Action</th>
                        <th>Last Activity</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </section>
    """


def render_top_attention_row(customer):
    customer_name = str(customer["customer"])
    customer_url = f"/customer-view?customer={quote(customer_name)}"

    return f"""
        <tr>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td><span class="score">{escape(str(customer["priority_score"]))}</span></td>
            <td>{escape(str(customer["days_since_last"]))} days</td>
            <td>{escape(str(customer["action"]))}</td>
            <td>
                {escape(format_activity_summary(customer))}
                {render_recent_activity_badge(customer)}
            </td>
        </tr>
    """


def build_home_action_plan(attention_customers, grouped_orders, dismissed_customers=None):
    due_today_customers = []
    hold_customers = []
    dismissed_customers = dismissed_customers or []

    for customer in attention_customers:
        customer_name = str(customer.get("customer", ""))
        customer_orders = grouped_orders.get(customer_name, [])
        territory = get_customer_territory(customer_orders).lower()
        enriched_customer = dict(customer)
        enriched_customer["territory"] = territory.title() if territory else "Unassigned"
        enriched_customer["last_order"] = (
            max(order.get("order_date", "") for order in customer_orders)
            if customer_orders else ""
        )

        if has_recent_activity(customer.get("days_since_last_activity")):
            hold_customers.append(enriched_customer)
            continue

        if float(enriched_customer.get("priority_score") or 0) >= 1.5:
            due_today_customers.append(enriched_customer)

    hold_customers = sort_late_customers(
        hold_customers,
        sort_key="days_since_last_activity",
        direction="asc",
    )[:6]
    due_today_customers = sort_late_customers(
        due_today_customers,
        sort_key="priority_score",
        direction="desc",
    )[:10]
    dismissed_customers = [
        customer for customer in dismissed_customers
        if float(customer.get("priority_score") or 0) >= 1.5
    ]

    return {
        "due_today": due_today_customers,
        "hold_customers": hold_customers,
        "dismissed": dismissed_customers,
    }


def get_customer_territory(customer_orders):
    territories = Counter(
        str(get_order_territory(order) or "").strip()
        for order in customer_orders
        if str(get_order_territory(order) or "").strip()
    )
    return territories.most_common(1)[0][0] if territories else ""


def get_action_plan_dismissals_path():
    raw_path = os.getenv("ACTION_PLAN_DISMISSALS_PATH", "").strip()

    if raw_path:
        return Path(raw_path).expanduser()

    return DEFAULT_ACTION_PLAN_DISMISSALS_PATH


def read_action_plan_dismissals():
    dismissals_path = get_action_plan_dismissals_path()

    try:
        if not dismissals_path.exists():
            return {}

        payload = json.loads(dismissals_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    normalized = {}

    for customer_name, dismissal in payload.items():
        if not isinstance(dismissal, dict):
            continue

        normalized[str(customer_name)] = {
            "last_order": str(dismissal.get("last_order") or "").strip(),
            "dismissed_at": str(dismissal.get("dismissed_at") or "").strip(),
            "reason": str(dismissal.get("reason") or "").strip(),
        }

    return normalized


def write_action_plan_dismissals(dismissals):
    dismissals_path = get_action_plan_dismissals_path()
    dismissals_path.parent.mkdir(parents=True, exist_ok=True)
    dismissals_path.write_text(json.dumps(dismissals, indent=2), encoding="utf-8")


def get_action_plan_dismissal(customer_name, last_order, dismissals=None):
    dismissals = dismissals or read_action_plan_dismissals()
    dismissal = dismissals.get(str(customer_name))

    if not dismissal:
        return None

    if str(dismissal.get("last_order") or "").strip() != str(last_order or "").strip():
        return None

    return dismissal

def is_action_plan_dismissed(customer_name, last_order, dismissals=None):
    return bool(get_action_plan_dismissal(customer_name, last_order, dismissals=dismissals))


def dismiss_action_plan_customer(customer_name, last_order, reason=""):
    dismissals = read_action_plan_dismissals()
    dismissals[str(customer_name)] = {
        "last_order": str(last_order or "").strip(),
        "dismissed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": str(reason or "").strip(),
    }
    write_action_plan_dismissals(dismissals)


def restore_action_plan_customer(customer_name):
    dismissals = read_action_plan_dismissals()

    if str(customer_name) in dismissals:
        dismissals.pop(str(customer_name), None)
        write_action_plan_dismissals(dismissals)


def get_order_territory(order):
    extra = order.get("extra", {})
    return extra.get("Companies 4::Territory") or extra.get("ai_Territory") or ""


def get_order_state(order):
    extra = order.get("extra", {})
    return extra.get("Companies 4::State") or extra.get("ai_State") or ""


def get_order_price_list(order):
    extra = order.get("extra", {})
    return extra.get("Companies 4::Price List") or extra.get("ai_PriceList") or ""


def get_order_primary_key(order):
    extra = order.get("extra", {})
    return extra.get("Companies 4::PrimaryKey") or extra.get("Customer Ref") or ""


def get_crm_days_since_latest_activity(latest_crm_activity):
    if not latest_crm_activity:
        return None

    activity_date = parse_crm_datetime(latest_crm_activity.get("date_created", ""))

    if not activity_date:
        return None

    return (get_analysis_today().date() - activity_date.date()).days


def get_sales_outreach_activities(activities):
    return [
        activity
        for activity in activities
        if get_activity_category(activity) != "customer_services"
    ]


def get_activity_category(activity):
    category_value = str(activity.get("activity_category") or "").strip().lower()

    if category_value:
        return category_value

    sender_email = str(activity.get("sender_email") or "").strip().lower()
    recipient_emails = extract_emails(str(activity.get("to") or ""))
    sender_company = str(activity.get("sender_company") or "").strip()
    subject = str(activity.get("subject") or "").strip()
    body = str(activity.get("body") or "").strip()

    if is_customer_service_activity(
        sender_email=sender_email,
        recipient_emails=recipient_emails,
        sender_company=sender_company,
        subject=subject,
        body=body,
    ):
        return "customer_services"

    return "sales_outreach"


def render_home_action_plan(action_plan):
    due_today_items = "".join(
        render_action_plan_item(customer)
        for customer in action_plan["due_today"]
    )

    if not due_today_items:
        due_today_items = "<li class='empty-action'>No immediate follow-ups right now.</li>"

    return f"""
        <section class="panel action-plan-panel" id="action-plan">
            <div class="panel-head action-plan-head">
                <h2>Today's Action Plan</h2>
            </div>
            <ol class="action-list action-list-wide">{due_today_items}</ol>
            {render_dismissed_action_plan_items(action_plan.get("dismissed", []))}
        </section>
    """


def render_home_workflow_layout(action_plan, queue_summaries, selected_preview, home_view, dismiss_customer=""):
    queue_rows = "".join(
        render_home_queue_row(
            summary,
            is_selected=(
                home_view == "focus"
                and selected_preview
                and str(summary.get("customer")) == str(selected_preview.get("customer"))
            ),
        )
        for summary in queue_summaries
    )

    if not queue_rows:
        queue_rows = (
            "<tr>"
            "<td colspan='5' class='empty'>No immediate follow-ups right now.</td>"
            "</tr>"
        )

    return f"""
        <section class="home-workspace">
            {render_home_view_toggle(selected_preview.get("customer", "") if selected_preview else "", home_view)}
            {render_home_focus_preview(selected_preview, dismiss_open=str(dismiss_customer or "").strip().lower() == str((selected_preview or {}).get("customer", "")).strip().lower()) if home_view == "focus" else ""}

            <section class="panel home-queue-panel" id="home-queue">
                <div class="panel-head home-queue-head">
                    <div>
                        <h2>Action Plan</h2>
                    </div>
                </div>
                <div class="table-wrap home-queue-wrap">
                    <table class="queue-table">
                        <thead>
                            <tr>
                                <th>Customer</th>
                                <th>Location</th>
                                <th>Mode</th>
                                <th>Next Action</th>
                                <th>Why Now</th>
                            </tr>
                        </thead>
                        <tbody>{queue_rows}</tbody>
                    </table>
                </div>
            </section>

            {render_recent_contact_holds(action_plan["hold_customers"])}
            {render_dismissed_action_plan_items(action_plan.get("dismissed", []), return_to="/#home-queue")}
        </section>
    """


def render_home_view_toggle(selected_customer, home_view):
    focus_href = build_home_selection_href(
        selected_customer=selected_customer,
        view="focus",
        anchor="focus-preview",
    )
    list_href = build_home_selection_href(
        selected_customer=selected_customer,
        view="list",
        anchor="home-queue",
    )
    focus_class = "toggle-chip active" if home_view == "focus" else "toggle-chip"
    list_class = "toggle-chip active" if home_view == "list" else "toggle-chip"

    return f"""
        <div class="home-view-toggle">
            <a class="{focus_class}" href="{focus_href}">Focus</a>
            <a class="{list_class}" href="{list_href}">List</a>
        </div>
    """


def build_home_selection_href(selected_customer="", view="focus", anchor=""):
    params = {}

    if selected_customer:
        params["selected_customer"] = str(selected_customer)

    if view and view != "focus":
        params["view"] = str(view)

    query = urlencode(params)
    href = "/"

    if query:
        href += f"?{query}"

    if anchor:
        href += f"#{anchor}"

    return href


def get_home_selected_customer_name(queue_summaries, selected_customer=""):
    if not queue_summaries:
        return ""

    selected_lookup = str(selected_customer or "").strip().lower()

    if selected_lookup:
        for summary in queue_summaries:
            if str(summary.get("customer", "")).strip().lower() == selected_lookup:
                return str(summary.get("customer", ""))

    return str(queue_summaries[0].get("customer", ""))


def build_home_queue_summaries(queue_customers, grouped_orders, crm_activity_map):
    summaries = []
    master_data_result = fetch_filemaker_master_data()
    contacts_by_customer_key = (
        master_data_result.get("contacts_by_customer_key", {})
        if master_data_result.get("status") == "ok"
        else {}
    )
    contacts_by_email = (
        master_data_result.get("contacts_by_email", {})
        if master_data_result.get("status") == "ok"
        else {}
    )

    for customer in queue_customers:
        customer_name = str(customer.get("customer", ""))
        customer_orders = grouped_orders.get(customer_name, [])
        customer_primary_key = get_customer_primary_key(customer_orders)
        crm_activities = crm_activity_map.get(customer_primary_key, [])
        sales_activities = get_sales_outreach_activities(crm_activities)
        outbound_sales = [
            activity for activity in sales_activities
            if str(activity.get("direction") or "").strip().lower() == "outbound"
        ]
        inbound_sales = [
            activity for activity in sales_activities
            if str(activity.get("direction") or "").strip().lower() == "inbound"
        ]
        top_contacts = build_outreach_contact_signals(
            sales_activities,
            contacts_by_email=contacts_by_email,
            customer_contacts=contacts_by_customer_key.get(customer_primary_key, []),
        )
        suggested_mode = determine_home_queue_mode(
            customer,
            outbound_count=len(outbound_sales),
            inbound_count=len(inbound_sales),
        )
        summaries.append({
            "customer": customer_name,
            "location": extract_customer_location_label(customer_name, customer_orders),
            "priority_score": customer.get("priority_score"),
            "days_since_last": customer.get("days_since_last"),
            "avg_gap": customer.get("avg_gap"),
            "last_order": customer.get("last_order"),
            "last_activity_date": customer.get("last_activity_date"),
            "action": customer.get("action"),
            "suggested_mode": suggested_mode,
            "next_action_label": get_home_next_action_label(suggested_mode),
            "why_now": build_home_why_now(
                customer,
                outbound_count=len(outbound_sales),
                inbound_count=len(inbound_sales),
            ),
            "top_contact": top_contacts[0] if top_contacts else None,
        })

    return summaries


def determine_home_queue_mode(customer, outbound_count=0, inbound_count=0):
    if has_recent_activity(customer.get("days_since_last_activity")):
        return "Hold"
    if outbound_count >= 3 and inbound_count == 0:
        return "Call"
    return "Email"


def get_home_next_action_label(suggested_mode):
    normalized = str(suggested_mode or "").strip().lower()

    if normalized == "call":
        return "Prepare Call"
    if normalized == "visit":
        return "Prepare Visit"
    if normalized == "hold":
        return "Review Context"
    return "Prepare Email"


def build_home_why_now(customer, outbound_count=0, inbound_count=0):
    days_since_last = customer.get("days_since_last")

    if inbound_count:
        return f"Reply history exists • {days_since_last} days since last order"
    if outbound_count >= 3:
        return f"Multiple outbound touches with no reply • {days_since_last} days since last order"
    if outbound_count == 0:
        return f"No outreach history • {days_since_last} days since last order"
    return f"Beyond usual cycle • {days_since_last} days since last order"


def extract_customer_location_label(customer_name, customer_orders=None):
    customer_name = str(customer_name or "").strip()
    match = re.search(r"\(([^()]+)\)\s*$", customer_name)

    if match:
        return match.group(1).strip()

    customer_orders = customer_orders or []
    state = get_order_state(customer_orders[-1]) if customer_orders else ""
    territory = get_customer_territory(customer_orders) if customer_orders else ""

    if state and territory:
        return f"{territory}, {state}"
    if state:
        return str(state)
    if territory:
        return str(territory)
    return "Location not recorded"


def build_home_preview_payload(selected_customer, queue_summaries, grouped_orders, attention_customers, crm_result):
    if not selected_customer:
        return None

    queue_summary = next(
        (
            item for item in queue_summaries
            if str(item.get("customer", "")) == str(selected_customer)
        ),
        None,
    )

    if not queue_summary:
        return None

    customer_orders = grouped_orders.get(selected_customer, [])
    if not customer_orders:
        return None

    attention_lookup = {
        str(item.get("customer", "")): item
        for item in attention_customers
    }
    attention = attention_lookup.get(selected_customer)
    context = build_outreach_context(selected_customer, customer_orders, attention, crm_result)
    result = build_outreach_prep_fallback(context)
    target_contact_name = str(result.get("recommended_contact_name") or "").strip()
    target_contact_email = str(result.get("recommended_contact_email") or "").strip()
    primary_contact = context.get("primary_contact") or {}
    target_role = (
        str(primary_contact.get("title") or "").strip()
        or str(primary_contact.get("role") or "").strip()
        or "Likely sales contact"
    )
    if not target_contact_name:
        target_contact_name = str(primary_contact.get("name") or "").strip() or "Best known contact"
    if not target_contact_email:
        target_contact_email = str(primary_contact.get("email") or "").strip()
    message_points = build_home_message_points(
        context=context,
        result=result,
        customer_name=selected_customer,
    )

    return {
        "customer": selected_customer,
        "location": queue_summary.get("location") or extract_customer_location_label(selected_customer, customer_orders),
        "queue_summary": queue_summary,
        "context": context,
        "result": result,
        "target_name": target_contact_name,
        "target_email": target_contact_email,
        "target_role": target_role,
        "message_points": message_points,
        "mode": str(result.get("suggested_mode") or queue_summary.get("suggested_mode") or "Email"),
        "mode_cta": get_home_next_action_label(
            str(result.get("suggested_mode") or queue_summary.get("suggested_mode") or "Email")
        ),
    }


def build_home_message_points(context, result, customer_name):
    points = []
    latest_subject = str(context.get("latest_sales_outreach_subject") or "").strip()
    last_reply_date = str(result.get("last_reply_date") or "").strip()
    average_cycle = context.get("average_cycle")
    days_since_last_order = context.get("days_since_last_order")
    primary_contact = context.get("primary_contact") or {}
    contact_name = str(primary_contact.get("name") or "").strip()
    target_name = str(result.get("recommended_contact_name") or "").strip() or contact_name

    if latest_subject:
        points.append(
            f"Acknowledge the latest sales contact about {latest_subject.lower()} and reconnect it to current repair needs."
        )
    elif target_name:
        points.append(
            f"Open by referencing the last conversation with {target_name} and checking whether repair priorities have shifted."
        )
    else:
        points.append(
            f"Open with a brief check-in on whether {customer_name} has any damaged mats or repair needs coming up."
        )

    if contact_name:
        points.append(
            f"Ask {contact_name} whether mats are currently being set aside for repair or if a manager decision is still needed."
        )
    else:
        points.append(
            "Ask whether mats are currently being set aside for repair or whether the customer has paused the program for a specific reason."
        )

    if average_cycle and days_since_last_order:
        points.append(
            f"Use the gap from the usual {average_cycle}-day cycle and the current {days_since_last_order}-day pause to frame why you are checking in now."
        )
    else:
        points.append(
            "Give a simple commercial reason for the outreach and suggest an easy next step such as a quick call, update, or visit."
        )

    return points[:3]


def render_home_focus_preview(preview, dismiss_open=False):
    if not preview:
        return """
            <section class="panel home-focus-panel" id="focus-preview">
                <p class="muted">No active action-plan item is selected.</p>
            </section>
        """

    customer_name = str(preview.get("customer", ""))
    queue_summary = preview.get("queue_summary", {})
    context = preview.get("context", {})
    result = preview.get("result", {})
    mode = str(preview.get("mode") or "Email")
    mode_cta = str(preview.get("mode_cta") or "Open Outreach Prep")
    dismiss_panel_id = "home-dismiss-" + re.sub(r"[^a-z0-9]+", "-", customer_name.lower()).strip("-")
    message_points = "".join(
        f"<li>{escape(point)}</li>"
        for point in preview.get("message_points", [])
    ) or "<li>No message guidance available yet.</li>"
    dismiss_form = f"""
        <button
            type="button"
            class="button secondary home-preview-dismiss-button"
            onclick="document.getElementById('{dismiss_panel_id}').hidden = false;"
        >Dismiss / Not Now</button>
    """
    dismiss_panel = f"""
        <section class="home-dismiss-panel" id="{dismiss_panel_id}" hidden>
            <form method="post" action="/action-plan-dismiss" class="home-dismiss-form">
                <input type="hidden" name="customer" value="{escape(customer_name)}">
                <input type="hidden" name="last_order" value="{escape(str(queue_summary.get('last_order', '')))}">
                <input type="hidden" name="return_to" value="/#home-queue">
                <div class="home-dismiss-presets">
                    <button type="submit" name="reason_preset" value="No damaged mats ready" class="home-dismiss-chip">No damaged mats ready</button>
                    <button type="submit" name="reason_preset" value="Waiting on manager decision" class="home-dismiss-chip">Waiting on manager decision</button>
                    <button type="submit" name="reason_preset" value="Customer asked us to wait" class="home-dismiss-chip">Customer asked us to wait</button>
                    <button type="submit" name="reason_preset" value="Recent visit / no follow-up yet" class="home-dismiss-chip">Recent visit / no follow-up yet</button>
                    <button type="submit" name="reason_preset" value="Pricing under review" class="home-dismiss-chip">Pricing under review</button>
                </div>
                <div class="home-dismiss-actions">
                    <input
                        type="text"
                        name="reason"
                        class="home-dismiss-input"
                        placeholder="Add a reason for dismissing urgency"
                    >
                    <button type="submit" class="button home-dismiss-confirm">Confirm Dismiss</button>
                    <button
                        type="button"
                        class="button secondary home-dismiss-cancel"
                        onclick="document.getElementById('{dismiss_panel_id}').hidden = true;"
                    >Cancel</button>
                </div>
            </form>
        </section>
    """

    return f"""
        <section class="panel home-focus-panel" id="focus-preview">
            <div class="home-focus-head">
                <div class="home-focus-title-block">
                    <h2 class="home-focus-title">{escape(customer_name)}</h2>
                    <p class="home-focus-location">{escape(str(preview.get("location") or "Location not recorded"))}</p>
                </div>
                {dismiss_form}
            </div>
            {dismiss_panel}

            <div class="home-focus-grid">
                <section class="home-focus-column">
                    <div class="home-step-head">
                        <span class="home-step-number">1</span>
                        <div>
                            <span class="home-step-title">Target</span>
                            <span class="home-step-subtitle">Who to contact</span>
                        </div>
                    </div>
                    <div class="home-focus-card">
                        <strong>{escape(str(preview.get("target_name") or "Best known contact"))}</strong>
                        <span class="home-focus-card-meta">{escape(str(preview.get("target_email") or "Email not recorded"))}</span>
                        <span class="home-focus-badge">Primary</span>
                        <span class="home-focus-card-note">{escape(str(preview.get("target_role") or "Likely sales contact"))}</span>
                    </div>
                </section>

                <section class="home-focus-column">
                    <div class="home-step-head">
                        <span class="home-step-number">2</span>
                        <div>
                            <span class="home-step-title">Message</span>
                            <span class="home-step-subtitle">What to say</span>
                        </div>
                    </div>
                    <div class="home-focus-card">
                        <ul class="home-message-points">{message_points}</ul>
                        <div class="home-subject-line">
                            <span class="label">Suggested subject</span>
                            <strong>{escape(str(result.get("email_subject") or "Not available"))}</strong>
                        </div>
                    </div>
                </section>

                <section class="home-focus-column">
                    <div class="home-step-head">
                        <span class="home-step-number">3</span>
                        <div>
                            <span class="home-step-title">Mode</span>
                            <span class="home-step-subtitle">How to contact</span>
                        </div>
                    </div>
                    <div class="home-focus-card">
                        <strong>{escape(mode)}</strong>
                        <span class="home-focus-card-meta">Confidence: {escape(str(result.get("confidence") or "Not available"))}</span>
                        <a class="button home-mode-cta" href="/outreach-prep?customer={quote(customer_name)}">
                            <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                <path fill="currentColor" d="M4 6.5h16A1.5 1.5 0 0 1 21.5 8v8A1.5 1.5 0 0 1 20 17.5H4A1.5 1.5 0 0 1 2.5 16V8A1.5 1.5 0 0 1 4 6.5Zm0 1a.5.5 0 0 0-.34.13L12 13.9l8.34-6.27A.5.5 0 0 0 20 7.5H4Zm16 9a.5.5 0 0 0 .5-.5V8.63l-7.9 5.94a1 1 0 0 1-1.2 0L3.5 8.63V16a.5.5 0 0 0 .5.5H20Z"/>
                            </svg>
                            <span>{escape(mode_cta)}</span>
                        </a>
                    </div>
                </section>
            </div>

            <div class="home-evidence-strip">
                <div><span class="label">Days Since Last Order</span><strong>{escape(str(context.get("days_since_last_order") or "n/a"))}</strong></div>
                <div><span class="label">Average Cycle</span><strong>{escape(str(context.get("average_cycle") or "n/a"))}</strong></div>
                <div><span class="label">Last Order Date</span><strong>{escape(str(context.get("last_order_date") or "n/a"))}</strong></div>
                <div><span class="label">Sales Outreach History</span><strong>{escape(str(context.get("sales_activity_count") or 0))}</strong></div>
                <div><span class="label">Last Reply</span><strong>{escape(str(result.get("last_reply_date") or "No reply recorded"))}</strong></div>
                <div><span class="label">Observed Replies</span><strong>{escape(str(result.get("observed_reply_count") or 0))}</strong></div>
                <div><span class="label">Evidence Strength</span><strong>{escape(str(result.get("evidence_strength") or "Not available"))}</strong></div>
            </div>
        </section>
    """


def render_home_queue_row(summary, is_selected=False):
    customer_name = str(summary.get("customer", ""))
    select_href = build_home_selection_href(
        selected_customer=customer_name,
        view="focus",
        anchor="focus-preview",
    )
    customer_href = f"/customer-view?customer={quote(customer_name)}"
    selected_class = " class=\"queue-selected\"" if is_selected else ""
    mode_label = str(summary.get("suggested_mode") or "Email")
    mode_class = f"mode-chip mode-{mode_label.strip().lower()}"
    mode_markup = render_queue_mode_chip(mode_label)

    return f"""
        <tr{selected_class}>
            <td>
                <div class="queue-customer-cell">
                    <a class="queue-focus-link" href="{select_href}"><strong>{escape(customer_name)}</strong></a>
                    <a class="queue-secondary-link" href="{customer_href}" title="Open customer record" aria-label="Open customer record">
                        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                            <path fill="currentColor" d="M12 12.5a4.25 4.25 0 1 1 0-8.5 4.25 4.25 0 0 1 0 8.5Zm0-1.5a2.75 2.75 0 1 0 0-5.5 2.75 2.75 0 0 0 0 5.5Zm0 3c3.78 0 6.9 1.94 7.82 4.77a.75.75 0 0 1-1.43.46C17.67 17.1 15.08 15.5 12 15.5s-5.67 1.6-6.39 3.73a.75.75 0 1 1-1.43-.46C5.1 15.94 8.22 14 12 14Z"/>
                        </svg>
                    </a>
                </div>
            </td>
            <td>{escape(str(summary.get("location") or ""))}</td>
            <td><span class="{mode_class}">{mode_markup}</span></td>
            <td><a class="queue-action-link" href="/outreach-prep?customer={quote(customer_name)}">{escape(str(summary.get("next_action_label") or "Open Outreach Prep"))}</a></td>
            <td>{escape(str(summary.get("why_now") or ""))}</td>
        </tr>
    """


def render_queue_mode_chip(mode_label):
    normalized = str(mode_label or "").strip().lower()

    if normalized == "email":
        return (
            '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
            '<path fill="currentColor" d="M4 6.5h16A1.5 1.5 0 0 1 21.5 8v8A1.5 1.5 0 0 1 20 17.5H4A1.5 1.5 0 0 1 2.5 16V8A1.5 1.5 0 0 1 4 6.5Zm0 1a.5.5 0 0 0-.34.13L12 13.9l8.34-6.27A.5.5 0 0 0 20 7.5H4Zm16 9a.5.5 0 0 0 .5-.5V8.63l-7.9 5.94a1 1 0 0 1-1.2 0L3.5 8.63V16a.5.5 0 0 0 .5.5H20Z"/>'
            '</svg>'
        )
    if normalized == "call":
        return (
            '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
            '<path fill="currentColor" d="M7.2 3.5h2.1c.5 0 .9.4 1 .8l.5 3.1c.1.4-.1.8-.4 1l-1.5 1.2a14.2 14.2 0 0 0 5.5 5.5l1.2-1.5c.2-.3.6-.5 1-.4l3.1.5c.5.1.8.5.8 1v2.1c0 .6-.5 1.1-1.1 1.1C10 19.9 4.1 14 4.1 4.6c0-.6.5-1.1 1.1-1.1Z"/>'
            '</svg>'
        )
    if normalized == "visit":
        return (
            '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
            '<path fill="currentColor" d="M12 2.8a6.2 6.2 0 0 1 6.2 6.2c0 4.4-4.8 10.2-5.4 10.9a1 1 0 0 1-1.6 0C10.6 19.2 5.8 13.4 5.8 9A6.2 6.2 0 0 1 12 2.8Zm0 8.6A2.4 2.4 0 1 0 12 6.6a2.4 2.4 0 0 0 0 4.8Z"/>'
            '</svg>'
        )
    if normalized == "hold":
        return (
            '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
            '<path fill="currentColor" d="M12 2.8A9.2 9.2 0 1 1 2.8 12 9.2 9.2 0 0 1 12 2.8Zm0 1.5A7.7 7.7 0 1 0 19.7 12 7.7 7.7 0 0 0 12 4.3Zm-2 3.4c.4 0 .8.3.8.8v7a.8.8 0 0 1-1.6 0v-7c0-.5.4-.8.8-.8Zm4 0c.4 0 .8.3.8.8v7a.8.8 0 0 1-1.6 0v-7c0-.5.4-.8.8-.8Z"/>'
            '</svg>'
        )

    return escape(mode_label)


def render_action_plan_item(customer):
    customer_name = str(customer.get("customer", ""))
    customer_url = f"/customer-view?customer={quote(customer_name)}"
    summary_bits = [
        f"Priority {customer.get('priority_score')}",
        f"{customer.get('days_since_last')} days since last order",
        f"Avg cycle {customer.get('avg_gap')} days",
    ]

    if customer.get("last_activity_date"):
        summary_bits.append(
            f"Last activity {customer.get('last_activity_date')}"
        )
    cue = build_action_outreach_cue(customer)

    return f"""
        <li>
            <div class="action-item">
                <a class="action-main" href="{customer_url}">
                    <strong>{escape(customer_name)}</strong>
                    <span class="action-meta">{escape(' • '.join(summary_bits))}</span>
                    <span class="action-cue">{escape(cue)}</span>
                </a>
                <div class="action-side">
                    <span class="score">{escape(str(customer.get("priority_score", "")))}</span>
                    <a class="button secondary small-button action-prep-button" href="/outreach-prep?customer={quote(customer_name)}">Prepare outreach</a>
                </div>
            </div>
            <form method="post" action="/action-plan-dismiss" class="action-dismiss-row">
                <input type="hidden" name="customer" value="{escape(customer_name)}">
                <input type="hidden" name="last_order" value="{escape(str(customer.get('last_order', '')))}">
                <input type="hidden" name="return_to" value="/#action-plan">
                <button type="submit" class="button secondary small-button action-dismiss-button">Dismiss</button>
                <input
                    type="text"
                    name="reason"
                    class="dismiss-reason-input dismiss-reason-wide"
                    placeholder="Reason for dismissing urgency"
                    value="{escape(str(customer.get('dismiss_reason', '')))}"
                >
            </form>
        </li>
    """


def build_action_outreach_cue(customer):
    priority = float(customer.get("priority_score") or 0)
    if has_recent_activity(customer.get("days_since_last_activity")):
        return "Read the latest contact first before sending anything new."
    if priority >= 2:
        return "Reach out today with a direct operational reason and short subject line."
    if priority >= 1.5:
        return "Send a brief follow-up and reference the latest contact or last order."
    return "Keep this one warm and check again soon if no new order lands."


def render_dismissed_action_plan_items(
    customers,
    return_to="/#action-plan",
    section_id="dismissed-urgency",
    summary_label="Dismissed urgency items",
):
    if not customers:
        return ""

    rows = "".join(
        render_dismissed_action_plan_item(customer, return_to=return_to)
        for customer in customers
    )

    return f"""
        <details class="dismissed-panel" id="{escape(section_id)}">
            <summary>{escape(summary_label)} ({len(customers)})</summary>
            <ul class="dismissed-list">{rows}</ul>
        </details>
    """


def render_dismissed_action_plan_item(customer, return_to="/#action-plan"):
    customer_name = str(customer.get("customer", ""))
    customer_url = f"/customer-view?customer={quote(customer_name)}"
    summary_bits = [
        f"Priority {customer.get('priority_score')}",
        f"{customer.get('days_since_last')} days since last order",
        f"Last order {customer.get('last_order')}",
    ]
    dismiss_reason = str(customer.get("dismiss_reason") or "").strip()

    return f"""
        <li>
            <div class="action-item dismissed-action-item">
                <a class="action-main" href="{customer_url}">
                    <strong>{escape(customer_name)}</strong>
                    <span class="action-meta">{escape(' • '.join(summary_bits))}</span>
                    {f'<span class="action-cue">Dismissed reason: {escape(dismiss_reason)}</span>' if dismiss_reason else ''}
                </a>
                <form method="post" action="/action-plan-restore" class="action-dismiss-form">
                    <input type="hidden" name="customer" value="{escape(customer_name)}">
                    <input type="hidden" name="return_to" value="{escape(return_to)}">
                    <button type="submit" class="button secondary small-button action-dismiss-button">Restore</button>
                </form>
            </div>
        </li>
    """


def render_recent_contact_holds(customers):
    rows = "".join(
        render_recent_contact_hold(customer)
        for customer in customers
    )

    if not rows:
        rows = "<li class='empty-action'>No recently contacted customers in the attention queue.</li>"

    return f"""
        <section class="panel hold-panel">
            <div class="panel-head">
                <div>
                    <h2>Hold / Recently Contacted</h2>
                    <p class="muted">Customers that still need attention, but already show recent activity so reps can avoid duplicate follow-up.</p>
                </div>
            </div>
            <ul class="hold-list">{rows}</ul>
        </section>
    """


def render_recent_contact_hold(customer):
    customer_name = str(customer.get("customer", ""))
    customer_url = f"/customer-view?customer={quote(customer_name)}"
    summary = format_activity_summary(customer)

    return f"""
        <li>
            <a class="hold-item" href="{customer_url}">
                <div>
                    <strong>{escape(customer_name)}</strong>
                    <span class="action-meta">{escape(summary)}</span>
                </div>
                <span class="badge">Review latest contact</span>
            </a>
        </li>
    """


def render_sample_data_page(upload_result=None):
    validation = validate_sample_csv_path()
    current_path = get_sample_csv_path()
    active_result = get_orders_for_analysis()
    status = upload_result or validation

    message = ""
    if upload_result:
        if upload_result.get("saved"):
            message = (
                "<p class='status success'>Sample CSV uploaded and activated.</p>"
            )
        else:
            message = (
                "<p class='status error'>Upload was not saved. "
                "Fix the issues below and try again.</p>"
            )

    body = f"""
        {message}

        {render_data_availability_banner(order_result, active_result)}

        <div class="summary">
            <div>
                <span class="label">Current Source</span>
                <strong>{escape(active_result["source"])}</strong>
            </div>
            <div>
                <span class="label">CSV Path</span>
                <strong class="path-value">{escape(str(current_path))}</strong>
            </div>
            <div>
                <span class="label">Rows</span>
                <strong>{validation["row_count"]}</strong>
            </div>
            <div>
                <span class="label">Customers</span>
                <strong>{validation["customer_count"]}</strong>
            </div>
            <div>
                <span class="label">Suggested Max</span>
                <strong>{MAX_SAMPLE_ROWS:,}</strong>
            </div>
        </div>

        <section class="panel">
            <h2>Upload Sample CSV</h2>
            <p class="muted">
                Upload a FileMaker CSV export to replace the active sample data.
                Required columns are customer name, creation timestamp, and repair value.
            </p>
            <form
                class="upload-form"
                method="post"
                action="/sample-data"
                enctype="multipart/form-data"
            >
                <input type="file" name="file" accept=".csv,text/csv" required>
                <button type="submit">Upload CSV</button>
            </form>
        </section>

        {render_validation_panel("Current File Check", validation)}
        {render_validation_panel("Upload Result", status) if upload_result else ""}
    """

    return render_page(title="Sample Data", body=body)


def render_crm_data_page(upload_result=None, sync_result=None):
    active_result = fetch_crm_activities()
    validation = (
        build_crm_validation_from_result(active_result)
        if get_crm_data_source() == "filemaker"
        else validate_crm_csv_path()
    )
    current_path = get_crm_sample_csv_path()
    uploaded_path = get_uploaded_crm_csv_path()
    sync_cache_path = get_filemaker_crm_cache_path()
    background_sync_status = get_crm_sync_status()
    order_result = get_orders_for_analysis()
    status = sync_result or upload_result or validation
    order_keys = {
        str(get_order_primary_key(order) or "").strip()
        for order in order_result.get("orders", [])
        if str(get_order_primary_key(order) or "").strip()
    }
    matched_activities = [
        activity for activity in active_result.get("activities", [])
        if activity.get("customer_primary_key") in order_keys
    ]
    matched_customers = {
        activity.get("customer_primary_key")
        for activity in matched_activities
        if activity.get("customer_primary_key")
    }
    unknown_direction_count = sum(
        1
        for activity in active_result.get("activities", [])
        if activity.get("direction") == "unknown"
    )
    crm_source = str(active_result.get("source", "")).strip().lower()
    full_sync_enabled = should_enable_full_crm_sync()
    using_filemaker_crm = crm_source in {
        "filemaker",
        "filemaker_recent_cache",
        "filemaker_sync_cache",
    }
    active_location_label = (
        "Active FileMaker Layout" if using_filemaker_crm else "Active CSV Path"
    )
    active_location_value = (
        active_result.get("path")
        if using_filemaker_crm
        else str(current_path)
    )
    upload_summary = (
        f"""
            <div>
                <span class="label">Upload Path</span>
                <strong class="path-value">{escape(str(uploaded_path))}</strong>
            </div>
        """
        if not using_filemaker_crm
        else ""
    )
    source_note = (
        "Recent live FileMaker email history is active for the hosted preview. CSV upload remains available as a fallback."
        if crm_source in {"filemaker", "filemaker_recent_cache"}
        else (
            "Synced full FileMaker CRM cache is active."
            if crm_source == "filemaker_sync_cache"
            else "Uploaded CRM CSV data is active."
        )
    )

    message = ""
    if sync_result:
        if sync_result.get("status") in ["started", "running"]:
            message = (
                "<p class='status'>Full CRM sync is running in the background. "
                "You can refresh this page in a minute or two to check progress.</p>"
            )
        elif sync_result.get("saved"):
            message = (
                "<p class='status success'>Full CRM sync completed and the local cache was refreshed.</p>"
            )
        else:
            message = (
                "<p class='status error'>Full CRM sync did not complete. "
                "Check the details below and try again.</p>"
            )
    elif upload_result:
        if upload_result.get("saved"):
            message = (
                "<p class='status success'>CRM CSV uploaded and activated.</p>"
            )
        else:
            message = (
                "<p class='status error'>Upload was not saved. "
                "Fix the issues below and try again.</p>"
            )

    if not message and background_sync_status.get("running"):
        message = (
            "<p class='status'>Full CRM sync is running in the background. "
            "You can keep using the app while it finishes.</p>"
        )

    sync_status_panel = render_crm_sync_status_panel(background_sync_status) if full_sync_enabled else ""
    sync_section = (
        f"""
        <section class="panel">
            <h2>Sync Full FileMaker CRM</h2>
            <p class="muted">
                Pull the full email table from FileMaker in batches, normalize it,
                and save a fast local cache for the app to use.
            </p>
            <form class="upload-form" method="post" action="/crm-sync-full">
                <button type="submit" {"disabled" if background_sync_status.get("running") else ""}>
                    {"Sync Running..." if background_sync_status.get("running") else "Sync Full CRM from FileMaker"}
                </button>
            </form>
        </section>
        """
        if full_sync_enabled else
        """
        <section class="panel">
            <h2>Live CRM Preview Mode</h2>
            <p class="muted">
                The hosted preview is currently using recent live CRM history from FileMaker.
                Full CRM sync is disabled here to keep the preview stable for sales reps.
            </p>
        </section>
        """
    )

    body = f"""
        {message}

        <div class="summary">
            <div>
                <span class="label">Current Source</span>
                <strong>{escape(active_result["source"])}</strong>
            </div>
            <div>
                <span class="label">{active_location_label}</span>
                <strong class="path-value">{escape(str(active_location_value or ""))}</strong>
            </div>
            {upload_summary}
            <div>
                <span class="label">Sync Cache Path</span>
                <strong class="path-value">{escape(str(sync_cache_path))}</strong>
            </div>
            <div>
                <span class="label">Last Synced</span>
                <strong>{escape(active_result.get("synced_at", "") or "Not synced yet")}</strong>
            </div>
            <div>
                <span class="label">Rows</span>
                <strong>{validation["row_count"]}</strong>
            </div>
            <div>
                <span class="label">Non-internal Rows</span>
                <strong>{validation.get("usable_count", 0)}</strong>
            </div>
            <div>
                <span class="label">Customers</span>
                <strong>{validation["customer_count"]}</strong>
            </div>
            <div>
                <span class="label">Internal Only Removed</span>
                <strong>{validation.get("excluded_internal_only", 0)}</strong>
            </div>
            <div>
                <span class="label">Matched to Orders</span>
                <strong>{len(matched_activities)}</strong>
            </div>
            <div>
                <span class="label">Matched Customers</span>
                <strong>{len(matched_customers)}</strong>
            </div>
            <div>
                <span class="label">Unknown Direction</span>
                <strong>{unknown_direction_count}</strong>
            </div>
            <div>
                <span class="label">Suggested Max</span>
                <strong>{MAX_CRM_SAMPLE_ROWS:,}</strong>
            </div>
        </div>

        {sync_section}

        {sync_status_panel}

        <section class="panel">
            <h2>Upload CRM CSV</h2>
            <p class="muted">
                {escape(source_note)}
            </p>
            <p class="muted">
                Upload a CRM email export to replace the active CRM sample data.
                Required columns are created date, body, sender email, recipient,
                and both company primary key / company name pairs.
            </p>
            <form
                class="upload-form"
                method="post"
                action="/crm-data"
                enctype="multipart/form-data"
            >
                <input type="file" name="file" accept=".csv,text/csv" required>
                <button type="submit">Upload CRM CSV</button>
            </form>
        </section>

        {render_validation_panel("Current File Check", validation)}
        {render_validation_panel("Sync Result", status) if sync_result else ""}
        {render_validation_panel("Upload Result", status) if upload_result else ""}
    """

    return render_page(title="CRM Data", body=body)


def render_crm_sync_status_panel(sync_status):
    if not sync_status.get("status") and not sync_status.get("running"):
        return ""

    tone = "success" if sync_status.get("saved") else ("error" if sync_status.get("status") == "error" else "")
    started_at = sync_status.get("started_at") or "Not started"
    finished_at = sync_status.get("finished_at") or ("In progress" if sync_status.get("running") else "Not finished")
    state_label = "Running" if sync_status.get("running") else (
        "Completed" if sync_status.get("saved") else sync_status.get("status", "Idle").replace("_", " ").title()
    )

    return f"""
        <section class="panel">
            <h2>Sync Status</h2>
            <div class="summary">
                <div>
                    <span class="label">State</span>
                    <strong>{escape(state_label)}</strong>
                </div>
                <div>
                    <span class="label">Started</span>
                    <strong>{escape(started_at)}</strong>
                </div>
                <div>
                    <span class="label">Finished</span>
                    <strong>{escape(finished_at)}</strong>
                </div>
            </div>
            <p class="status {tone}">{escape(sync_status.get("message") or "No sync activity yet.")}</p>
        </section>
    """


def build_crm_validation_from_result(result):
    status = result.get("status", "unknown")
    counts = result.get("counts", {})
    path = result.get("path", "")

    return {
        "valid": status == "ok",
        "status": status,
        "path": path,
        "row_count": counts.get("total_rows", 0),
        "customer_count": counts.get("customer_count", 0),
        "usable_count": counts.get("kept_rows", 0),
        "excluded_internal_only": counts.get("excluded_internal_only", 0),
        "warnings": [],
        "errors": [] if status == "ok" else [
            f"CRM source returned status: {status}"
        ],
    }


def render_validation_panel(title, validation):
    warnings = validation.get("warnings", [])
    errors = validation.get("errors", [])

    warning_items = "".join(
        f"<li>{escape(warning)}</li>"
        for warning in warnings
    )
    error_items = "".join(
        f"<li>{escape(error)}</li>"
        for error in errors
    )

    if not warning_items:
        warning_items = "<li>None</li>"

    if not error_items:
        error_items = "<li>None</li>"

    return f"""
        <section class="panel">
            <h2>{escape(title)}</h2>
            <div class="summary compact-summary">
                <div>
                    <span class="label">Status</span>
                    <strong>{escape(str(validation.get("status", "unknown")))}</strong>
                </div>
                <div>
                    <span class="label">Valid</span>
                    <strong>{escape(str(validation.get("valid", False)))}</strong>
                </div>
                <div>
                    <span class="label">Rows</span>
                    <strong>{validation.get("row_count", 0)}</strong>
                </div>
                <div>
                    <span class="label">Customers</span>
                    <strong>{validation.get("customer_count", 0)}</strong>
                </div>
            </div>

            <div class="validation-grid">
                <div>
                    <h3>Warnings</h3>
                    <ul>{warning_items}</ul>
                </div>
                <div>
                    <h3>Errors</h3>
                    <ul>{error_items}</ul>
                </div>
            </div>
        </section>
    """


def render_dashboard_context(source):
    analysis_today = get_analysis_today().strftime("%m/%d/%Y")
    refreshed_at = datetime.now().strftime("%m/%d/%Y %H:%M")

    return f"""
        <p class="context-note">
            Data source: <strong>{escape(source)}</strong>.
            Review date: <strong>{analysis_today}</strong>.
            Updated: <strong>{refreshed_at}</strong>.
        </p>
    """


def render_data_availability_banner(*results):
    warnings = []

    for result in results:
        warning = str((result or {}).get("warning", "") or "").strip()

        if warning and warning not in warnings:
            warnings.append(warning)

    if not warnings:
        return ""

    items = "".join(f"<li>{escape(warning)}</li>" for warning in warnings)
    return f"""
        <section class="panel">
            <p class="status">{' '.join(escape(warning) for warning in warnings)}</p>
            <ul class="muted">{items}</ul>
        </section>
    """


def parse_iso_date(value):
    if not value:
        return None

    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except ValueError:
        return None


def render_orders_filter_form(customer, status, sort, direction):
    sort_options = {
        "order_date": "Order Date",
        "customer": "Customer",
        "amount": "Amount",
        "order_no": "Order No",
        "status": "Status",
        "state": "State",
        "territory": "Territory",
    }
    direction_options = {
        "desc": "Descending",
        "asc": "Ascending",
    }

    return f"""
        <form class="controls" method="get" action="/orders-view">
            <label>
                <span>Customer</span>
                <input
                    type="search"
                    name="customer"
                    value="{escape(customer)}"
                    placeholder="Search customer"
                >
            </label>

            <label>
                <span>Status</span>
                <input
                    type="search"
                    name="status"
                    value="{escape(status)}"
                    placeholder="Complete"
                >
            </label>

            <label>
                <span>Sort</span>
                <select name="sort">
                    {render_select_options(sort_options, sort)}
                </select>
            </label>

            <label>
                <span>Direction</span>
                <select name="direction">
                    {render_select_options(direction_options, direction)}
                </select>
            </label>

            <button type="submit">Apply</button>
            <a class="button secondary" href="/orders-view">Reset</a>
        </form>
    """


def render_late_customers_filter_form(customer, action, sort, direction):
    sort_options = {
        "priority_score": "Priority Score",
        "days_since_last": "Days Since Last",
        "avg_gap": "Average Gap",
        "customer": "Customer",
        "action": "Action",
        "days_since_last_activity": "Last Activity",
    }
    direction_options = {
        "desc": "Descending",
        "asc": "Ascending",
    }

    return f"""
        <form class="controls compact" method="get" action="/customers-needing-attention-view">
            <label>
                <span>Customer</span>
                <input
                    type="search"
                    name="customer"
                    value="{escape(customer)}"
                    placeholder="Search customer"
                >
            </label>

            <label>
                <span>Action</span>
                <input
                    type="search"
                    name="action"
                    value="{escape(action)}"
                    placeholder="Urgent"
                >
            </label>

            <label>
                <span>Sort</span>
                <select name="sort">
                    {render_select_options(sort_options, sort)}
                </select>
            </label>

            <label>
                <span>Direction</span>
                <select name="direction">
                    {render_select_options(direction_options, direction)}
                </select>
            </label>

            <button type="submit">Apply</button>
            <a class="button secondary" href="/customers-needing-attention-view">Reset</a>
        </form>
    """


def render_customers_filter_form(customer, state, sort, direction):
    sort_options = {
        "last_order": "Last Order",
        "attention": "Attention Priority",
        "customer": "Customer",
        "order_count": "Order Count",
        "crm_count": "CRM Emails",
        "state": "State",
        "avg_cycle": "Average Cycle",
        "total_value": "Total Value",
        "avg_value": "Avg Value / Order",
        "last_activity": "Last Activity",
    }
    direction_options = {
        "desc": "Descending",
        "asc": "Ascending",
    }

    return f"""
        <form class="controls" method="get" action="/customers-view">
            <label>
                <span>Customer</span>
                <input
                    type="search"
                    name="customer"
                    value="{escape(customer)}"
                    placeholder="Search customer"
                >
            </label>

            <label>
                <span>State</span>
                <input
                    type="search"
                    name="state"
                    value="{escape(state)}"
                    placeholder="OH"
                >
            </label>

            <label>
                <span>Sort</span>
                <select name="sort">
                    {render_select_options(sort_options, sort)}
                </select>
            </label>

            <label>
                <span>Direction</span>
                <select name="direction">
                    {render_select_options(direction_options, direction)}
                </select>
            </label>

            <button type="submit">Apply</button>
            <a class="button secondary" href="/customers-view">Reset</a>
        </form>
    """


def render_customer_sort_form(customer, sort, direction, crm_limit, crm_page, crm_direction, crm_category):
    sort_options = {
        "order_date": "Order Date",
        "amount": "Amount",
        "order_no": "Order No",
        "status": "Status",
        "state": "State",
        "territory": "Territory",
        "last_activity": "Last Activity",
    }
    direction_options = {
        "desc": "Descending",
        "asc": "Ascending",
    }

    return f"""
        <form class="controls compact" method="get" action="/customer-view">
            <input type="hidden" name="customer" value="{escape(customer)}">
            <input type="hidden" name="crm_limit" value="{crm_limit}">
            <input type="hidden" name="crm_page" value="{crm_page}">
            <input type="hidden" name="crm_direction" value="{escape(crm_direction)}">
            <input type="hidden" name="crm_category" value="{escape(crm_category)}">

            <label>
                <span>Sort</span>
                <select name="sort">
                    {render_select_options(sort_options, sort)}
                </select>
            </label>

            <label>
                <span>Direction</span>
                <select name="direction">
                    {render_select_options(direction_options, direction)}
                </select>
            </label>

            <button type="submit">Apply</button>
        </form>
    """


def render_select_options(options, selected_value):
    return "".join(
        render_select_option(value, label, selected_value)
        for value, label in options.items()
    )


def render_select_option(value, label, selected_value):
    selected = " selected" if value == selected_value else ""
    return (
        f"<option value=\"{escape(value)}\"{selected}>"
        f"{escape(label)}</option>"
    )


def render_global_nav(title):
    nav_items = [
        ("Home", "/"),
        ("Insights", "/insights-view"),
        ("Orders", "/orders-view"),
        ("Customers Needing Attention", "/customers-needing-attention-view"),
        ("Customers", "/customers-view"),
    ]
    items_html = []

    for label, href in nav_items:
        if title == label:
            items_html.append(f"<span class=\"nav-current\">{escape(label)}</span>")
        else:
            items_html.append(f"<a href=\"{href}\">{escape(label)}</a>")

    if title.startswith("Customer: "):
        items_html.append(
            f"<span class=\"nav-current\">{escape(title.replace('Customer: ', '', 1))}</span>"
        )

    admin_menu = """
        <details class="nav-admin-menu">
            <summary>Admin</summary>
            <div class="nav-admin-dropdown">
                <a href="/filemaker-health">FileMaker Health</a>
                <a href="/crm-activities-view">CRM Activities</a>
                <a href="/crm-data">CRM Data</a>
                <a href="/sample-data">Sample Data</a>
                <a href="/api">API</a>
            </div>
        </details>
    """

    return (
        "<div class=\"nav-shell\">"
        "<a class=\"brand-logo\" href=\"/\" aria-label=\"Numat home\">"
        "<svg class=\"brand-logo-svg\" viewBox=\"0 0 300 300\" aria-hidden=\"true\" focusable=\"false\">"
        "<rect fill=\"#003786\" width=\"300\" height=\"300\"/>"
        "<path fill=\"#fff\" d=\"M82.7,142.5c0,9.5,5,13.9,11.3,13.9s10.3-2.8,12.5-5.1v4.3h19.7v-11h-3.6v-19.9h0v-4.4h-19.7v11h3.8v11.2c-.8,1.2-2,2.2-3.9,2.2-4.3,0-4.7-3.3-4.7-8.4s0-7.6,0-10h0v-5.9h-18.6v11h3.3v11.2Z\"/>"
        "<polygon fill=\"#fff\" points=\"28.7 143.1 24.7 143.1 24.7 155.3 42.3 155.3 42.3 143.1 38.9 143.1 38.9 125.2 57.5 155.2 61.5 155.2 71.7 155.2 71.7 149 71.7 116.8 75.4 116.8 75.4 104.6 57.8 104.6 57.8 116.8 61.5 116.8 61.5 131.9 45.2 104.6 24 104.6 24 116.8 28.7 116.8 28.7 143.1\"/>"
        "<polygon fill=\"#fff\" points=\"137.1 155.3 147.3 155.3 147.3 155.3 151 155.3 151 143.1 147.3 143.1 147.3 116.8 148.3 116.8 160.8 155.2 166.7 155.2 169 147.8 169.1 148 180.5 116.8 180.7 116.8 180.7 143.1 177.2 143.1 177.2 155.3 180.7 155.3 180.7 155.3 195.5 155.3 195.5 155.3 199.2 155.3 199.2 143.1 195.5 143.1 195.5 116.8 199.1 116.8 199.1 104.6 173.2 104.6 166.3 123.6 160 104.6 134 104.6 134 116.8 137.1 116.8 137.1 143.1 133.4 143.1 133.4 155.3 137.1 155.3 137.1 155.3\"/>"
        "<path fill=\"#fff\" d=\"M221.7,126.3c2.8,0,5.4,2.5,5.4,4.9s0,4.6,0,4.6c0,0-2.8-2.1-10.8-2.1s-12.9,8.1-12.9,11.6,2.4,11,11.9,11,9.8-1.8,11.7-3.1v2h19.9v-10.8h-4.5v-15c0-2-.9-10.2-18.2-10.2s-21.3,8.6-21.3,8.6l12,2.2s2.4-3.7,6.7-3.7M222.8,148.4c-2.5,0-4.6-2.1-4.6-4.7s2.1-4.7,4.6-4.7,4.6,1.2,4.6,4.7-2,4.7-4.6,4.7\"/>"
        "<path fill=\"#fff\" d=\"M253,133.1v14c0,6.6,6.4,9.7,11.4,9.7s9.9-3,9.9-3v-11.2c-3.9,2.7-6.2-.4-6.2-3.4v-8.2h6.4v-10.8h-6.4v-14.4l-15.6,8.7.2,5.7h-4.2v10.8h4.4v2.2Z\"/>"
        "<polygon fill=\"#fff\" points=\"259.8 170.5 150 209.8 40.2 170.5 23.1 170.5 150 224.8 276.9 170.5 259.8 170.5\"/>"
        "<polygon fill=\"#fff\" points=\"150 191.2 65.4 168.5 48.3 168.5 150 204.1 251.7 168.5 234.6 168.5 150 191.2\"/>"
        "</svg>"
        "</a>"
        "<p class=\"nav\">"
        + "<span>/</span>".join(items_html)
        + "</p>"
        + admin_menu
        + "</div>"
    )


def render_page(title, body, top_right="", show_title=True):
    title_class = "page-title"

    if title == "Numat AI Sales Assistant":
        title_class += " home-page-title"

    page_title_html = (
        f"<h1 class=\"{title_class}\">{escape(title)}</h1>"
        if show_title else ""
    )

    return f"""
        <!doctype html>
        <html lang="en">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>{escape(title)}</title>
                <style>
                    :root {{
                        color-scheme: light;
                        --bg: #fbfdff;
                        --surface: #ffffff;
                        --surface-soft: #fbfdff;
                        --surface-panel: #ffffff;
                        --surface-card: #ffffff;
                        --surface-inset: #f9fbfe;
                        --border: #dde8f3;
                        --border-strong: #bfd0e0;
                        --text: #1f2933;
                        --muted: #5b6b7c;
                        --muted-soft: #7b8794;
                        --blue: #245cff;
                        --blue-dark: #1b46c9;
                        --shadow-soft: 0 14px 34px rgba(15, 23, 42, 0.1);
                        --shadow-panel: 0 16px 36px rgba(15, 23, 42, 0.08);
                        --shadow-card: 0 10px 24px rgba(15, 23, 42, 0.06);
                        --shadow-inset: 0 6px 16px rgba(15, 23, 42, 0.03);
                        --type-body: 13px;
                        --type-label: 12px;
                        --type-card-title: 16px;
                        --type-section-title: 20px;
                        --type-page-title: 30px;
                    }}

                    body {{
                        margin: 0;
                        font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
                        color: var(--text);
                        background: var(--bg);
                    }}

                    main {{
                        max-width: 1180px;
                        margin: 0 auto;
                        padding: 24px 20px 36px;
                    }}

                    h1 {{
                        margin: 0 0 18px;
                        font-size: var(--type-page-title);
                        line-height: 1.1;
                        letter-spacing: 0;
                    }}

                    h2, h3 {{
                        color: var(--blue);
                    }}

                    .home-page-title {{
                        color: var(--blue);
                        font-size: 34px;
                    }}

                    a {{
                        color: var(--blue);
                    }}

                    .top-row {{
                        display: flex;
                        flex-wrap: wrap;
                        justify-content: space-between;
                        align-items: center;
                        gap: 12px;
                        margin: 0 0 14px;
                    }}

                    .nav-shell {{
                        display: flex;
                        flex: 1 1 auto;
                        align-items: center;
                        gap: 14px;
                        min-width: 0;
                    }}

                    .brand-logo {{
                        display: inline-flex;
                        align-items: center;
                        color: var(--blue);
                        text-decoration: none;
                        flex-shrink: 0;
                    }}

                    .brand-logo-svg {{
                        width: 42px;
                        height: 42px;
                        border-radius: 8px;
                        flex-shrink: 0;
                    }}

                    .nav {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                        margin: 0;
                        color: var(--muted-soft);
                        font-size: 13px;
                        align-items: center;
                        min-width: 0;
                    }}

                    .nav a {{
                        color: var(--blue);
                        text-decoration: none;
                    }}

                    .nav-current {{
                        color: #314458;
                        font-weight: 700;
                    }}

                    .nav-admin-menu {{
                        position: relative;
                        margin-left: auto;
                        flex-shrink: 0;
                    }}

                    .nav-admin-menu summary {{
                        list-style: none;
                        cursor: pointer;
                        padding: 6px 10px;
                        border: 1px solid var(--border);
                        border-radius: 6px;
                        background: var(--surface);
                        color: #334e68;
                        font-size: 13px;
                        font-weight: 600;
                    }}

                    .nav-admin-menu summary::-webkit-details-marker {{
                        display: none;
                    }}

                    .nav-admin-menu[open] summary {{
                        border-color: var(--border-strong);
                        box-shadow: var(--shadow-soft);
                    }}

                    .nav-admin-dropdown {{
                        position: absolute;
                        top: calc(100% + 8px);
                        right: 0;
                        min-width: 190px;
                        display: grid;
                        gap: 4px;
                        padding: 8px;
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        background: var(--surface);
                        box-shadow: 0 14px 30px rgba(15, 23, 42, 0.12);
                        z-index: 20;
                    }}

                    .nav-admin-dropdown a {{
                        display: block;
                        padding: 7px 9px;
                        border-radius: 6px;
                        color: #334e68;
                        text-decoration: none;
                    }}

                    .nav-admin-dropdown a:hover {{
                        background: #f2f7fd;
                    }}

                    .hero {{
                        display: grid;
                        grid-template-columns: minmax(0, 1fr) auto;
                        gap: 16px;
                        align-items: end;
                        margin-bottom: 16px;
                    }}

                    .hero h1 {{
                        margin-bottom: 6px;
                    }}

                    .hero p {{
                        max-width: 720px;
                        margin: 0;
                        color: #52606d;
                        font-size: 15px;
                        line-height: 1.45;
                    }}

                    .hero-actions {{
                        display: grid;
                        grid-template-columns: 1fr;
                        gap: 10px;
                        min-width: 240px;
                    }}

                    .hero-actions .button {{
                        height: 36px;
                        padding: 8px 12px;
                        font-size: 14px;
                    }}

                    .eyebrow {{
                        margin: 0 0 6px;
                        color: #1f5f99;
                        font-size: 13px;
                        font-weight: 700;
                        text-transform: uppercase;
                    }}

                    .context-note {{
                        margin: -6px 0 10px;
                        padding: 10px 12px;
                        background: #eef6ff;
                        border: 1px solid #c9def3;
                        border-radius: 8px;
                        color: #334e68;
                        font-size: 14px;
                    }}

                    .home-workspace {{
                        display: grid;
                        gap: 16px;
                    }}

                    .home-view-toggle {{
                        display: inline-flex;
                        align-items: center;
                        gap: 8px;
                        padding: 4px;
                        border: 1px solid var(--border);
                        border-radius: 10px;
                        background: rgba(255, 255, 255, 0.78);
                        box-shadow: var(--shadow-soft);
                        margin-bottom: 4px;
                        width: fit-content;
                    }}

                    .home-view-toggle .toggle-chip {{
                        min-height: 38px;
                        min-width: 86px;
                        padding: 0 16px;
                        border-radius: 8px;
                        border: 1px solid transparent;
                        color: var(--muted);
                        text-decoration: none;
                        font-weight: 600;
                        background: transparent;
                    }}

                    .home-view-toggle .toggle-chip.active {{
                        border-color: #c8daf3;
                        background: #ffffff;
                        color: #1652d1;
                        box-shadow: 0 6px 14px rgba(31, 95, 153, 0.08);
                    }}

                    .home-focus-panel {{
                        padding: 20px 22px 16px;
                        box-shadow: 0 18px 42px rgba(15, 23, 42, 0.12);
                    }}

                    .home-focus-head {{
                        display: flex;
                        justify-content: space-between;
                        align-items: start;
                        gap: 16px;
                        margin-bottom: 18px;
                    }}

                    .panel h2.home-focus-title {{
                        margin: 0 0 6px;
                        color: var(--text);
                        font-size: 44px;
                        font-weight: 800;
                        line-height: 1.1;
                    }}

                    .home-focus-location {{
                        margin: 0;
                        color: var(--muted);
                        font-size: 16px;
                    }}

                    .home-preview-dismiss-form {{
                        margin: 0;
                        flex-shrink: 0;
                    }}

                    .home-preview-dismiss-button {{
                        width: auto;
                        min-height: 44px;
                        min-width: 190px;
                        font-size: 15px;
                        padding: 10px 18px;
                    }}

                    .home-dismiss-panel {{
                        margin: 0 0 18px;
                        padding: 14px 16px;
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        background: rgba(255, 255, 255, 0.98);
                        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
                    }}

                    .home-dismiss-form {{
                        display: grid;
                        gap: 12px;
                    }}

                    .home-dismiss-presets {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                    }}

                    .home-dismiss-chip {{
                        width: auto;
                        min-height: 32px;
                        padding: 0 12px;
                        border: 1px solid var(--border);
                        border-radius: 999px;
                        background: #fff;
                        color: #334e68;
                        font-size: 13px;
                        font-weight: 600;
                    }}

                    .home-dismiss-actions {{
                        display: grid;
                        grid-template-columns: minmax(0, 1fr) auto auto;
                        gap: 10px;
                        align-items: center;
                    }}

                    .home-dismiss-input {{
                        width: 100%;
                        min-width: 0;
                    }}

                    .home-dismiss-confirm,
                    .home-dismiss-cancel {{
                        width: auto;
                        min-width: 140px;
                    }}

                    .home-focus-grid {{
                        display: grid;
                        grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.15fr) minmax(0, 0.95fr);
                        gap: 26px;
                        margin-bottom: 18px;
                    }}

                    .home-focus-column {{
                        position: relative;
                        min-width: 0;
                    }}

                    .home-focus-column:not(:last-child)::after {{
                        content: "";
                        position: absolute;
                        top: 0;
                        right: -14px;
                        bottom: 2px;
                        width: 1px;
                        background: #c0cfdf;
                    }}

                    .home-step-head {{
                        display: flex;
                        align-items: center;
                        gap: 12px;
                        margin-bottom: 8px;
                    }}

                    .home-step-number {{
                        width: 34px;
                        height: 34px;
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        border-radius: 999px;
                        background: #245cff;
                        color: #fff;
                        font-weight: 700;
                        flex-shrink: 0;
                    }}

                    .home-step-title {{
                        display: block;
                        color: #245cff;
                        font-size: 22px;
                        font-weight: 700;
                        line-height: 1.1;
                    }}

                    .home-step-subtitle {{
                        display: block;
                        margin-top: 2px;
                        color: var(--muted);
                        font-size: 14px;
                    }}

                    .home-focus-card {{
                        min-height: 196px;
                        display: grid;
                        align-content: start;
                        gap: 10px;
                        padding: 16px 18px;
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        background: var(--surface-card);
                        box-shadow: var(--shadow-card);
                    }}

                    .home-focus-card strong {{
                        font-size: 16px;
                        line-height: 1.3;
                        overflow-wrap: anywhere;
                    }}

                    .home-focus-card-meta {{
                        display: block;
                        color: var(--muted);
                        font-size: 13px;
                        line-height: 1.35;
                        overflow-wrap: anywhere;
                    }}

                    .home-focus-card-note {{
                        display: block;
                        color: #334e68;
                        font-size: 13px;
                        line-height: 1.4;
                    }}

                    .home-focus-badge {{
                        display: inline-flex;
                        width: fit-content;
                        align-items: center;
                        justify-content: center;
                        min-height: 28px;
                        padding: 0 12px;
                        border-radius: 999px;
                        background: #e7f8eb;
                        color: #1f7a45;
                        font-size: 12px;
                        font-weight: 700;
                    }}

                    .home-message-points {{
                        margin: 0;
                        padding-left: 18px;
                        color: #334e68;
                        font-size: 13px;
                        line-height: 1.4;
                    }}

                    .home-message-points li + li {{
                        margin-top: 6px;
                    }}

                    .home-subject-line {{
                        margin-top: auto;
                        padding-top: 2px;
                    }}

                    .home-mode-cta {{
                        margin-top: auto;
                        width: 100%;
                        min-height: 46px;
                        justify-content: center;
                        font-size: 14px;
                        gap: 12px;
                        border-color: #245cff;
                        background: #245cff;
                    }}

                    .home-mode-cta svg {{
                        width: 28px;
                        height: 28px;
                        flex-shrink: 0;
                        display: block;
                    }}

                    .home-evidence-strip {{
                        display: grid;
                        grid-template-columns:
                            minmax(110px, 0.9fr)
                            minmax(110px, 0.9fr)
                            minmax(120px, 0.95fr)
                            minmax(120px, 0.95fr)
                            minmax(120px, 0.9fr)
                            minmax(110px, 0.9fr)
                            minmax(200px, 1.55fr);
                        gap: 0;
                        border: 1px solid #dbe5f0;
                        border-radius: 12px;
                        overflow: hidden;
                        background: rgba(255, 255, 255, 0.99);
                    }}

                    .home-evidence-strip div {{
                        padding: 14px 16px;
                        border-right: 1px solid #dbe5f0;
                        background: #f9fbfe;
                    }}

                    .home-evidence-strip div:last-child {{
                        border-right: 0;
                    }}

                    .home-evidence-strip div:nth-child(even) {{
                        background: #fcfdff;
                    }}

                    .home-evidence-strip strong {{
                        display: block;
                        font-size: 15px;
                        line-height: 1.3;
                    }}

                    .home-queue-panel {{
                        margin-bottom: 0;
                    }}

                    .home-queue-head {{
                        margin-bottom: 10px;
                    }}

                    .home-queue-wrap {{
                        margin-bottom: 0;
                    }}

                    .queue-table {{
                        min-width: 0;
                    }}

                    .queue-table th:nth-child(3),
                    .queue-table td:nth-child(3),
                    .queue-table th:nth-child(4),
                    .queue-table td:nth-child(4) {{
                        white-space: nowrap;
                    }}

                    .queue-selected {{
                        background: #edf4ff;
                    }}

                    .queue-selected td {{
                        background: #edf4ff;
                    }}

                    .queue-selected td:first-child {{
                        box-shadow: inset 3px 0 0 #265cdb;
                    }}

                    .queue-customer-cell {{
                        display: grid;
                        gap: 6px;
                    }}

                    .queue-focus-link {{
                        color: var(--text);
                        text-decoration: none;
                    }}

                    .queue-focus-link strong {{
                        color: var(--text);
                    }}

                    .queue-focus-link:hover strong,
                    .queue-secondary-link:hover,
                    .queue-action-link:hover {{
                        color: var(--blue);
                    }}

                    .queue-secondary-link {{
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        width: 24px;
                        height: 24px;
                        color: #245cff;
                        text-decoration: none;
                    }}

                    .queue-secondary-link svg {{
                        width: 18px;
                        height: 18px;
                        display: block;
                    }}

                    .queue-action-link {{
                        color: #245cff;
                        font-weight: 600;
                        text-decoration: none;
                    }}

                    .mode-chip {{
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        min-height: 28px;
                        padding: 0 10px;
                        border-radius: 999px;
                        border: 1px solid #d3e2f3;
                        background: #f5f9ff;
                        color: #24527a;
                        font-size: 13px;
                        font-weight: 700;
                    }}

                    .mode-chip svg {{
                        width: 16px;
                        height: 16px;
                        display: block;
                    }}

                    .mode-call {{
                        background: #f7f2ff;
                        border-color: #ddd2fb;
                        color: #5c43a8;
                    }}

                    .mode-hold {{
                        background: #eef5ff;
                        border-color: #cfe0f7;
                        color: #3a5f87;
                    }}

                    .summary {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                        gap: 12px;
                        margin-bottom: 18px;
                    }}

                    .summary div {{
                        background: var(--surface-card);
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        padding: 13px 15px;
                        box-shadow: var(--shadow-card);
                    }}

                    .summary strong {{
                        display: block;
                        font-size: var(--type-card-title);
                        line-height: 1.25;
                        overflow-wrap: anywhere;
                        word-break: break-word;
                    }}

                    .home-summary {{
                        grid-template-columns: repeat(auto-fit, minmax(165px, 1fr));
                        gap: 12px;
                        margin-bottom: 16px;
                    }}

                    .home-summary div {{
                        padding: 11px 14px;
                    }}

                    .compact-summary {{
                        margin-bottom: 14px;
                    }}

                    .customer-summary {{
                        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                        gap: 10px;
                        margin-bottom: 12px;
                    }}

                    .customer-summary div {{
                        padding: 10px 12px;
                    }}

                    .customer-summary strong {{
                        font-size: 15px;
                        line-height: 1.2;
                    }}

                    .label {{
                        display: block;
                        margin-bottom: 4px;
                        color: var(--muted);
                        font-size: var(--type-label);
                    }}

                    .path-value {{
                        font-size: var(--type-body);
                        line-height: 1.35;
                        font-weight: 600;
                    }}

                    .cards {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                        gap: 12px;
                        margin-bottom: 18px;
                    }}

                    .sales-cards {{
                        grid-template-columns: repeat(3, minmax(220px, 1fr));
                    }}

                    .nav-card {{
                        display: block;
                        padding: 14px 16px;
                        background: var(--surface-card);
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        color: inherit;
                        text-decoration: none;
                        box-shadow: var(--shadow-card);
                    }}

                    .nav-card strong {{
                        display: block;
                        margin-bottom: 8px;
                        color: var(--text);
                    }}

                    .nav-card p {{
                        margin: 0;
                        color: var(--muted);
                        font-size: 15px;
                        line-height: 1.35;
                    }}

                    .dashboard-grid {{
                        display: grid;
                        grid-template-columns: repeat(2, minmax(0, 1fr));
                        gap: 16px;
                        margin-bottom: 18px;
                    }}

                    .panel {{
                        margin-bottom: 18px;
                        padding: 18px;
                        background: var(--surface-panel);
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        box-shadow: var(--shadow-panel);
                    }}

                    .panel h2 {{
                        margin: 0 0 6px;
                        font-size: var(--type-section-title);
                    }}

                    .dashboard-grid .panel h2 {{
                        color: var(--text);
                    }}

                    .insights-grid .panel h2,
                    .insights-grid .panel h3 {{
                        color: #1f2937;
                    }}

                    .insights-grid {{
                        grid-template-columns: repeat(3, minmax(0, 1fr));
                        gap: 12px;
                    }}

                    .insights-span-2 {{
                        grid-column: span 2;
                    }}

                    .insights-grid .panel {{
                        padding: 13px 14px;
                        border-radius: 10px;
                    }}

                    .insights-grid .panel h2 {{
                        margin-bottom: 3px;
                        font-size: 14px;
                        line-height: 1.15;
                    }}

                    .insights-grid .muted {{
                        margin-bottom: 10px;
                        font-size: 12px;
                        line-height: 1.35;
                    }}

                    .panel-head {{
                        display: flex;
                        justify-content: space-between;
                        gap: 12px;
                        align-items: start;
                        margin-bottom: 14px;
                    }}

                    .action-plan-head {{
                        justify-content: center;
                        align-items: center;
                        text-align: center;
                    }}

                    .action-plan-head h2 {{
                        font-size: 28px;
                        line-height: 1.1;
                    }}

                    .muted {{
                        margin: 0 0 16px;
                        color: var(--muted);
                        line-height: 1.4;
                    }}

                    .action-columns {{
                        display: grid;
                        grid-template-columns: repeat(2, minmax(0, 1fr));
                        gap: 16px;
                    }}

                    .action-column {{
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        background: var(--surface-soft);
                        padding: 14px;
                    }}

                    .action-column-head {{
                        margin-bottom: 10px;
                    }}

                    .bucket-head {{
                        margin: 10px 0 8px;
                        color: #314458;
                        font-size: 12px;
                        font-weight: 700;
                        text-transform: uppercase;
                        letter-spacing: 0.03em;
                    }}

                    .bucket-head.secondary {{
                        margin-top: 14px;
                        color: var(--muted);
                    }}

                    .action-list,
                    .hold-list {{
                        list-style: none;
                        margin: 0;
                        padding: 0;
                        display: grid;
                        gap: 10px;
                    }}

                    .action-list {{
                        counter-reset: action-rank;
                    }}

                    .action-list li {{
                        counter-increment: action-rank;
                    }}

                    .action-item,
                    .hold-item {{
                        display: flex;
                        justify-content: space-between;
                        gap: 12px;
                        align-items: start;
                        padding: 12px 13px;
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        background: var(--surface);
                        text-decoration: none;
                        color: inherit;
                    }}

                    .action-item::before {{
                        content: counter(action-rank) ".";
                        color: var(--muted-soft);
                        font-weight: 700;
                        margin-right: 2px;
                    }}

                    .action-main {{
                        display: grid;
                        gap: 5px;
                        min-width: 0;
                        flex: 1;
                        color: inherit;
                        text-decoration: none;
                    }}

                    .action-side {{
                        display: grid;
                        justify-items: end;
                        gap: 8px;
                        flex-shrink: 0;
                    }}

                    .action-dismiss-form {{
                        margin: 0;
                        display: grid;
                        gap: 6px;
                        justify-items: end;
                    }}

                    .action-dismiss-row {{
                        margin: 8px 0 0 44px;
                        display: flex;
                        align-items: center;
                        gap: 10px;
                    }}

                    .action-dismiss-button {{
                        min-width: 88px;
                        height: 32px;
                        padding: 6px 10px;
                        font-size: 12px;
                    }}

                    .action-prep-button {{
                        min-width: 138px;
                        height: 32px;
                        padding: 6px 10px;
                        font-size: 12px;
                    }}

                    .dismiss-reason-input {{
                        width: 120px;
                        padding: 6px 8px;
                        font-size: 12px;
                    }}

                    .dismiss-reason-wide {{
                        width: min(420px, 100%);
                        flex: 1 1 280px;
                    }}

                    .action-meta {{
                        display: block;
                        color: var(--muted);
                        font-size: 13px;
                        line-height: 1.4;
                    }}

                    .action-cue {{
                        display: block;
                        color: #314458;
                        font-size: 13px;
                        line-height: 1.4;
                    }}

                    .hold-item {{
                        align-items: center;
                    }}

                    .empty-action {{
                        padding: 12px 13px;
                        border: 1px dashed var(--border-strong);
                        border-radius: 8px;
                        color: var(--muted);
                        background: var(--surface);
                    }}

                    .dismissed-panel {{
                        margin-top: 12px;
                    }}

                    .dismissed-panel summary {{
                        cursor: pointer;
                        color: var(--muted);
                        font-size: 14px;
                        font-weight: 600;
                    }}

                    .dismissed-list {{
                        list-style: none;
                        margin: 10px 0 0;
                        padding: 0;
                        display: grid;
                        gap: 10px;
                    }}

                    .dismissed-action-item {{
                        opacity: 0.9;
                    }}

                    .outreach-head {{
                        display: flex;
                        justify-content: space-between;
                        align-items: start;
                        gap: 16px;
                        margin-bottom: 14px;
                    }}

                    .outreach-head h1 {{
                        margin: 0 0 4px;
                    }}

                    .outreach-links {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 10px;
                    }}

                    .outreach-summary {{
                        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                        margin-bottom: 14px;
                    }}

                    .outreach-summary div,
                    .outreach-recommendation-summary div,
                    .outreach-evidence-summary div {{
                        background: var(--surface-card);
                        box-shadow: var(--shadow-card);
                    }}

                    .outreach-grid {{
                        display: grid;
                        grid-template-columns: repeat(2, minmax(0, 1fr));
                        gap: 14px;
                        margin-bottom: 14px;
                    }}

                    .outreach-grid > .panel,
                    .outreach-head + .summary + .outreach-grid > .panel,
                    .outreach-head + .summary + .panel,
                    .outreach-grid + .panel {{
                        background: var(--surface-panel);
                        box-shadow: var(--shadow-panel);
                    }}

                    .outreach-panel {{
                        background: var(--surface-panel);
                        border-color: var(--border);
                        box-shadow: var(--shadow-panel);
                    }}

                    .outreach-recommendation-summary,
                    .outreach-evidence-summary {{
                        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
                        margin-bottom: 12px;
                    }}

                    .outreach-rationale-list {{
                        margin: 10px 0 0;
                        padding-left: 18px;
                        color: #334e68;
                        font-size: var(--type-body);
                        line-height: 1.5;
                    }}

                    .outreach-pattern {{
                        margin: 0 0 10px;
                        color: #334e68;
                        font-size: var(--type-body);
                        line-height: 1.45;
                    }}

                    .outreach-note-box {{
                        margin-top: 10px;
                        padding: 12px 14px;
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        background: var(--surface-inset);
                        box-shadow: var(--shadow-inset);
                    }}

                    .outreach-note-box.outreach-pattern {{
                        margin-bottom: 12px;
                    }}

                    .outreach-note-box.outreach-rationale-list {{
                        margin: 10px 0 0;
                        padding: 12px 14px 12px 32px;
                    }}

                    .outreach-draft-block {{
                        margin-bottom: 12px;
                    }}

                    .outreach-draft-subject,
                    .outreach-draft-body {{
                        white-space: pre-wrap;
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        background: var(--surface-card);
                        padding: 12px 14px;
                        font-size: var(--type-body);
                        line-height: 1.5;
                        color: #233142;
                        box-shadow: var(--shadow-inset);
                    }}

                    .outreach-context-item {{
                        padding: 10px 12px;
                        background: var(--surface-card);
                        box-shadow: var(--shadow-inset);
                        font-size: var(--type-body);
                    }}

                    .outreach-context-item strong,
                    .outreach-panel strong {{
                        font-size: var(--type-card-title);
                    }}

                    .outreach-panel .muted,
                    .outreach-panel .action-meta,
                    .outreach-panel .action-cue {{
                        font-size: var(--type-body);
                    }}

                    .bars {{
                        display: grid;
                        gap: 10px;
                    }}

                    .donut-layout {{
                        display: grid;
                        grid-template-columns: minmax(150px, 190px) minmax(0, 1fr);
                        gap: 16px;
                        align-items: center;
                    }}

                    .donut-chart {{
                        width: 164px;
                        height: 164px;
                        border-radius: 50%;
                        display: grid;
                        place-items: center;
                        margin: 4px auto;
                        box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.75);
                    }}

                    .donut-hole {{
                        width: 98px;
                        height: 98px;
                        border-radius: 50%;
                        background: var(--surface);
                        display: grid;
                        place-items: center;
                        text-align: center;
                        box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
                    }}

                    .donut-hole strong {{
                        display: block;
                        font-size: 24px;
                        line-height: 1;
                    }}

                    .donut-hole span {{
                        display: block;
                        margin-top: 4px;
                        color: var(--muted);
                        font-size: 12px;
                        font-weight: 600;
                    }}

                    .donut-legend {{
                        list-style: none;
                        margin: 0;
                        padding: 0;
                        display: grid;
                        gap: 10px;
                    }}

                    .donut-legend-row {{
                        display: flex;
                        align-items: center;
                        justify-content: space-between;
                        gap: 12px;
                        font-size: 13px;
                    }}

                    .donut-legend-label {{
                        display: inline-flex;
                        align-items: center;
                        gap: 10px;
                        min-width: 0;
                    }}

                    .donut-swatch {{
                        width: 12px;
                        height: 12px;
                        border-radius: 999px;
                        flex-shrink: 0;
                    }}

                    .donut-legend-value {{
                        font-weight: 700;
                        white-space: nowrap;
                    }}

                    .insights-grid .bar-row {{
                        gap: 7px;
                        font-size: 12px;
                    }}

                    .insights-grid .bar-track {{
                        height: 9px;
                    }}

                    .insights-grid .bars {{
                        gap: 7px;
                    }}

                    .insights-grid .stacked-legend {{
                        margin-bottom: 8px;
                        gap: 8px 12px;
                    }}

                    .insights-grid .stacked-legend-item {{
                        font-size: 11px;
                    }}

                    .insights-grid .donut-layout {{
                        grid-template-columns: minmax(118px, 148px) minmax(0, 1fr);
                        gap: 12px;
                    }}

                    .insights-grid .donut-chart {{
                        width: 132px;
                        height: 132px;
                    }}

                    .insights-grid .donut-hole {{
                        width: 76px;
                        height: 76px;
                    }}

                    .insights-grid .donut-hole strong {{
                        font-size: 17px;
                    }}

                    .insights-grid .donut-hole span {{
                        font-size: 10px;
                    }}

                    .insights-grid .donut-legend {{
                        gap: 7px;
                    }}

                    .insights-grid .donut-legend-row {{
                        gap: 8px;
                        font-size: 11px;
                    }}

                    .insights-grid .donut-swatch {{
                        width: 10px;
                        height: 10px;
                    }}

                    .chart-panel summary {{
                        cursor: pointer;
                        font-size: 18px;
                        font-weight: 700;
                        color: #314458;
                    }}

                    .chart-panel[open] summary {{
                        margin-bottom: 14px;
                    }}

                    .chart-grid {{
                        margin-bottom: 0;
                    }}

                    .bar-row {{
                        display: grid;
                        grid-template-columns: 88px minmax(120px, 1fr) 34px;
                        gap: 10px;
                        align-items: center;
                        font-size: 14px;
                    }}

                    .wide-labels .bar-row {{
                        grid-template-columns: 150px minmax(120px, 1fr) 34px;
                    }}

                    .bar-track {{
                        height: 12px;
                        overflow: hidden;
                        background: #dbe5ef;
                        border-radius: 999px;
                    }}

                    .bar-fill {{
                        height: 100%;
                        background: var(--blue);
                        border-radius: 999px;
                    }}

                    .stacked-legend {{
                        list-style: none;
                        margin: 0 0 10px;
                        padding: 0;
                        display: flex;
                        flex-wrap: wrap;
                        gap: 10px 14px;
                    }}

                    .stacked-legend-item {{
                        display: inline-flex;
                        align-items: center;
                        gap: 6px;
                        font-size: 12px;
                        color: var(--muted);
                        font-weight: 600;
                    }}

                    .stacked-legend-swatch {{
                        width: 10px;
                        height: 10px;
                        border-radius: 999px;
                        flex-shrink: 0;
                    }}

                    .stacked-track {{
                        display: flex;
                        align-items: stretch;
                        gap: 0;
                    }}

                    .stacked-segment {{
                        height: 100%;
                        min-width: 2px;
                    }}

                    .stacked-segment:first-child {{
                        border-top-left-radius: 999px;
                        border-bottom-left-radius: 999px;
                    }}

                    .stacked-segment:last-child {{
                        border-top-right-radius: 999px;
                        border-bottom-right-radius: 999px;
                    }}

                    .controls {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
                        gap: 12px;
                        align-items: end;
                        margin-bottom: 18px;
                        padding: 16px;
                        background: rgba(255, 255, 255, 0.98);
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .upload-form {{
                        display: grid;
                        grid-template-columns: minmax(220px, 1fr) minmax(160px, 220px);
                        gap: 12px;
                        align-items: center;
                    }}

                    .validation-grid {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                        gap: 16px;
                    }}

                    .validation-grid h3 {{
                        margin: 0 0 8px;
                        font-size: 16px;
                    }}

                    .validation-grid ul {{
                        margin: 0;
                        padding-left: 20px;
                    }}

                    .controls.compact {{
                        grid-template-columns: repeat(auto-fit, minmax(180px, 240px));
                    }}

                    .crm-filter-grid {{
                        grid-template-columns: repeat(4, minmax(0, 1fr));
                        grid-template-areas:
                            "toggle toggle toggle toggle"
                            "customer direction subject rows"
                            "range datefrom dateto actions";
                    }}

                    .crm-filter-toggle {{
                        grid-area: toggle;
                        display: flex;
                        align-items: center;
                        justify-content: space-between;
                        gap: 16px;
                        padding: 2px 0 4px;
                    }}

                    .toggle-label {{
                        font-size: 13px;
                        font-weight: 600;
                        color: var(--muted);
                    }}

                    .toggle-chips {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 10px;
                    }}

                    .toggle-chip {{
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        min-height: 36px;
                        padding: 0 14px;
                        border: 1px solid var(--border);
                        border-radius: 999px;
                        background: #fff;
                        color: var(--muted);
                        font-weight: 600;
                        text-decoration: none;
                    }}

                    .toggle-chip.active {{
                        background: var(--accent-soft);
                        border-color: var(--accent-border);
                        color: var(--accent);
                    }}

                    .crm-filter-grid .crm-field.customer {{
                        grid-area: customer;
                    }}

                    .crm-filter-grid .crm-field.direction {{
                        grid-area: direction;
                    }}

                    .crm-filter-grid .crm-field.subject {{
                        grid-area: subject;
                    }}

                    .crm-filter-grid .crm-field.rows {{
                        grid-area: rows;
                    }}

                    .crm-filter-grid .crm-field.range {{
                        grid-area: range;
                    }}

                    .crm-filter-grid .crm-field.date-from {{
                        grid-area: datefrom;
                    }}

                    .crm-filter-grid .crm-field.date-to {{
                        grid-area: dateto;
                    }}

                    .crm-filter-action {{
                        min-width: 0;
                        align-self: end;
                    }}

                    .crm-filter-actions {{
                        grid-area: actions;
                        display: grid;
                        grid-template-columns: repeat(2, minmax(0, 1fr));
                        gap: 12px;
                        align-self: end;
                    }}

                    .controls label {{
                        display: grid;
                        gap: 6px;
                        min-width: 0;
                    }}

                    .controls span {{
                        color: var(--muted);
                        font-size: 13px;
                    }}

                    input, select {{
                        width: 100%;
                        box-sizing: border-box;
                        padding: 10px 11px;
                        border: 1px solid var(--border-strong);
                        border-radius: 6px;
                        background: #fff;
                        color: var(--text);
                        font: inherit;
                    }}

                    button, .button {{
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        gap: 10px;
                        box-sizing: border-box;
                        width: 100%;
                        height: 40px;
                        padding: 10px 14px;
                        border: 1px solid var(--blue);
                        border-radius: 6px;
                        background: var(--blue);
                        color: white;
                        font: inherit;
                        font-weight: 700;
                        text-decoration: none;
                        cursor: pointer;
                    }}

                    .button svg,
                    button svg {{
                        width: 18px;
                        height: 18px;
                        flex-shrink: 0;
                        display: block;
                    }}

                    .controls button,
                    .controls .button {{
                        align-self: stretch;
                    }}

                    .button.secondary {{
                        border-color: var(--blue);
                        background: var(--blue);
                        color: #ffffff;
                    }}

                    .button.home-mode-cta {{
                        border-color: #245cff;
                        background: #245cff;
                        color: #ffffff;
                    }}

                    .home-focus-head .home-preview-dismiss-button {{
                        width: auto;
                        flex: 0 0 auto;
                        display: inline-flex;
                    }}

                    .pager {{
                        display: flex;
                        flex-wrap: wrap;
                        justify-content: space-between;
                        align-items: center;
                        gap: 12px;
                        margin-top: 12px;
                    }}

                    .pager-actions {{
                        display: flex;
                        gap: 10px;
                        margin-left: auto;
                    }}

                    .pager-button {{
                        min-width: 120px;
                    }}

                    .note {{
                        margin-bottom: 20px;
                        padding: 16px 18px;
                        background: rgba(255, 255, 255, 0.98);
                        border: 1px solid #f5d8ac;
                        border-radius: 12px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .note h2 {{
                        margin: 0 0 8px;
                        font-size: 18px;
                    }}

                    .note p {{
                        margin: 0 0 10px;
                    }}

                    .note ul {{
                        margin: 0;
                        padding-left: 20px;
                    }}

                    .note li {{
                        margin: 4px 0;
                    }}

                    .attention-top {{
                        display: grid;
                        grid-template-columns: minmax(0, 1fr) minmax(250px, 300px);
                        gap: 16px;
                        align-items: start;
                        margin-bottom: 20px;
                    }}

                    .attention-top .controls.compact {{
                        margin-bottom: 0;
                    }}

                    .compact-note {{
                        margin-bottom: 0;
                        padding: 12px 14px;
                    }}

                    .compact-note h2 {{
                        margin-bottom: 6px;
                        font-size: 16px;
                    }}

                    .compact-note p {{
                        margin-bottom: 8px;
                        font-size: 13px;
                        color: #52606d;
                    }}

                    .compact-note ul {{
                        padding-left: 18px;
                    }}

                    .compact-note li {{
                        margin: 3px 0;
                        font-size: 13px;
                    }}

                    table {{
                        width: 100%;
                        border-collapse: separate;
                        border-spacing: 0;
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 12px;
                        font-size: 14px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .table-wrap {{
                        width: 100%;
                        overflow-x: auto;
                        overflow-y: visible;
                        border-radius: 12px;
                    }}

                    .table-wrap.tall-table {{
                        max-height: min(70vh, 760px);
                        overflow: auto;
                        overscroll-behavior: contain;
                    }}

                    .crm-timeline-table {{
                        table-layout: fixed;
                    }}

                    .crm-timeline-table th:nth-child(1),
                    .crm-timeline-table td:nth-child(1) {{
                        width: 14%;
                        min-width: 120px;
                        white-space: normal;
                    }}

                    .crm-timeline-table th:nth-child(2),
                    .crm-timeline-table td:nth-child(2) {{
                        width: 10%;
                        min-width: 100px;
                        white-space: normal;
                    }}

                    .crm-timeline-table th:nth-child(3),
                    .crm-timeline-table td:nth-child(3) {{
                        width: 26%;
                        min-width: 220px;
                        white-space: normal;
                        overflow-wrap: anywhere;
                    }}

                    .crm-timeline-table th:nth-child(4),
                    .crm-timeline-table td:nth-child(4) {{
                        width: 27%;
                        min-width: 240px;
                        white-space: normal;
                        overflow-wrap: anywhere;
                    }}

                    .crm-timeline-table th:nth-child(5),
                    .crm-timeline-table td:nth-child(5) {{
                        width: 23%;
                        min-width: 220px;
                        white-space: normal;
                        overflow-wrap: anywhere;
                    }}

                    .crm-activities-table {{
                        table-layout: fixed;
                    }}

                    .crm-activities-table th:nth-child(1),
                    .crm-activities-table td:nth-child(1) {{
                        width: 38%;
                        min-width: 320px;
                        white-space: normal;
                        overflow-wrap: anywhere;
                    }}

                    .crm-activities-table th:nth-child(2),
                    .crm-activities-table td:nth-child(2) {{
                        width: 62%;
                        min-width: 420px;
                        white-space: normal;
                        overflow-wrap: anywhere;
                    }}

                    .customers-table {{
                        table-layout: fixed;
                    }}

                    .customers-table th,
                    .customers-table td {{
                        white-space: normal;
                    }}

                    .customers-table th:nth-child(1),
                    .customers-table td:nth-child(1) {{
                        width: 18%;
                        min-width: 180px;
                        overflow-wrap: anywhere;
                    }}

                    .customers-table th:nth-child(2),
                    .customers-table td:nth-child(2),
                    .customers-table th:nth-child(3),
                    .customers-table td:nth-child(3),
                    .customers-table th:nth-child(4),
                    .customers-table td:nth-child(4) {{
                        width: 6%;
                    }}

                    .customers-table th:nth-child(5),
                    .customers-table td:nth-child(5) {{
                        width: 10%;
                    }}

                    .customers-table th:nth-child(6),
                    .customers-table td:nth-child(6) {{
                        width: 8%;
                    }}

                    .customers-table th:nth-child(7),
                    .customers-table td:nth-child(7),
                    .customers-table th:nth-child(8),
                    .customers-table td:nth-child(8) {{
                        width: 9%;
                    }}

                    .customers-table th:nth-child(9),
                    .customers-table td:nth-child(9) {{
                        width: 15%;
                    }}

                    .customers-table th:nth-child(10),
                    .customers-table td:nth-child(10) {{
                        width: 10%;
                        min-width: 72px;
                        text-align: center;
                        white-space: nowrap;
                    }}

                    .customers-last-activity {{
                        display: grid;
                        gap: 6px;
                        justify-items: start;
                        min-width: 0;
                    }}

                    .attention-table {{
                        table-layout: fixed;
                    }}

                    .attention-table th,
                    .attention-table td {{
                        white-space: normal;
                    }}

                    .attention-table th:nth-child(1),
                    .attention-table td:nth-child(1) {{
                        width: 18%;
                        min-width: 170px;
                        overflow-wrap: anywhere;
                    }}

                    .attention-table th:nth-child(2),
                    .attention-table td:nth-child(2),
                    .attention-table th:nth-child(3),
                    .attention-table td:nth-child(3) {{
                        width: 7%;
                    }}

                    .attention-table th:nth-child(4),
                    .attention-table td:nth-child(4) {{
                        width: 8%;
                        text-align: center;
                    }}

                    .attention-table th:nth-child(5),
                    .attention-table td:nth-child(5) {{
                        width: 22%;
                    }}

                    .attention-table th:nth-child(6),
                    .attention-table td:nth-child(6) {{
                        width: 16%;
                        overflow-wrap: anywhere;
                    }}

                    .attention-table th:nth-child(7),
                    .attention-table td:nth-child(7) {{
                        width: 18%;
                        overflow-wrap: anywhere;
                    }}

                    .crm-activity-meta {{
                        display: grid;
                        gap: 6px;
                    }}

                    .crm-activity-meta div {{
                        color: #334e68;
                        line-height: 1.35;
                    }}

                    th, td {{
                        padding: 11px 12px;
                        border-bottom: 1px solid #e2eaf2;
                        text-align: left;
                        vertical-align: top;
                    }}

                    th {{
                        position: sticky;
                        top: 0;
                        z-index: 3;
                        background: #eaf1f8;
                        font-size: 12px;
                        text-transform: uppercase;
                        color: #39516b;
                        letter-spacing: 0.02em;
                        box-shadow: inset 0 -1px 0 #d8e4f0;
                    }}

                    th:nth-child(3),
                    td:nth-child(3),
                    th:nth-last-child(2),
                    td:nth-last-child(2) {{
                        min-width: 112px;
                        white-space: nowrap;
                    }}

                    th:nth-child(4),
                    td:nth-child(4) {{
                        min-width: 112px;
                        white-space: nowrap;
                    }}

                    tr:last-child td {{
                        border-bottom: 0;
                    }}

                    tbody tr:hover td {{
                        background: #fbfdff;
                    }}

                    td strong {{
                        line-height: 1.3;
                    }}

                    .score {{
                        display: inline-block;
                        min-width: 48px;
                        padding: 4px 8px;
                        border-radius: 6px;
                        background: #fde8e8;
                        color: #991b1b;
                        font-weight: 700;
                        text-align: center;
                    }}

                    .badge {{
                        display: inline-block;
                        margin-top: 6px;
                        padding: 4px 7px;
                        border-radius: 6px;
                        background: #dcfce7;
                        color: #166534;
                        font-size: 12px;
                        font-weight: 700;
                    }}

                    .activity-cell {{
                        display: grid;
                        gap: 6px;
                        min-width: 0;
                    }}

                    .attention-action-cell {{
                        display: grid;
                        gap: 8px;
                        min-width: 0;
                    }}

                    .attention-dismiss-form {{
                        display: grid;
                        grid-template-columns: 1fr;
                        gap: 8px;
                        align-items: stretch;
                        width: 100%;
                        justify-items: start;
                    }}

                    .attention-dismiss-form .action-dismiss-button {{
                        width: 110px;
                    }}

                    .attention-dismiss-form .dismiss-reason-input {{
                        width: 100%;
                        min-width: 0;
                    }}

                    .activity-summary {{
                        display: block;
                    }}

                    .activity-note {{
                        display: block;
                        margin-top: 6px;
                        color: var(--muted);
                        font-size: 13px;
                        white-space: pre-wrap;
                        line-height: 1.45;
                    }}

                    .crm-direction {{
                        display: inline-block;
                        min-width: 72px;
                        padding: 4px 8px;
                        border-radius: 6px;
                        background: #e4eef8;
                        color: #1f5f99;
                        font-size: 12px;
                        font-weight: 700;
                        text-align: center;
                        text-transform: capitalize;
                    }}

                    .crm-direction.inbound {{
                        background: #dcfce7;
                        color: #166534;
                    }}

                    .crm-direction.outbound {{
                        background: #dbeafe;
                        color: #1d4ed8;
                    }}

                    .crm-pill-stack {{
                        display: flex;
                        flex-direction: column;
                        align-items: center;
                        gap: 6px;
                    }}

                    .crm-category {{
                        display: inline-block;
                        min-width: 96px;
                        padding: 4px 8px;
                        border-radius: 6px;
                        background: #eef2f7;
                        color: #475569;
                        font-size: 12px;
                        font-weight: 700;
                        text-align: center;
                    }}

                    .crm-category.sales-outreach {{
                        background: #ede9fe;
                        color: #6d28d9;
                    }}

                    .crm-category.customer-services {{
                        background: #fff7ed;
                        color: #c2410c;
                    }}

                    .activity-expand {{
                        display: block;
                    }}

                    .activity-expand summary {{
                        cursor: pointer;
                        color: #334e68;
                        line-height: 1.45;
                    }}

                    .activity-expand[open] summary {{
                        margin-bottom: 8px;
                    }}

                    .contact-panel {{
                        margin-bottom: 20px;
                    }}

                    .contact-summary {{
                        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                    }}

                    .contact-guidance {{
                        margin: 0 0 10px;
                        color: #334e68;
                        line-height: 1.45;
                    }}

                    .contact-guidance.subtle {{
                        margin-bottom: 0;
                        color: #52606d;
                        font-size: 14px;
                    }}

                    .timeline-actions {{
                        margin-top: 12px;
                        display: flex;
                        justify-content: flex-end;
                    }}

                    .small-button {{
                        width: auto;
                        min-width: 140px;
                    }}

                    .activity-content-summary {{
                        display: block;
                        min-height: 160px;
                        max-height: 220px;
                        overflow: auto;
                        white-space: pre-wrap;
                        font-size: 13px;
                        line-height: 1.45;
                        font-weight: 400;
                        color: #334e68;
                    }}

                    .status, .empty {{
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        padding: 16px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .error {{
                        color: #991b1b;
                    }}

                    .success {{
                        color: #166534;
                    }}

                    @media (max-width: 1480px) {{
                        .insights-grid {{
                            grid-template-columns: repeat(2, minmax(0, 1fr));
                        }}

                        .insights-span-2 {{
                            grid-column: span 2;
                        }}
                    }}

                    @media (max-width: 1120px) {{
                        .dashboard-grid {{
                            grid-template-columns: 1fr;
                        }}

                        .insights-span-2 {{
                            grid-column: auto;
                        }}

                        main {{
                            padding: 22px 18px 32px;
                        }}

                        .panel h2.home-focus-title {{
                            font-size: 38px;
                        }}

                        .home-focus-grid {{
                            grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
                            gap: 22px;
                        }}

                        .home-focus-grid .home-focus-column:last-child {{
                            grid-column: 1 / -1;
                        }}

                        .home-focus-column:nth-child(2)::after {{
                            display: none;
                        }}

                        .home-focus-card {{
                            min-height: 0;
                        }}

                        .home-evidence-strip {{
                            grid-template-columns: repeat(3, minmax(0, 1fr));
                        }}

                        .home-evidence-strip div {{
                            border-right: 1px solid var(--border);
                            border-bottom: 1px solid var(--border);
                        }}

                        .home-evidence-strip div:nth-child(3n) {{
                            border-right: 0;
                        }}

                        .home-evidence-strip div:nth-last-child(-n + 1) {{
                            border-bottom: 0;
                        }}

                        .home-evidence-strip div:nth-last-child(-n + 2):nth-child(3n + 1),
                        .home-evidence-strip div:nth-last-child(-n + 2):nth-child(3n + 2) {{
                            border-bottom: 0;
                        }}

                        .queue-table th:nth-child(3),
                        .queue-table td:nth-child(3),
                        .queue-table th:nth-child(4),
                        .queue-table td:nth-child(4) {{
                            white-space: normal;
                        }}

                        .donut-layout {{
                            grid-template-columns: 1fr;
                        }}
                    }}

                    @media (max-width: 720px) {{
                        main {{
                            padding: 18px 14px 28px;
                        }}

                        .panel h2.home-focus-title {{
                            font-size: 34px;
                        }}

                        h1 {{
                            margin-bottom: 14px;
                            font-size: 26px;
                        }}

                        .top-row {{
                            align-items: start;
                            margin-bottom: 12px;
                        }}

                        .nav {{
                            font-size: 12px;
                        }}

                        .nav-shell {{
                            flex-wrap: wrap;
                        }}

                        .nav-admin-menu {{
                            margin-left: 0;
                        }}

                        .hero {{
                            grid-template-columns: 1fr;
                            gap: 16px;
                        }}

                        .hero-actions {{
                            min-width: 0;
                        }}

                        .summary {{
                            gap: 10px;
                            margin-bottom: 14px;
                        }}

                        .summary div {{
                            padding: 11px 12px;
                        }}

                        .home-focus-head {{
                            flex-direction: column;
                            align-items: stretch;
                        }}

                        .home-preview-dismiss-form {{
                            width: 100%;
                        }}

                        .home-preview-dismiss-button {{
                            width: auto;
                            min-width: 160px;
                            align-self: start;
                        }}

                        .home-dismiss-actions {{
                            grid-template-columns: 1fr;
                        }}

                        .home-dismiss-confirm,
                        .home-dismiss-cancel {{
                            width: 100%;
                            min-width: 0;
                        }}

                        .home-focus-grid {{
                            grid-template-columns: 1fr;
                        }}

                        .home-focus-grid .home-focus-column:last-child {{
                            grid-column: auto;
                        }}

                        .home-focus-column:not(:last-child)::after {{
                            display: none;
                        }}

                        .home-focus-card {{
                            min-height: 0;
                        }}

                        .home-evidence-strip {{
                            grid-template-columns: repeat(2, minmax(0, 1fr));
                        }}

                        .home-evidence-strip div {{
                            border-right: 0;
                            border-bottom: 1px solid var(--border);
                        }}

                        .home-evidence-strip div:nth-last-child(-n + 2) {{
                            border-bottom: 0;
                        }}

                        .panel,
                        .controls,
                        .note {{
                            padding: 14px;
                        }}

                        .upload-form {{
                            grid-template-columns: 1fr;
                        }}

                        .sales-cards {{
                            grid-template-columns: 1fr;
                        }}

                        .attention-top {{
                            grid-template-columns: 1fr;
                        }}

                        .controls {{
                            grid-template-columns: 1fr;
                            gap: 10px;
                        }}

                        .controls.compact {{
                            grid-template-columns: 1fr;
                        }}

                        .crm-filter-grid {{
                            grid-template-columns: 1fr;
                            grid-template-areas:
                                "toggle"
                                "customer"
                                "direction"
                                "subject"
                                "rows"
                                "range"
                                "datefrom"
                                "dateto"
                                "actions";
                        }}

                        .crm-filter-toggle {{
                            flex-direction: column;
                            align-items: stretch;
                        }}

                        .crm-filter-actions {{
                            grid-template-columns: 1fr;
                        }}

                        .customer-summary {{
                            grid-template-columns: repeat(2, minmax(0, 1fr));
                        }}

                        .outreach-head,
                        .outreach-grid {{
                            grid-template-columns: 1fr;
                            display: grid;
                        }}

                        .outreach-links {{
                            justify-content: stretch;
                        }}

                        .donut-chart {{
                            width: 156px;
                            height: 156px;
                        }}
                    }}
                </style>
            </head>
            <body>
                <main>
                    <div class="top-row">
                        {render_global_nav(title)}
                        {top_right}
                    </div>
                    {page_title_html}
                    {body}
                </main>
            </body>
        </html>
    """
