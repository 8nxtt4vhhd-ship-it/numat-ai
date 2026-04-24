from collections import Counter, defaultdict
import base64
from datetime import datetime, timedelta
from html import escape
from html.parser import HTMLParser
import os
import secrets
from threading import Lock, Thread
from urllib.parse import quote

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse

from ai import add_ai_explanations
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
    fetch_crm_activities,
    get_filemaker_crm_cache_path,
    get_crm_sample_csv_path,
    get_uploaded_crm_csv_path,
    sync_filemaker_crm_cache,
    validate_crm_csv_content,
    validate_crm_csv_path,
)
from filemaker import (
    check_filemaker_connection,
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


PREVIEW_AUTH_EXEMPT_PATHS = {
    "/health",
    "/filemaker-health",
}


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
def read_root():
    return render_home_page()


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
        subject=subject,
        date_from=date_from,
        date_to=date_to,
        range_key=range_key,
    )
    body = f"""
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

        {render_crm_filter_form(customer, direction, subject, page_size, date_from, date_to, range_key)}
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

        {render_crm_pagination(customer, direction, subject, date_from, date_to, range_key, page, total_pages, page_size, total_filtered)}
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


def render_home_page():
    order_result = get_orders_for_analysis()
    attention_result = build_customers_needing_attention_response()

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
    customer_count = len(group_by_customer(orders))
    recent_activity_count = count_recent_attention_activity(attention_customers)
    action_plan = build_home_action_plan(attention_customers, grouped_orders)

    body = f"""
        <section class="hero">
            <div>
                <p>
                    Review customer order rhythm, spot accounts needing attention,
                    and check recent contact before reaching out.
                </p>
            </div>
        </section>

        {render_dashboard_context(order_result["source"])}

        <div class="summary home-summary">
            <div>
                <span class="label">Source</span>
                <strong>{escape(order_result["source"])}</strong>
            </div>
            <div>
                <span class="label">Orders</span>
                <strong>{len(orders)}</strong>
            </div>
            <div>
                <span class="label">Customers</span>
                <strong>{customer_count}</strong>
            </div>
            <div>
                <span class="label">Need Attention</span>
                <strong>{len(attention_customers)}</strong>
            </div>
            <div>
                <span class="label">Recently Contacted</span>
                <strong>{recent_activity_count}</strong>
            </div>
        </div>

        <section class="cards sales-cards">
            <a class="nav-card" href="/customers-needing-attention-view">
                <span class="label">Follow-up queue</span>
                <strong>Customers Needing Attention</strong>
                <p>Prioritized by how far each customer is beyond their usual cycle.</p>
            </a>
            <a class="nav-card" href="/orders-view">
                <span class="label">Order browser</span>
                <strong>Orders</strong>
                <p>Filter, sort, and drill into individual customer order history.</p>
            </a>
            <a class="nav-card" href="/customers-view">
                <span class="label">Customer summary</span>
                <strong>Customers</strong>
                <p>Compare order cycles, value, last activity, and attention status by customer.</p>
            </a>
        </section>

        {render_home_action_plan(action_plan)}

        {render_recent_contact_holds(action_plan["hold_customers"])}

        <details class="panel chart-panel">
            <summary>Trend and Territory Charts</summary>
            <div class="dashboard-grid chart-grid">
                {render_monthly_orders_chart(orders)}
                {render_attention_chart(attention_customers)}
                {render_state_orders_chart(orders)}
            </div>
        </details>

        {render_top_attention_table(attention_customers)}
    """

    return render_page(
        title="Numat AI Sales Assistant",
        body=body,
        top_right=render_home_admin_links(),
    )


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
    filtered_crm_activities = filter_customer_crm_activities(
        crm_activities,
        direction=crm_direction,
    )
    latest_crm_activity = crm_activities[0] if crm_activities else None
    latest_crm_activity_date = parse_crm_datetime(
        latest_crm_activity.get("date_created", "")
    ) if latest_crm_activity else None
    display_last_activity = last_activity or latest_crm_activity_date

    last_activity_content = (
        last_activity_info["content"]
        if last_activity_info else ""
    )
    display_last_contact, _last_contact_source = build_last_contact_display(
        last_activity_content,
        latest_crm_activity,
        crm_activities=crm_activities,
    )

    body = f"""
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
            <div class="wide-summary-item">
                <span class="label">Last Contact</span>
                <strong class="activity-content-summary">
                    {escape(display_last_contact) or "Not available"}
                </strong>
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
            sort,
            direction,
        )}

        {render_customer_sort_form(customer, sort, direction, crm_limit, crm_page, crm_direction)}

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


def build_customers_needing_attention_response():
    order_result = get_orders_for_analysis()

    if order_result["status"] != "ok":
        return {
            "source": order_result["source"],
            "status": order_result["status"],
            "late_customers": []
        }

    customers = group_by_customer(order_result["orders"])
    crm_result = fetch_crm_activities()
    crm_activity_map = (
        crm_result.get("activity_map", {})
        if crm_result["status"] == "ok" else {}
    )
    late = find_late_customers(customers)

    for customer in late:
        customer_orders = customers.get(customer["customer"], [])
        customer_primary_key = get_customer_primary_key(customer_orders)
        crm_matches = crm_activity_map.get(customer_primary_key, [])
        latest_crm_activity = crm_matches[0] if crm_matches else None
        crm_recent_days = get_crm_days_since_latest_activity(latest_crm_activity)
        display_last_contact, _last_contact_source = build_last_contact_display(
            customer.get("last_activity_content", ""),
            latest_crm_activity,
            crm_activities=crm_matches,
        )
        customer["display_last_contact"] = display_last_contact
        if customer.get("days_since_last_activity") is None and crm_recent_days is not None:
            customer["days_since_last_activity"] = crm_recent_days
            customer["last_activity_date"] = (
                latest_crm_activity.get("date_created", "")[:10]
                if latest_crm_activity else None
            )

    late_with_explanations = (
        add_ai_explanations(late)
        if should_add_ai_explanations()
        else late
    )
    return {
        "source": order_result["source"],
        "status": "ok",
        "late_customers": late_with_explanations
    }


def render_late_customer_row(customer):
    customer_name = str(customer["customer"])
    customer_url = f"/customer-view?customer={quote(customer_name)}"

    return f"""
        <tr>
            <td><a href="{customer_url}"><strong>{escape(customer_name)}</strong></a></td>
            <td>{escape(str(customer["avg_gap"]))} days</td>
            <td>{escape(str(customer["days_since_last"]))} days</td>
            <td><span class="score">{escape(str(customer["priority_score"]))}</span></td>
            <td>{escape(str(customer["action"]))}</td>
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
        latest_crm_activity = crm_activities[0] if crm_activities else None
        latest_crm_activity_date = parse_crm_datetime(
            latest_crm_activity.get("date_created", "")
        ) if latest_crm_activity else None
        display_last_activity = last_activity or latest_crm_activity_date

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
                latest_crm_activity.get("date_created", "")
                if latest_crm_activity else ""
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


def render_customer_crm_timeline(customer, activities, crm_limit, crm_page, crm_direction, sort, direction):
    total_pages = max(1, (len(activities) + crm_limit - 1) // crm_limit)
    crm_page = max(1, min(crm_page, total_pages))
    start_index = (crm_page - 1) * crm_limit
    shown_activities = activities[start_index:start_index + crm_limit]
    shown_count = len(shown_activities)
    summary_html = render_customer_crm_timeline_summary(
        activities=activities,
        crm_direction=crm_direction,
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
        sort=sort,
        direction=direction,
    )

    return f"""
        <section class="panel">
            <h2>CRM Timeline</h2>
            {summary_html}
            {render_customer_crm_timeline_controls(customer, crm_direction, crm_limit, sort, direction)}
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


def render_customer_crm_timeline_summary(activities, crm_direction, start_index, shown_count):
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

    return (
        "<p class=\"filter-summary\">"
        + " | ".join(escape(part) for part in parts)
        + "</p>"
    )


def render_customer_crm_timeline_controls(customer, crm_direction, crm_limit, sort, direction):
    direction_options = {
        "": "All",
        "inbound": "Inbound",
        "outbound": "Outbound",
        "unknown": "Unknown",
    }
    limit_options = {
        "12": "12 rows",
        "24": "24 rows",
        "48": "48 rows",
    }

    return f"""
        <form class="controls compact crm-timeline-controls" method="get" action="/customer-view">
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
                <span>Rows</span>
                <select name="crm_limit">
                    {render_select_options(limit_options, str(crm_limit))}
                </select>
            </label>

            <button type="submit">Apply</button>
        </form>
    """


def render_customer_crm_pagination(customer, crm_page, total_pages, crm_limit, crm_direction, sort, direction):
    if total_pages <= 1:
        return ""

    previous_link = ""
    next_link = ""

    base_query = (
        f"customer={quote(customer)}&sort={quote(sort)}&direction={quote(direction)}"
        f"&crm_limit={crm_limit}&crm_direction={quote(crm_direction)}"
    )

    if crm_page > 1:
        previous_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/customer-view?{base_query}&crm_page={crm_page - 1}">Previous</a>'
        )

    if crm_page < total_pages:
        next_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/customer-view?{base_query}&crm_page={crm_page + 1}">Next</a>'
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

    full_text = clean_activity_content(activity.get("body", ""))
    preview = truncate_text(full_text, 260)

    return f"""
        <tr>
            <td>{escape(format_optional_datetime(activity.get("date_created", "")))}</td>
            <td><span class="{direction_class}">{escape(activity.get("direction", "unknown"))}</span></td>
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


def render_crm_filter_form(customer, direction, subject, page_size, date_from, date_to, range_key):
    direction_options = {
        "": "All",
        "inbound": "Inbound",
        "outbound": "Outbound",
        "unknown": "Unknown",
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
        f'&subject={quote(subject)}&range_key=90d&page_size={page_size}'
    )
    all_link = (
        f'/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}'
        f'&subject={quote(subject)}&range_key=all&page_size={page_size}'
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


def render_crm_filter_summary(total_filtered, customer="", direction="", subject="", date_from="", date_to="", range_key="all"):
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


def render_crm_pagination(customer, direction, subject, date_from, date_to, range_key, page, total_pages, page_size, total_filtered):
    if total_filtered <= page_size:
        return ""

    previous_link = ""
    next_link = ""

    if page > 1:
        previous_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}&subject={quote(subject)}&date_from={quote(date_from)}&date_to={quote(date_to)}&range_key={quote(range_key)}&page={page - 1}&page_size={page_size}">Previous</a>'
        )

    if page < total_pages:
        next_link = (
            f'<a class="button secondary small-button pager-button" '
            f'href="/crm-activities-view?customer={quote(customer)}&direction={quote(direction)}&subject={quote(subject)}&date_from={quote(date_from)}&date_to={quote(date_to)}&range_key={quote(range_key)}&page={page + 1}&page_size={page_size}">Next</a>'
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


def filter_crm_activities(activities, customer="", direction="", subject="", date_from="", date_to="", range_key="all"):
    customer_filter = customer.strip().lower()
    direction_filter = direction.strip().lower()
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

        if customer_filter and customer_filter not in customer_value:
            continue

        if direction_filter and direction_filter != direction_value:
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


def filter_customer_crm_activities(activities, direction=""):
    direction_filter = str(direction or "").strip().lower()

    if not direction_filter:
        return activities

    return [
        activity
        for activity in activities
        if str(activity.get("direction") or "").strip().lower() == direction_filter
    ]


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


def clean_activity_content(content):
    content = str(content or "").strip()

    if not content:
        return ""

    if looks_like_html(content):
        content = ActivityHTMLToText.convert(content)

    content = tidy_activity_text(content)
    return strip_activity_noise(content)


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

    for order in orders:
        order_date = parse_iso_date(order.get("order_date"))

        if not order_date:
            continue

        monthly_counts[order_date.strftime("%Y-%m")] += 1

    months = sorted(monthly_counts.keys())[-8:]
    max_count = max([monthly_counts[month] for month in months], default=1)
    bars = "".join(
        render_bar(
            label=datetime.strptime(month, "%Y-%m").strftime("%b %Y"),
            value=monthly_counts[month],
            max_value=max_count
        )
        for month in months
    )

    if not bars:
        bars = "<p class='empty'>No dated orders available.</p>"

    return f"""
        <section class="panel">
            <h2>Order Trend</h2>
            <p class="muted">Order count by month from the selected data source.</p>
            <div class="bars">{bars}</div>
        </section>
    """


def render_attention_chart(customers):
    action_counts = Counter(customer["action"] for customer in customers)
    max_count = max(action_counts.values(), default=1)
    bars = "".join(
        render_bar(
            label=action,
            value=count,
            max_value=max_count
        )
        for action, count in action_counts.most_common()
    )

    if not bars:
        bars = "<p class='empty'>No customers currently need attention.</p>"

    return f"""
        <section class="panel">
            <h2>Attention Breakdown</h2>
            <p class="muted">How urgent the current follow-up queue is.</p>
            <div class="bars">{bars}</div>
        </section>
    """


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
            max_value=max_count
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


def render_bar(label, value, max_value):
    width = 0

    if max_value:
        width = max(6, round((value / max_value) * 100))

    return f"""
        <div class="bar-row">
            <span>{escape(str(label))}</span>
            <div class="bar-track">
                <div class="bar-fill" style="width: {width}%"></div>
            </div>
            <strong>{escape(str(value))}</strong>
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


def build_home_action_plan(attention_customers, grouped_orders):
    territory_map = {"east": [], "west": []}
    hold_customers = []

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

        if territory in territory_map:
            territory_map[territory].append(enriched_customer)

    for territory in territory_map:
        territory_customers = sort_late_customers(
            territory_map[territory],
            sort_key="priority_score",
            direction="desc",
        )
        territory_map[territory] = {
            "due_today": [
                customer
                for customer in territory_customers
                if float(customer.get("priority_score") or 0) >= 1.5
            ][:4],
            "watch_next": [
                customer
                for customer in territory_customers
                if float(customer.get("priority_score") or 0) < 1.5
            ][:4],
        }

    hold_customers = sort_late_customers(
        hold_customers,
        sort_key="days_since_last_activity",
        direction="asc",
    )[:6]

    return {
        "east": territory_map["east"],
        "west": territory_map["west"],
        "hold_customers": hold_customers,
    }


def get_customer_territory(customer_orders):
    territories = Counter(
        str(get_order_territory(order) or "").strip()
        for order in customer_orders
        if str(get_order_territory(order) or "").strip()
    )
    return territories.most_common(1)[0][0] if territories else ""


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


def render_home_action_plan(action_plan):
    return f"""
        <section class="panel action-plan-panel">
            <div class="panel-head">
                <div>
                    <h2>Today's Action Plan</h2>
                    <p class="muted">Top live follow-ups for each territory, ranked by urgency and filtered to avoid duplicate recent outreach.</p>
                </div>
            </div>
            <div class="action-columns">
                {render_territory_action_column("East", action_plan["east"])}
                {render_territory_action_column("West", action_plan["west"])}
            </div>
        </section>
    """


def render_territory_action_column(label, customers):
    due_today_items = "".join(
        render_action_plan_item(customer)
        for customer in customers["due_today"]
    )
    watch_next_items = "".join(
        render_action_plan_item(customer)
        for customer in customers["watch_next"]
    )

    if not due_today_items:
        due_today_items = "<li class='empty-action'>No immediate follow-ups right now.</li>"

    if not watch_next_items:
        watch_next_items = "<li class='empty-action'>Nothing queued next right now.</li>"

    return f"""
        <section class="action-column">
            <div class="action-column-head">
                <span class="label">Territory</span>
                <strong>{escape(label)}</strong>
            </div>
            <div class="bucket-head">Due Today</div>
            <ol class="action-list">{due_today_items}</ol>
            <div class="bucket-head secondary">Watch Next</div>
            <ol class="action-list watch-list">{watch_next_items}</ol>
        </section>
    """


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
            <a class="action-item" href="{customer_url}">
                <div class="action-main">
                    <strong>{escape(customer_name)}</strong>
                    <span class="action-meta">{escape(' • '.join(summary_bits))}</span>
                    <span class="action-cue">{escape(cue)}</span>
                </div>
                <span class="score">{escape(str(customer.get("priority_score", "")))}</span>
            </a>
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
    validation = validate_crm_csv_path()
    current_path = get_crm_sample_csv_path()
    uploaded_path = get_uploaded_crm_csv_path()
    sync_cache_path = get_filemaker_crm_cache_path()
    background_sync_status = get_crm_sync_status()
    active_result = fetch_crm_activities()
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
    active_location_label = (
        "Active FileMaker Layout" if crm_source == "filemaker" else "Active CSV Path"
    )
    active_location_value = (
        active_result.get("path")
        if crm_source == "filemaker"
        else str(current_path)
    )
    upload_summary = (
        f"""
            <div>
                <span class="label">Upload Path</span>
                <strong class="path-value">{escape(str(uploaded_path))}</strong>
            </div>
        """
        if crm_source != "filemaker"
        else ""
    )
    source_note = (
        "Live FileMaker email data is active. CSV upload remains available as a fallback."
        if crm_source == "filemaker"
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

    sync_status_panel = render_crm_sync_status_panel(background_sync_status)

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


def render_customer_sort_form(customer, sort, direction, crm_limit, crm_page, crm_direction):
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
        ("Orders", "/orders-view"),
        ("Customers Needing Attention", "/customers-needing-attention-view"),
        ("Customers", "/customers-view"),
        ("CRM Activities", "/crm-activities-view"),
        ("CRM Data", "/crm-data"),
        ("Sample Data", "/sample-data"),
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

    return "<p class=\"nav\">" + "<span>/</span>".join(items_html) + "</p>"

def render_home_admin_links():
    return """
        <div class="admin-links nav-admin-links">
            <span>Admin</span>
            <a href="/filemaker-health">FileMaker Health</a>
            <a href="/crm-activities-view">CRM Activities</a>
            <a href="/crm-data">CRM Data</a>
            <a href="/sample-data">Sample Data</a>
            <a href="/api">API</a>
        </div>
    """


def render_page(title, body, top_right=""):
    title_class = "page-title"

    if title == "Numat AI Sales Assistant":
        title_class += " home-page-title"

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
                        --bg: #f3f6fb;
                        --surface: #ffffff;
                        --surface-soft: #f8fbff;
                        --border: #d6e2ef;
                        --border-strong: #bfd0e0;
                        --text: #1f2933;
                        --muted: #5b6b7c;
                        --muted-soft: #7b8794;
                        --blue: #1f5f99;
                        --blue-dark: #174974;
                        --shadow-soft: 0 8px 24px rgba(15, 23, 42, 0.04);
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
                        font-size: 30px;
                        line-height: 1.1;
                        letter-spacing: 0;
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

                    .nav {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                        margin: 0;
                        color: var(--muted-soft);
                        font-size: 13px;
                        align-items: center;
                    }}

                    .nav a {{
                        color: var(--blue);
                        text-decoration: none;
                    }}

                    .nav-current {{
                        color: #314458;
                        font-weight: 700;
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

                    .admin-links {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                        align-items: center;
                        justify-content: flex-end;
                        font-size: 13px;
                    }}

                    .nav-admin-links {{
                        margin-left: auto;
                    }}

                    .admin-links span {{
                        color: #52606d;
                        font-weight: 700;
                    }}

                    .admin-links a {{
                        padding: 6px 9px;
                        border: 1px solid var(--border);
                        border-radius: 6px;
                        background: var(--surface);
                        color: #334e68;
                        text-decoration: none;
                    }}

                    .summary {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                        gap: 12px;
                        margin-bottom: 18px;
                    }}

                    .summary div {{
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        padding: 13px 15px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .summary strong {{
                        display: block;
                        font-size: 17px;
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
                        grid-template-columns: repeat(6, minmax(0, 1fr));
                    }}

                    .customer-summary .wide-summary-item {{
                        grid-column: span 4;
                    }}

                    .label {{
                        display: block;
                        margin-bottom: 4px;
                        color: var(--muted);
                        font-size: 12px;
                    }}

                    .path-value {{
                        font-size: 13px;
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
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        color: inherit;
                        text-decoration: none;
                        box-shadow: var(--shadow-soft);
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
                        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                        gap: 16px;
                        margin-bottom: 18px;
                    }}

                    .panel {{
                        margin-bottom: 18px;
                        padding: 18px;
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 8px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .panel h2 {{
                        margin: 0 0 6px;
                        font-size: 20px;
                    }}

                    .panel-head {{
                        display: flex;
                        justify-content: space-between;
                        gap: 12px;
                        align-items: start;
                        margin-bottom: 14px;
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

                    .bars {{
                        display: grid;
                        gap: 10px;
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

                    .controls {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
                        gap: 12px;
                        align-items: end;
                        margin-bottom: 18px;
                        padding: 16px;
                        background: var(--surface);
                        border: 1px solid var(--border);
                        border-radius: 8px;
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

                    .controls button,
                    .controls .button {{
                        align-self: stretch;
                    }}

                    .button.secondary {{
                        border-color: var(--border-strong);
                        background: white;
                        color: #334e68;
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
                        background: #fff7ed;
                        border: 1px solid #fed7aa;
                        border-radius: 8px;
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
                        border-radius: 8px;
                        font-size: 14px;
                        box-shadow: var(--shadow-soft);
                    }}

                    .table-wrap {{
                        width: 100%;
                        overflow-x: auto;
                        overflow-y: visible;
                        border-radius: 8px;
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
                        width: 8%;
                    }}

                    .attention-table th:nth-child(4),
                    .attention-table td:nth-child(4) {{
                        width: 8%;
                        text-align: center;
                    }}

                    .attention-table th:nth-child(5),
                    .attention-table td:nth-child(5) {{
                        width: 12%;
                    }}

                    .attention-table th:nth-child(6),
                    .attention-table td:nth-child(6) {{
                        width: 22%;
                        overflow-wrap: anywhere;
                    }}

                    .attention-table th:nth-child(7),
                    .attention-table td:nth-child(7) {{
                        width: 24%;
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

                    @media (max-width: 720px) {{
                        main {{
                            padding: 18px 14px 28px;
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

                        .customer-summary .wide-summary-item {{
                            grid-column: span 2;
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
                    <h1 class="{title_class}">{escape(title)}</h1>
                    {body}
                </main>
            </body>
        </html>
    """
