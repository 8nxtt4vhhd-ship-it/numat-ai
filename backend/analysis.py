import os
from datetime import datetime

from dateutil import parser

# Mock order data (replace later with FileMaker)
orders = [
    {"customer": "ABC Ltd", "order_date": "2024-01-01", "amount": 200},
    {"customer": "ABC Ltd", "order_date": "2024-02-01", "amount": 220},
    {"customer": "ABC Ltd", "order_date": "2024-03-01", "amount": 210},
    {"customer": "XYZ Ltd", "order_date": "2024-01-15", "amount": 150},
    {"customer": "XYZ Ltd", "order_date": "2024-03-20", "amount": 140},
    {"customer": "FastClean Co", "order_date": "2024-02-01", "amount": 300}
]


def group_by_customer(orders):
    customers = {}

    for order in orders:
        name = order["customer"]

        if name not in customers:
            customers[name] = []

        customers[name].append(order)

    return customers


def calculate_average_gap(order_list):
    dates = sorted([
        datetime.strptime(o["order_date"], "%Y-%m-%d")
        for o in order_list
    ])

    if len(dates) < 2:
        return None

    gaps = []
    for i in range(1, len(dates)):
        gap = (dates[i] - dates[i - 1]).days
        gaps.append(gap)

    return sum(gaps) / len(gaps)


def get_analysis_today():
    today = os.getenv("ANALYSIS_TODAY", "2024-07-01")
    return datetime.strptime(today, "%Y-%m-%d")


def parse_date(value):
    if not value:
        return None

    date_order = os.getenv("FILEMAKER_DATE_ORDER", "mdy").strip().lower()

    try:
        return parser.parse(str(value), dayfirst=date_order == "dmy")
    except (TypeError, ValueError):
        return None


def get_last_activity(order_list):
    activity = get_last_activity_info(order_list)
    return activity["date"] if activity else None


def get_last_activity_info(order_list):
    latest_activity = None
    latest_content = ""

    for order in order_list:
        extra = order.get("extra", {})
        activity_date = parse_date(extra.get("Companies 4::Last Activity Act"))

        if activity_date and (
            latest_activity is None or activity_date > latest_activity
        ):
            latest_activity = activity_date
            latest_content = get_activity_content(extra)

    if not latest_activity:
        return None

    return {
        "date": latest_activity,
        "content": latest_content,
    }


def get_activity_content(extra):
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


def find_late_customers(customers, today=None):
    results = []

    if today is None:
        today = get_analysis_today()

    for name, orders in customers.items():
        avg_gap = calculate_average_gap(orders)

        if not avg_gap:
            continue

        last_order = max([
            datetime.strptime(o["order_date"], "%Y-%m-%d")
            for o in orders
        ])

        days_since_last = (today - last_order).days

        if days_since_last > avg_gap:
            priority = days_since_last / avg_gap
            last_activity = get_last_activity_info(orders)
            last_activity_date = last_activity["date"] if last_activity else None

            if priority > 2:
                action = "Urgent: contact immediately"
            elif priority > 1.5:
                action = "Follow up soon"
            elif priority > 1.25:
                action = "Consider follow-up"
            else:
                action = "Watch"

            results.append({
                "customer": name,
                "avg_gap": round(avg_gap, 1),
                "days_since_last": days_since_last,
                "priority_score": round(priority, 2),
                "action": action,
                "last_activity_date": (
                    last_activity_date.strftime("%Y-%m-%d")
                    if last_activity_date else None
                ),
                "last_activity_content": (
                    last_activity["content"]
                    if last_activity else ""
                ),
                "days_since_last_activity": (
                    (today.date() - last_activity_date.date()).days
                    if last_activity_date else None
                ),
                "status": "needs_attention"
            })

    return results


if __name__ == "__main__":
    customers = group_by_customer(orders)
    late = find_late_customers(customers)

    print("RESULTS:")
    for c in late:
        print(c)
