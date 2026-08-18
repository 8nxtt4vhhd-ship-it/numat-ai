import os
from datetime import datetime
from statistics import median

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
    cycle = analyze_order_cycle(order_list)
    return cycle["cycle_days"]


def analyze_order_cycle(order_list):
    dates = sorted([
        datetime.strptime(o["order_date"], "%Y-%m-%d")
        for o in order_list
        if o.get("order_date")
    ])

    if len(dates) < 3:
        return {
            "cycle_days": None,
            "average_gap": None,
            "median_gap": None,
            "recent_gap": None,
            "pattern": "insufficient",
            "pattern_label": "Not enough orders",
            "consistency": "unknown",
            "consistency_ratio": 0,
            "gap_count": 0,
        }

    gaps = []
    for i in range(1, len(dates)):
        gap = (dates[i] - dates[i - 1]).days
        gaps.append(gap)

    average_gap = sum(gaps) / len(gaps)
    median_gap = float(median(gaps))
    recent_gap = gaps[-1]
    tolerance = max(7, median_gap * 0.25)
    consistent_gaps = sum(
        1 for gap in gaps
        if abs(gap - median_gap) <= tolerance
    )
    consistency_ratio = consistent_gaps / len(gaps)

    if consistency_ratio >= 0.75:
        consistency = "high"
    elif consistency_ratio >= 0.5:
        consistency = "medium"
    else:
        consistency = "low"

    cycle_days = round(median_gap, 1)

    if consistency == "low":
        pattern = "irregular"
        pattern_label = "Irregular"
    elif cycle_days <= 10:
        pattern = "weekly"
        pattern_label = "Weekly"
    elif cycle_days <= 18:
        pattern = "biweekly"
        pattern_label = "Biweekly"
    elif cycle_days <= 45:
        pattern = "monthly"
        pattern_label = "Monthly"
    elif cycle_days <= 75:
        pattern = "bimonthly"
        pattern_label = "Every 2 months"
    elif cycle_days <= 120:
        pattern = "quarterly"
        pattern_label = "Quarterly"
    elif cycle_days <= 220:
        pattern = "semiannual"
        pattern_label = "Every 6 months"
    else:
        pattern = "occasional"
        pattern_label = "Occasional"

    return {
        "cycle_days": cycle_days,
        "average_gap": round(average_gap, 1),
        "median_gap": cycle_days,
        "recent_gap": recent_gap,
        "pattern": pattern,
        "pattern_label": pattern_label,
        "consistency": consistency,
        "consistency_ratio": round(consistency_ratio, 2),
        "gap_count": len(gaps),
    }


def get_analysis_today():
    today = str(os.getenv("ANALYSIS_TODAY", "") or "").strip()

    if not today:
        return datetime.now()

    return datetime.strptime(today, "%Y-%m-%d")


def get_limited_history_follow_up_days():
    raw_days = str(os.getenv("ACTION_PLAN_LIMITED_HISTORY_FOLLOW_UP_DAYS", "60") or "60").strip()
    try:
        return max(1, int(raw_days))
    except ValueError:
        return 60


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


def _safe_order_amount(order):
    try:
        return float(order.get("amount") or 0)
    except (TypeError, ValueError):
        return 0.0


def calculate_order_value_metrics(order_list):
    order_list = order_list or []
    amounts = [_safe_order_amount(order) for order in order_list]
    total_spend_value = round(sum(amounts), 2)
    average_order_value = round((total_spend_value / len(amounts)), 2) if amounts else 0.0
    return {
        "order_count": len(order_list),
        "total_spend_value": total_spend_value,
        "average_order_value": average_order_value,
    }


def apply_value_weight_to_priority(priority_score, value_metrics):
    base_score = float(priority_score or 0)
    value_metrics = value_metrics or {}
    total_spend_value = float(value_metrics.get("total_spend_value") or 0)
    average_order_value = float(value_metrics.get("average_order_value") or 0)

    spend_boost = 0.0
    if total_spend_value >= 100000:
        spend_boost = 0.6
    elif total_spend_value >= 50000:
        spend_boost = 0.45
    elif total_spend_value >= 25000:
        spend_boost = 0.3
    elif total_spend_value >= 10000:
        spend_boost = 0.18
    elif total_spend_value >= 5000:
        spend_boost = 0.1

    average_boost = 0.0
    if average_order_value >= 5000:
        average_boost = 0.45
    elif average_order_value >= 2500:
        average_boost = 0.32
    elif average_order_value >= 1500:
        average_boost = 0.22
    elif average_order_value >= 750:
        average_boost = 0.12
    elif average_order_value >= 250:
        average_boost = 0.05

    return round(base_score + spend_boost + average_boost, 2)


def find_late_customers(customers, today=None):
    results = []

    if today is None:
        today = get_analysis_today()

    for name, orders in customers.items():
        cycle = analyze_order_cycle(orders)
        avg_gap = cycle["cycle_days"]
        valid_order_dates = [
            datetime.strptime(o["order_date"], "%Y-%m-%d")
            for o in orders
            if o.get("order_date")
        ]
        if not valid_order_dates:
            continue
        last_order = max(valid_order_dates)
        days_since_last = (today - last_order).days

        if not avg_gap:
            follow_up_days = get_limited_history_follow_up_days()
            if days_since_last < follow_up_days:
                continue

            value_metrics = calculate_order_value_metrics(orders)
            base_priority = max(1.5, (days_since_last / follow_up_days) * 1.5)
            priority = apply_value_weight_to_priority(base_priority, value_metrics)
            last_activity = get_last_activity_info(orders)
            last_activity_date = last_activity["date"] if last_activity else None
            results.append({
                "customer": name,
                "avg_gap": None,
                "cycle_pattern": "limited_history",
                "cycle_pattern_label": "Limited order history",
                "cycle_consistency": "unknown",
                "days_since_last": days_since_last,
                "priority_score": round(priority, 2),
                "order_count": value_metrics["order_count"],
                "total_spend_value": value_metrics["total_spend_value"],
                "average_order_value_value": value_metrics["average_order_value"],
                "action": "Reconnect for another order",
                "last_activity_date": last_activity_date.strftime("%Y-%m-%d") if last_activity_date else None,
                "last_activity_content": last_activity["content"] if last_activity else "",
                "days_since_last_activity": ((today.date() - last_activity_date.date()).days if last_activity_date else None),
                "limited_history": True,
                "follow_up_threshold_days": follow_up_days,
                "status": "needs_attention",
            })
            continue

        if days_since_last > avg_gap:
            base_priority = days_since_last / avg_gap
            value_metrics = calculate_order_value_metrics(orders)
            priority = apply_value_weight_to_priority(base_priority, value_metrics)
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
                "cycle_pattern": cycle["pattern"],
                "cycle_pattern_label": cycle["pattern_label"],
                "cycle_consistency": cycle["consistency"],
                "days_since_last": days_since_last,
                "priority_score": round(priority, 2),
                "order_count": value_metrics["order_count"],
                "total_spend_value": value_metrics["total_spend_value"],
                "average_order_value_value": value_metrics["average_order_value"],
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
