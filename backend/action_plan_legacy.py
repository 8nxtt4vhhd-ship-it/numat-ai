"""Preserved pre-November-2026 Action Plan prioritisation rules.

This module is intentionally not used by the live Action Plan.  It keeps the
former order-cycle/value scoring in one place so it can be reinstated without
reconstructing the thresholds from history.
"""


def apply_value_weight(priority_score, total_spend=0, average_order_value=0):
    spend_boost = 0.0
    if total_spend >= 100000:
        spend_boost = 0.6
    elif total_spend >= 50000:
        spend_boost = 0.45
    elif total_spend >= 25000:
        spend_boost = 0.3
    elif total_spend >= 10000:
        spend_boost = 0.18
    elif total_spend >= 5000:
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
    return round(float(priority_score or 0) + spend_boost + average_boost, 2)


def legacy_priority_score(days_since_last_order, usual_cycle_days, total_spend=0, average_order_value=0):
    """Former score: overdue ratio plus lifetime and average-order boosts."""
    if usual_cycle_days:
        base_score = days_since_last_order / float(usual_cycle_days)
    else:
        # Customers with fewer than three orders became eligible after 60 days.
        base_score = max(1.5, (days_since_last_order / 60.0) * 1.5)
    return apply_value_weight(base_score, total_spend, average_order_value)


LEGACY_RULES = {
    "minimum_live_score": 1.5,
    "limited_history_follow_up_days": 60,
    "recent_contact_hold_days": 14,
    "email_then_call_or_text_cooloff_days": 30,
    "live_queue_limit": 10,
    "sort": "priority score descending",
}
