import os
from pathlib import Path
import json
import time

from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
BASE_DIR = Path(__file__).resolve().parent
_OUTREACH_PREP_CACHE = {}


def get_outreach_context_path():
    raw_path = os.getenv("OUTREACH_CONTEXT_PATH", "").strip()

    if raw_path:
        return Path(raw_path).expanduser()

    return BASE_DIR.parent / "numat-outreach-context.md"


def load_outreach_business_context():
    context_path = get_outreach_context_path()

    try:
        if not context_path.exists():
            return ""

        return context_path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def get_outreach_prep_cache_seconds():
    raw_seconds = os.getenv("OUTREACH_PREP_CACHE_SECONDS", "900").strip()

    try:
        return max(0, int(raw_seconds))
    except ValueError:
        return 900


def get_outreach_context_signature():
    context_path = get_outreach_context_path()

    try:
        if not context_path.exists():
            return ("missing", str(context_path))

        stat = context_path.stat()
        return (str(context_path), stat.st_mtime_ns, stat.st_size)
    except OSError:
        return ("unreadable", str(context_path))


def build_outreach_prep_cache_key(context):
    return (
        os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        get_outreach_context_signature(),
        json.dumps(context, sort_keys=True, default=str),
    )


def get_cached_outreach_prep(cache_key):
    cache_seconds = get_outreach_prep_cache_seconds()

    if not cache_seconds:
        return None

    cached = _OUTREACH_PREP_CACHE.get(cache_key)

    if not cached:
        return None

    if cached["expires_at"] <= time.time():
        _OUTREACH_PREP_CACHE.pop(cache_key, None)
        return None

    return cached["result"]


def cache_outreach_prep(cache_key, result):
    cache_seconds = get_outreach_prep_cache_seconds()

    if not cache_seconds:
        return

    _OUTREACH_PREP_CACHE[cache_key] = {
        "expires_at": time.time() + cache_seconds,
        "result": result,
    }


def fallback_explanation(customer):
    return (
        f"{customer['customer']} needs attention because it has been "
        f"{customer['days_since_last']} days since their last order, compared "
        f"with their usual average gap of {customer['avg_gap']} days. "
        f"Recommended action: {customer['action']}."
    )


def generate_customer_explanation(customer):
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return fallback_explanation(customer)

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)

        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a sales assistant. Explain customer order "
                        "risk in one short, practical sentence. Do not invent "
                        "facts. Base your answer only on the data provided."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Customer: {customer['customer']}\n"
                        f"Average order gap: {customer['avg_gap']} days\n"
                        f"Days since last order: {customer['days_since_last']}\n"
                        f"Priority score: {customer['priority_score']}\n"
                        f"Recommended action: {customer['action']}"
                    )
                }
            ],
            max_output_tokens=80
        )

        explanation = response.output_text.strip()

        if not explanation:
            return fallback_explanation(customer)

        return explanation
    except Exception as error:
        print(f"AI explanation failed: {error}")
        return fallback_explanation(customer)


def add_ai_explanations(customers):
    explained_customers = []

    for customer in customers:
        explained_customer = customer.copy()
        explained_customer["explanation"] = generate_customer_explanation(customer)
        explained_customers.append(explained_customer)

    return explained_customers


def build_outreach_prep_fallback(context):
    customer_name = context.get("customer", "Customer")
    priority_score = context.get("priority_score", 0)
    outbound_count = int(context.get("sales_outreach_sent_count") or 0)
    inbound_count = int(context.get("sales_reply_count") or 0)
    recent_context = context.get("recent_sales_context", [])
    last_subject = recent_context[0]["subject"] if recent_context else ""
    latest_reply_date = context.get("last_reply_date") or "No reply recorded"
    response_rate = context.get("approx_response_rate")
    primary_contact = context.get("primary_contact") or {}
    recommended_contact_name = str(primary_contact.get("name") or "").strip()
    recommended_contact_email = str(primary_contact.get("email") or "").strip()
    contact_role = str(primary_contact.get("role") or "").strip()

    suggested_mode = "Email"
    confidence = "Medium"
    evidence_strength = "Medium"

    if context.get("has_recent_sales_activity"):
        suggested_mode = "Hold"
        confidence = "High"
        evidence_strength = "High"
    elif outbound_count >= 3 and inbound_count == 0:
        suggested_mode = "Call"
        confidence = "Medium"
        evidence_strength = "Low"
    elif outbound_count == 0:
        confidence = "Low"
        evidence_strength = "Low"

    likely_preferred_mode = (
        "Email"
        if inbound_count > 0 else
        ("Call" if outbound_count >= 3 else "Email")
    )
    tone = (
        "Hold off on new contact and review the latest outreach first."
        if suggested_mode == "Hold" else
        ("Direct, operational, and concise" if priority_score and float(priority_score) >= 2 else "Friendly, brief, and practical")
    )
    rationale_bullets = []

    if context.get("has_recent_sales_activity"):
        rationale_bullets.append("Recent sales outreach exists, so another message today risks duplication.")
    else:
        rationale_bullets.append(
            f"{customer_name} is beyond the usual order cycle and is in the current action plan."
        )

    if outbound_count:
        rationale_bullets.append(
            f"Sales outreach history shows {outbound_count} outbound touchpoint(s)."
        )

    if inbound_count:
        rationale_bullets.append(
            f"{inbound_count} observed sales reply/replies suggest email engagement is possible."
        )
    elif outbound_count >= 3:
        rationale_bullets.append("Repeated outbound emails with no reply suggest a call may be more effective than another email.")

    if context.get("customer_services_present"):
        rationale_bullets.append("Customer service traffic exists, but it has been excluded from the sales recommendation.")

    email_subject = (
        f"Checking in on {customer_name}"
        if not last_subject else
        f"Following up on {customer_name}"
    )
    email_body = (
        f"Hi,\n\n"
        f"I wanted to check in because it has been {context.get('days_since_last_order')} days since the last order, "
        f"against a usual cycle of {context.get('average_cycle')} days. "
        f"I wanted to see whether there is anything upcoming that we should be planning around.\n\n"
        f"If it helps, I can also review current needs and timing with you directly.\n\n"
        f"Best regards,"
    )
    call_objective = (
        "Confirm whether there is a genuine operational reason for the reorder gap and agree the next follow-up step."
    )
    call_talking_points = [
        f"Acknowledge the last order date ({context.get('last_order_date')}) and usual cycle ({context.get('average_cycle')} days).",
        "Ask whether there is a current operational reason for the gap in ordering.",
        "Offer a simple next step: quote review, timing check, or quick visit if useful.",
    ]
    voicemail_draft = (
        f"Hi, this is from Numat. I was just checking in because you're beyond your usual reorder timing and "
        f"wanted to see whether there is anything upcoming we can help with. Please call me back when convenient."
    )
    suggested_text_message = (
        f"Hi, it’s Numat checking in. You’re beyond your usual reorder timing, so I wanted to see whether "
        f"you have any upcoming repair needs or damaged mats we should plan around. Happy to keep it brief if easier by text."
    )
    targeting_note = (
        f"Start with {recommended_contact_name or 'the most active contact'}"
        + (f" at {recommended_contact_email}" if recommended_contact_email else "")
        + (f", who looks like an {contact_role.lower()}." if contact_role else ".")
    )

    return {
        "suggested_mode": suggested_mode,
        "tone": tone,
        "confidence": confidence,
        "rationale_bullets": rationale_bullets[:4],
        "sales_outreach_count": outbound_count,
        "observed_reply_count": inbound_count,
        "approx_response_rate": response_rate,
        "last_reply_date": latest_reply_date,
        "likely_preferred_mode": likely_preferred_mode,
        "recommended_contact_name": recommended_contact_name,
        "recommended_contact_email": recommended_contact_email,
        "targeting_note": targeting_note,
        "observed_pattern": (
            "Replies are present in the sales history and suggest email is workable."
            if inbound_count else
            "There is little or no reply evidence in the sales history, so the recommendation is more cautious."
        ),
        "evidence_strength": evidence_strength,
        "customer_services_present": bool(context.get("customer_services_present")),
        "email_subject": email_subject,
        "email_body": email_body,
        "call_objective": call_objective,
        "call_talking_points": call_talking_points,
        "voicemail_draft": voicemail_draft,
        "suggested_text_message": suggested_text_message,
    }


def generate_outreach_prep(context):
    api_key = os.getenv("OPENAI_API_KEY")
    cache_key = build_outreach_prep_cache_key(context)
    cached_result = get_cached_outreach_prep(cache_key)

    if cached_result is not None:
        return cached_result

    if not api_key:
        result = build_outreach_prep_fallback(context)
        cache_outreach_prep(cache_key, result)
        return result

    fallback = build_outreach_prep_fallback(context)
    business_context = load_outreach_business_context()

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a practical B2B sales assistant. Recommend an outreach mode, tone, and draft using only the provided data. "
                        "Treat customer service traffic as supporting context, not the main sales signal. "
                        "Use the supplied business context to correctly interpret what Numat does, what damaged mats usually mean, and how repair-service outreach differs from complaint handling. "
                        "Treat the latest sales outreach item as a high-priority signal. If the most recent sales outreach is a visit note, meeting summary, or post-visit follow-up, use that as the primary anchor for the recommendation, rationale, and draft unless a newer reply clearly changes the situation. "
                        "Use the inferred contact signals to decide who the most appropriate target is, and tune the tone for their likely role and influence. "
                        "Reference the latest sales outreach concretely when it is commercially relevant, rather than drifting into a generic follow-up. "
                        "Pay close attention to dates. Do not describe an activity as recent unless it happened within the last 90 days relative to the current analysis date. "
                        "If the latest relevant outreach or reply is older than 90 days, describe it as older, historical, previous, or the latest recorded activity instead. "
                        "If an older message is still important, make that clear without implying it happened recently. "
                        "Return strict JSON with keys: suggested_mode, tone, confidence, rationale_bullets, sales_outreach_count, "
                        "observed_reply_count, approx_response_rate, last_reply_date, likely_preferred_mode, observed_pattern, "
                        "evidence_strength, customer_services_present, recommended_contact_name, recommended_contact_email, targeting_note, email_subject, email_body, call_objective, call_talking_points, voicemail_draft, suggested_text_message. "
                        "rationale_bullets and call_talking_points must be arrays of short strings. "
                        "Do not invent facts or channels not supported by the history."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(context, indent=2),
                },
                {
                    "role": "user",
                    "content": (
                        "Business context for interpreting this outreach:\n\n"
                        + (business_context or "No additional business context supplied.")
                    ),
                },
            ],
            max_output_tokens=1400,
        )

        raw_text = response.output_text.strip()
        if not raw_text:
            cache_outreach_prep(cache_key, fallback)
            return fallback

        parsed = json.loads(raw_text)
        if not isinstance(parsed, dict):
            cache_outreach_prep(cache_key, fallback)
            return fallback

        result = fallback.copy()
        for key, value in parsed.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, list) and not value:
                continue
            result[key] = value
        cache_outreach_prep(cache_key, result)
        return result
    except Exception as error:
        print(f"AI outreach prep failed: {error}")
        cache_outreach_prep(cache_key, fallback)
        return fallback
