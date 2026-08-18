import os
from pathlib import Path
import json
import time

from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
BASE_DIR = Path(__file__).resolve().parent
_OUTREACH_PREP_CACHE = {}
_OUTREACH_PREP_PROMPT_VERSION = "2026-08-18-speech-act-provenance-16"
_STRATEGIC_DISCOVERY_PROMPT_VERSION = "2026-07-21-openai-strategic-discovery-1"

OUTREACH_EMAIL_STYLE_GUIDANCE = (
    "Write the customer email in concise American business English. Use ASD-STE100-inspired principles for clarity, "
    "but do not imitate a technical manual or claim strict STE compliance. Use short sentences, active voice, common words, "
    "and concrete facts. Be factual and straightforward, not sales-pitchy. Remove soft British constructions such as "
    "'I wondered whether', 'I was hoping', 'would you be able to', and 'it would be great'. Avoid vague location language "
    "such as 'in your area', 'near you', or 'in the region'. If the supplied evidence names Chicago, say Chicago. "
    "Never invent a city, trip, visit, date, customer meeting, or reason for travel. Every opening fact must lead naturally "
    "to the reason for writing. Use this logical structure when a trigger exists: specific trigger, why it relates to this "
    "customer, then one direct low-pressure question. For example, connect 'I was visiting customers in Chicago' to "
    "'it reminded me that it has been a while since your last repair order' before asking about current repair needs. "
    "Do not use that example unless Chicago and the visit are supplied facts. Do not place unrelated facts next to each other. Prefer 45 to 85 words, "
    "and use fewer words when the evidence is sparse. Do not use promotional phrases such as 'unlock value', 'exciting opportunity', "
    "'partner with you', 'tailored solution', or 'help take your business to the next level'. Choose one historical thread only. "
    "Do not combine a missed visit, an old repair note, an order gap, and a general check-in merely because they are all available. "
    "Use the single fact that best supports today's question. State it plainly in no more than two short sentences, then ask one direct question. "
    "If a missed visit is the chosen thread, say 'We missed each other' or 'We didn't get a chance to meet'. Never say 'due to schedule a visit' "
    "or 'due to be in the area'. Do not use 'I wanted to check'. Do not offer to arrange pickup or start logistics until the customer confirms "
    "a current repair need or a batch ready for collection."
)

RECENT_REPLY_ANCHOR_DAYS = 30
RECENT_OUTREACH_ANCHOR_DAYS = 30


def has_stale_logistics_signal(context):
    return bool(context.get("stale_logistics_signal"))


def draft_looks_like_active_logistics(text):
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False

    logistics_phrases = [
        "mats ready",
        "ready for repair pickup",
        "schedule a pickup",
        "arrange the pickup",
        "preferred pickup",
        "dock hours",
        "dock times",
        "pallet or cage count",
        "pallet count",
        "cage count",
        "shipping or dock",
        "coordinate the next step",
        "help get things moving",
    ]
    return any(phrase in lowered for phrase in logistics_phrases)


def draft_violates_outreach_style(text):
    lowered = str(text or "").strip().lower()
    disallowed_phrases = [
        "in your area",
        "near you",
        "in the region",
        "i wondered whether",
        "i was hoping",
        "would you be able to",
        "it would be great",
        "due to schedule a visit",
        "due to be in the area",
        "i wanted to check",
        "unlock value",
        "exciting opportunity",
        "partner with you",
        "tailored solution",
        "take your business to the next level",
    ]
    return any(phrase in lowered for phrase in disallowed_phrases) or str(text or "").count("?") > 1


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


def get_cold_outreach_context_path():
    raw_path = os.getenv("OUTREACH_COLD_CONTEXT_PATH", "").strip()

    if raw_path:
        return Path(raw_path).expanduser()

    return BASE_DIR.parent / "numat-cold-outreach-context.md"


def load_cold_outreach_context():
    context_path = get_cold_outreach_context_path()

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


def get_cold_outreach_context_signature():
    context_path = get_cold_outreach_context_path()

    try:
        if not context_path.exists():
            return ("missing", str(context_path))

        stat = context_path.stat()
        return (str(context_path), stat.st_mtime_ns, stat.st_size)
    except OSError:
        return ("unreadable", str(context_path))


def discover_strategic_contacts_with_openai(
    organization_name,
    *,
    region="",
    function="",
    limit=20,
):
    organization_name = str(organization_name or "").strip()
    region = str(region or "").strip()
    function = str(function or "").strip()
    limit = max(1, min(int(limit or 20), 50))
    api_key = os.getenv("OPENAI_API_KEY", "").strip()

    if not organization_name:
        return {"status": "missing_organization", "results": [], "error_message": ""}

    if not api_key:
        return {"status": "missing_api_key", "results": [], "error_message": ""}

    system_prompt = (
        "You help a sales operations app identify likely strategic contacts at large uniform-services companies. "
        "Only return people who are plausibly real and relevant. "
        "Prioritize executive and senior decision-makers in operations, production, procurement, facilities, supply chain, or general leadership. "
        "Include regional directors and other regional leadership when they clearly sit above branch level. "
        "Prefer uniform-services leadership only. Avoid HR, recruiting, talent, marketing, finance, retail, medical, sports, entertainment, education, foodservice, and unrelated divisions unless the requested function clearly asks for them. "
        "If you are not reasonably confident a person belongs to the requested organisation, leave them out. "
        "Prefer candidates with at least one verifiable signal such as a work email, phone number, or LinkedIn profile. "
        "When enough plausible candidates exist, fill the requested limit instead of stopping early. "
        "Return strict JSON only with this shape: "
        "{\"results\":[{\"name\":\"...\",\"title\":\"...\",\"organization_name\":\"...\",\"city\":\"...\",\"region\":\"...\",\"country\":\"...\",\"email\":\"...\",\"phone\":\"...\",\"linkedin_url\":\"...\",\"reason\":\"...\"}]}. "
        "Use empty strings for unknown fields. "
        "Do not include markdown. "
        "Do not include commentary outside the JSON."
    )
    user_payload = {
        "prompt_version": _STRATEGIC_DISCOVERY_PROMPT_VERSION,
        "organization_name": organization_name,
        "region": region,
        "function": function,
        "limit": limit,
        "goal": (
            "Find likely strategic contacts for this organisation. "
            "Bias toward uniform-services leadership and practical decision-makers."
        ),
    }

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": json.dumps(user_payload, indent=2),
                },
            ],
            max_output_tokens=3600,
        )

        raw_text = response.output_text.strip()
        if not raw_text:
            return {
                "status": "error",
                "results": [],
                "error_message": "OpenAI returned no discovery data.",
                "system_prompt": system_prompt,
                "user_payload": user_payload,
                "raw_response_text": raw_text,
            }

        parsed = json.loads(raw_text)
        if not isinstance(parsed, dict):
            return {
                "status": "error",
                "results": [],
                "error_message": "OpenAI returned malformed discovery data.",
                "system_prompt": system_prompt,
                "user_payload": user_payload,
                "raw_response_text": raw_text,
            }

        raw_results = parsed.get("results") or []
        if not isinstance(raw_results, list):
            raw_results = []

        cleaned = []
        for item in raw_results:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            title = str(item.get("title") or "").strip()
            if not name or not title:
                continue
            cleaned.append(
                {
                    "name": name,
                    "title": title,
                    "organization_name": str(item.get("organization_name") or organization_name).strip(),
                    "city": str(item.get("city") or "").strip(),
                    "region": str(item.get("region") or region).strip(),
                    "country": str(item.get("country") or "").strip(),
                    "email": str(item.get("email") or "").strip(),
                    "phone": str(item.get("phone") or "").strip(),
                    "linkedin_url": str(item.get("linkedin_url") or "").strip(),
                    "reason": str(item.get("reason") or "").strip(),
                }
            )

        return {
            "status": "ok",
            "results": cleaned[:limit],
            "error_message": "",
            "system_prompt": system_prompt,
            "user_payload": user_payload,
            "raw_response_text": raw_text,
        }
    except Exception as error:
        return {
            "status": "error",
            "results": [],
            "error_message": str(error),
            "system_prompt": system_prompt,
            "user_payload": user_payload,
            "raw_response_text": "",
        }


def build_outreach_prep_cache_key(context):
    return (
        os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        _OUTREACH_PREP_PROMPT_VERSION,
        get_outreach_context_signature(),
        get_cold_outreach_context_signature() if context.get("is_cold_outreach") else None,
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


def get_contact_first_name(name):
    raw_name = str(name or "").strip()
    if not raw_name:
        return ""
    first_part = raw_name.split()[0].strip()
    if not first_part or "@" in first_part or first_part.lower() in {"unknown", "contact"}:
        return ""
    return first_part.title()


def summarise_outreach_signal(*values, limit=80):
    for value in values:
        text = " ".join(str(value or "").strip().split())
        if not text:
            continue
        text = text.strip(" -:;,")
        if not text:
            continue
        if len(text) > limit:
            text = text[: limit - 1].rstrip(" ,;:-") + "…"
        return text
    return ""


def summarise_recent_outreach_context(recent_context, max_items=2):
    signals = []
    for item in recent_context[:max_items]:
        signal = summarise_outreach_signal(item.get("subject"), item.get("preview"))
        if signal:
            signals.append(signal)
    return signals


def format_elapsed_time_label(days_value):
    if not isinstance(days_value, (int, float)):
        return "a while"

    days = int(days_value)
    if days < 30:
        return f"{days} days"
    if days < 60:
        return "about a month"
    if days < 90:
        return "a couple of months"
    if days < 180:
        months = max(3, round(days / 30))
        return f"about {months} months"
    if days < 270:
        return "around six months"
    if days < 365:
        months = round(days / 30)
        return f"about {months} months"
    if days < 550:
        return "over a year"
    if days < 730:
        return "well over a year"

    years = round(days / 365)
    return f"about {years} years"


def build_text_tone_email_body(context, recommended_contact_name="", is_stale=False):
    greeting_name = get_contact_first_name(recommended_contact_name)
    greeting = f"Hi {greeting_name}," if greeting_name else "Hi,"
    recent_context = context.get("recent_sales_context", []) or []
    recent_signals = summarise_recent_outreach_context(recent_context)
    latest_reply_signal = summarise_outreach_signal(
        context.get("latest_replied_outreach_subject"),
        context.get("latest_replied_outreach_preview"),
    )
    latest_sales_signal = summarise_outreach_signal(
        context.get("latest_sales_outreach_subject"),
        context.get("latest_sales_outreach_preview"),
    )
    latest_sales_days = context.get("days_since_latest_sales_outreach")
    latest_reply_days = context.get("days_since_latest_replied_outreach")
    has_recent_sales_activity = bool(context.get("has_recent_sales_activity"))
    customer_name = str(context.get("customer") or "your team").strip()
    cycle_days = context.get("average_cycle")
    days_since_last_order = context.get("days_since_last_order")
    inbound_count = int(context.get("sales_reply_count") or 0)
    outbound_count = int(context.get("sales_outreach_sent_count") or 0)
    likely_preferred_mode = str(context.get("likely_preferred_mode") or "").strip().lower()
    primary_activity_type = str(context.get("primary_activity_type") or "").strip().lower()

    if (
        latest_reply_signal
        and isinstance(latest_reply_days, (int, float))
        and latest_reply_days <= RECENT_REPLY_ANCHOR_DAYS
    ):
        paragraphs = [
            "Thanks for the update.",
            "Is there anything you need us to arrange on the repair side?",
        ]
    elif (
        (likely_preferred_mode == "email" or inbound_count > 0)
        and isinstance(latest_sales_days, (int, float))
        and latest_sales_days <= RECENT_OUTREACH_ANCHOR_DAYS
        and outbound_count >= 2
    ):
        # A deterministic fallback cannot reliably interpret or paraphrase free-form
        # CRM notes. Keep it useful without quoting fragments back to the customer.
        paragraphs = [
            "Do you have any mats that need repairing at the moment?",
            "If so, send me an approximate quantity and I can confirm the next step.",
        ]
    elif (
        len(recent_signals) >= 2
        and isinstance(latest_sales_days, (int, float))
        and latest_sales_days <= RECENT_OUTREACH_ANCHOR_DAYS
        and (likely_preferred_mode == "visit" or primary_activity_type == "meeting")
    ):
        paragraphs = [
            "Is there anything from our last visit that still needs sorting?",
            "If so, send me the details and I can confirm the next step.",
        ]
    elif outbound_count >= 3 and inbound_count == 0 and recent_signals:
        paragraphs = [
            "We have tried a few times, so I’ll keep this brief.",
            "Do you have any damaged mats building up that need repair?",
            "If not, I’ll leave it there for now.",
        ]
    elif has_recent_sales_activity and latest_sales_signal and latest_sales_days is not None and latest_sales_days <= RECENT_OUTREACH_ANCHOR_DAYS:
        paragraphs = [
            "Does this still need any action from us?",
            "If so, send me the details and I can confirm the next step.",
        ]
    elif is_stale and latest_sales_signal:
        paragraphs = [
            "It has been a while since we last worked with you on repairs.",
            "Do you have any mats that need repair now?",
            "If so, send me an approximate quantity and I can confirm the next step.",
        ]
    elif is_stale:
        paragraphs = [
            "It has been a while since your last repair order.",
            "Do you have any mats that need repair now?",
        ]
    elif isinstance(days_since_last_order, (int, float)):
        elapsed_label = format_elapsed_time_label(days_since_last_order)
        paragraphs = [
            f"It has been {elapsed_label} since the last repair order for {customer_name}.",
            "Do you have any repair work coming up?",
            "If so, send me an approximate quantity and I can confirm the next step.",
        ]
    else:
        paragraphs = [
            "Do you have any mats that need repairing at the moment?",
            "If so, send me an approximate quantity and I can confirm the next step.",
        ]

    return "\n\n".join([greeting, *paragraphs, "Thanks,"])


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
    latest_sales_days = context.get("days_since_latest_sales_outreach")
    latest_reply_date = context.get("last_reply_date") or "No reply recorded"
    response_rate = context.get("approx_response_rate")
    primary_activity_type = str(context.get("primary_activity_type") or "").strip()
    activity_type_pattern = str(context.get("activity_type_pattern") or "").strip()
    activity_type_counts = context.get("activity_type_counts") or {}
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

    likely_preferred_mode = str(context.get("likely_preferred_mode") or "").strip() or (
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

    if primary_activity_type and primary_activity_type != "Unknown":
        rationale_bullets.append(
            f"The outreach history is mainly {primary_activity_type.lower()}-based."
        )

    if inbound_count:
        rationale_bullets.append(
            f"{inbound_count} observed sales reply/replies suggest email engagement is possible."
        )
    elif outbound_count >= 3:
        rationale_bullets.append("Repeated outbound emails with no reply suggest a call may be more effective than another email.")

    if context.get("customer_services_present"):
        rationale_bullets.append("Customer service traffic exists, but it has been excluded from the sales recommendation.")

    email_subject = "Any mats for repair?"
    is_stale = has_stale_logistics_signal(context) or (latest_sales_days is not None and latest_sales_days > 90)
    email_body = build_text_tone_email_body(
        context,
        recommended_contact_name=recommended_contact_name,
        is_stale=is_stale,
    )
    call_objective = (
        "Confirm whether there is a genuine operational reason for the reorder gap and agree the next follow-up step."
    )
    if primary_activity_type == "Meeting":
        call_objective = "Build on the existing meeting/visit history and confirm the most useful next step from that discussion."
    call_talking_points = [
        f"Acknowledge the last order date ({context.get('last_order_date')}) and the customer's usual repair rhythm ({context.get('order_cycle_pattern') or context.get('average_cycle')}).",
        "Ask whether anything has changed operationally or whether new damaged mats may be building up.",
        "Offer a simple next step: quote review, timing check, or quick visit if useful.",
    ]
    voicemail_draft = (
        f"Hi, this is from Numat. I was just checking in to see whether there is anything upcoming we can help with on the repair side. "
        f"Please call me back when convenient."
    )
    text_greeting_name = get_contact_first_name(recommended_contact_name)
    suggested_text_message = (
        f"Hi {text_greeting_name}, it’s Numat checking in. " if text_greeting_name else "Hi, it’s Numat checking in. "
    ) + (
        "Just wanted to see whether you have any damaged mats or upcoming repair needs we should plan around. "
        "Happy to keep it brief if that’s easier."
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
            activity_type_pattern
            or (
                "Replies are present in the sales history and suggest email is workable."
                if inbound_count else
                "There is little or no reply evidence in the sales history, so the recommendation is more cautious."
            )
        ),
        "evidence_strength": evidence_strength,
        "customer_services_present": bool(context.get("customer_services_present")),
        "email_subject": email_subject,
        "email_body": email_body,
        "call_objective": call_objective,
        "call_talking_points": call_talking_points,
        "voicemail_draft": voicemail_draft,
        "suggested_text_message": suggested_text_message,
        "activity_type_counts": activity_type_counts,
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
    cold_outreach_context = load_cold_outreach_context() if context.get("is_cold_outreach") else ""

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
                        "Use CRM activity type as a real signal. Pay attention to whether the history is made up mostly of email, calls, meetings/visits, LinkedIn, or a mix. "
                        "That should influence the recommended mode, tone, observed pattern, and whether the relationship looks meeting-led, email-led, or call-led. "
                        "When writing the email draft, keep the tone closer to a good business text than a formal sales letter: practical, conversational, direct, and easy to reply to. "
                        "The team's standard is simple and purposeful. The draft must feel sincere, familiar, and written by a real salesperson who has read the conversation. It must never feel distant, performative, over-polished, or like generic sales copy. "
                        "Avoid sales filler such as 'I wanted to touch base', 'reach out', 'circle back', 'connect', or 'whenever you have a moment'. Start with the useful point. "
                        "Read recent_sales_context as a conversation in date order: inbound items are the customer's words and must be answered or acknowledged when they are still relevant; outbound items are examples of how the Numat salesperson naturally writes. "
                        "Preserve attribution and meaning exactly. Never rewrite a fact from an outbound salesperson note as something the customer mentioned or said. Use 'you mentioned' only for an inbound customer statement. Distinguish an observation from a question or conditional offer: 'if you have any damaged or wavy mats' means the salesperson asked about them; it does not mean those mats existed. Ask that question directly rather than writing 'I noted some mats'. Use 'we discussed' only when the records clearly show a two-way discussion. "
                        "Adapt to the salesperson's established level of formality, sentence length, greeting, sign-off, and vocabulary when there are usable outbound email examples. Match the voice, but do not copy whole sentences or reproduce typos. "
                        "CRM entries may be shorthand notes rather than customer-facing prose. Never paste, quote, or mechanically join note fragments in the draft. In particular, do not write constructions such as 'our notes around X and Y'. First understand the underlying point, then respond in normal language—or omit it if its meaning is unclear. "
                        "Continue the actual conversation rather than merely announcing a follow-up. If the customer asked a question or gave an update, address it directly before making the next ask. Do not claim to answer something unless the supplied history supports the answer. "
                        "Before drafting, silently choose one primary purpose supported by the evidence. Use this priority order: respond to a live customer question or update; address a recorded objection or decision blocker; continue a recent visit or agreed action; close the loop after repeated unanswered contact; re-open a previously responsive relationship; identify the right owner when contact evidence is weak; otherwise check for a current repair need. Do not combine several purposes in one email. "
                        "For dormant accounts, do not assume every useful email asks whether mats are being set aside. Where supported, vary the substance instead: ask whether the repair programme is still active, ask what changed, confirm whether this is still the right contact, refer to a genuine earlier blocker, or offer one relevant next step. Variation must come from evidence, not synonyms or arbitrary creativity. "
                        "If several outbound attempts have received no reply, do not send another normal check-in. Keep it especially short and either ask whether the contact is still responsible or give them an easy way to say the timing is not right. "
                        "When sales_outreach_sent_count is 3 or more and sales_reply_count is 0, this is mandatory: do not ask again whether mats are building up and do not add a value proposition. Ask whether this is still the right contact, or briefly close the loop. "
                        "If the account previously replied or ordered regularly but has gone quiet, write as an existing supplier relationship, not as a cold prospect and not as though a transaction is currently active. "
                        "When an older reply records a blocker, acknowledge it without interrogating the customer or listing possible causes. Ask only whether the situation has changed or who now owns the decision. "
                        "Write for busy operational people who may be moving between the office and production floor. "
                        "Keep emails concise and specific. Prefer 2 or 3 short sentences plus greeting and sign-off. Use exactly one question or next step. "
                        "For dormant existing customers, usually keep the body between 45 and 85 words unless a current customer question needs a different length. "
                        "Anchor each email to one concrete point, not several. "
                        "End with one clear ask or next step. "
                        "Avoid vague filler, stretched phrasing, and soft lead-ins. "
                        "Prefer present-day, practical wording over CRM-style recap wording. "
                        "Do not make the draft sound like it is quoting old notes back at the customer unless the contact was genuinely recent. "
                        "Do not default to a generic 'just checking in' email when there is concrete recent outreach or reply context available. "
                        "Use the broader shape of the recent communication history, not just the latest item. If there were several recent notes, reflect that naturally rather than writing every email as a follow-up to a single message. "
                        "Vary the structure and wording meaningfully between customers. Do not keep reusing the same three-paragraph shape with only one swapped reference. "
                        "Avoid repeatedly using phrases like 'wanted to pick back up', 'just checking in', 'if anything is starting to build up again', 'if priorities have shifted', 'no pressure', or 'work around whatever timing is easiest' across drafts unless the context genuinely demands them. "
                        "If the history is meeting-led or visit-led, write like someone continuing an operational relationship rather than reviving an old email thread. "
                        "If the history shows several unanswered attempts, acknowledge that lightly and either offer to leave it there or ask for a quick update. "
                        "If the history shows replies, reflect the thread as an ongoing conversation rather than reducing it to one isolated message. "
                        "Use different openings depending on the evidence: some can anchor to timing, some to the broader thread, some to operational planning, some to the customer's order rhythm. "
                        "Reference the latest sales outreach concretely when it is commercially relevant, rather than drifting into a generic follow-up. "
                        "Prefer concrete verbs like plan, arrange, book, send, confirm, check, or schedule. "
                        "Use at most one direct question unless the context strongly requires two. "
                        "Use a short, natural subject that reflects the real next step. Do not use 'Following up on [customer name]' or put the customer's full account/location label in the subject. "
                        "Avoid vague subjects beginning with 'Checking in'. "
                        "End with 'Thanks,' only. Do not add '[Your Name]', a fabricated name, or a signature because the sending mailbox adds the salesperson's signature. "
                        "Do not anchor the email to the last replied message if that reply is older than 30 days. At that point it is background context, not an active thread. "
                        "Do not anchor the email to the latest sales outreach if it is older than 30 days unless the note is still clearly operationally live. "
                        "When the last reply or latest outreach is older than 30 days, do not write 'following up on your note', 'following up on my last note', or 'looking back at our recent notes'. Use a present-day opener instead. "
                        "Avoid the word 'steer' in the draft. Prefer 'let me know', 'send me a note', or 'send me an update'. "
                        "Do not introduce pallet minimums, weights, pricing claims, process explanations, sustainability points, or repair thresholds merely to make the email more specific. Include such detail only when it directly answers the customer or removes a recorded blocker. "
                        "Do not promise or imply that a pickup will be arranged unless the supplied history shows enough current volume or an active logistics conversation. Otherwise offer to check, discuss, or suggest the next step. "
                        "It is fine to sound lightly conversational and personal, but keep it short and commercially clear. "
                        "Pay close attention to dates. Do not describe an activity as recent unless it happened within the last 90 days relative to the current analysis date. "
                        "If the latest relevant outreach or reply is older than 90 days, describe it as older, historical, previous, or the latest recorded activity instead. "
                        "If an older message is still important, make that clear without implying it happened recently. "
                        "If the latest meaningful signal is an older pickup request, scheduling request, repair-logistics note, or other operational request that is older than 90 days, do not write as though the pickup or repair process is still active. "
                        "Treat it as historical evidence of prior repair interest only. In those cases, ask whether there is any current repair need, whether mats are still being set aside, or whether priorities have changed. "
                        "Do not assume we should arrange collection, confirm dock times, or continue logistics unless there is a genuinely recent signal showing the process is still active. "
                        "For older logistics signals, do not thank the customer for having mats ready, do not say they currently have mats ready, and do not imply pickup is already being coordinated. "
                        "If the outreach is clearly net-new or dormant reactivation, use the cold-outreach addendum as style and structure guidance. "
                        "If the outreach has a live relationship, recent visit, recent operational discussion, or active thread, do not force cold-email structure onto it. "
                        "Return strict JSON with keys: suggested_mode, tone, confidence, rationale_bullets, sales_outreach_count, "
                        "observed_reply_count, approx_response_rate, last_reply_date, likely_preferred_mode, observed_pattern, "
                        "evidence_strength, customer_services_present, recommended_contact_name, recommended_contact_email, targeting_note, email_subject, email_body, call_objective, call_talking_points, voicemail_draft, suggested_text_message. "
                        "rationale_bullets and call_talking_points must be arrays of short strings. "
                        "Do not invent facts or channels not supported by the history. "
                        + OUTREACH_EMAIL_STYLE_GUIDANCE
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
                {
                    "role": "user",
                    "content": (
                        "Cold outreach addendum for use only when this outreach is net-new or dormant:\n\n"
                        + (cold_outreach_context or "No cold outreach addendum supplied.")
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

        if has_stale_logistics_signal(context) and draft_looks_like_active_logistics(result.get("email_body", "")):
            result["email_body"] = fallback["email_body"]
            result["targeting_note"] = (
                "Previous repair logistics contact appears historical rather than active, "
                "so the outreach should check whether there is any current repair need instead of assuming pickup is still being arranged."
            )
        if draft_violates_outreach_style(result.get("email_body", "")):
            result["email_body"] = fallback["email_body"]
            result["targeting_note"] = "The generated draft used vague, overly soft, or promotional language, so a direct factual fallback was used."

        cache_outreach_prep(cache_key, result)
        return result
    except Exception as error:
        print(f"AI outreach prep failed: {error}")
        cache_outreach_prep(cache_key, fallback)
        return fallback


def generate_action_plan_email_draft(context):
    """Generate only the two fields needed by the Action Plan focus view."""
    fallback = build_outreach_prep_fallback(context)
    prepared_sales_context = redact_expired_scheduling_notes(context.get("recent_sales_context"))
    prepared_sales_context = redact_intent_resolved_by_later_order(prepared_sales_context)
    primary_fact = select_primary_outreach_fact(prepared_sales_context)
    primary_fact_fallback = build_primary_fact_fallback_email(context, primary_fact)
    if primary_fact_fallback:
        fallback = {**fallback, **primary_fact_fallback}
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "email_subject": fallback["email_subject"],
            "email_body": fallback["email_body"],
        }

    payload_keys = [
        "customer",
        "analysis_date",
        "days_since_last_order",
        "last_order_date",
        "average_cycle",
        "order_cycle_pattern",
        "order_count",
        "sales_outreach_sent_count",
        "sales_reply_count",
        "days_since_latest_sales_outreach",
        "days_since_latest_replied_outreach",
        "primary_activity_type",
        "likely_preferred_mode",
        "is_cold_outreach",
        "stale_logistics_signal",
        "later_order_recorded",
        "recent_sales_context",
        "primary_contact",
    ]
    draft_context = {key: context.get(key) for key in payload_keys}
    draft_context["recent_sales_context"] = prepared_sales_context
    draft_context["primary_outreach_fact"] = primary_fact
    cache_key = (
        "action-plan-email-only",
        _OUTREACH_PREP_PROMPT_VERSION,
        os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        json.dumps(draft_context, sort_keys=True, default=str),
    )
    cached_result = get_cached_outreach_prep(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "Write one short customer email for NuMat, which repairs damaged mats for industrial laundry and mat-service customers. "
                        "Return strict JSON with only email_subject and email_body. Use only supplied facts. "
                        "Read recent_sales_context as a conversation: inbound is the customer's wording to respond to; outbound shows the salesperson's natural voice. "
                        "Use primary_outreach_fact as the default subject of the email. It was selected by a deterministic commercial-priority step. A specific unresolved repair signal or recorded blocker outranks years since the last order, general account dormancy, and expired scheduling. Depart from it only when a later order clearly resolved it or using it would contradict a newer customer message. A brief acknowledgement of the latest inbound message may precede the primary fact when it connects naturally. Preserve both speaker and speech act: direction Inbound means the customer wrote it; direction Outbound means the NuMat salesperson wrote it. speech_act salesperson_question means the salesperson asked or conditionally offered something—it is not evidence that the customer had those mats. Never convert 'if you have any damaged or wavy mats' into 'you mentioned', 'we discussed', 'I noted some mats', or a claim that mats existed. Ask the question directly instead. Use 'we discussed' only when the records clearly show a two-way discussion. "
                        "Treat analysis_date as today. Every CRM item's date and age_days determine when its wording was true. Relative phrases inside an old item—such as 'tomorrow', 'this week', 'next week', 'later this month', or 'next month'—are relative to that item's date, not analysis_date. "
                        "Sequence CRM activity against last_order_date. If a note anticipated mats, a repair order, or a pickup and later_order_recorded is true, treat that anticipated need as fulfilled by the later order unless a newer record explicitly says otherwise. Do not ask for those same mats or describe that old plan as still pending. You may refer to the later order itself when useful. "
                        "For an expired scheduling proposal with no later recorded outcome, assume the proposed visit or meeting did not take place. If that missed meeting is the single best reason to write, say simply 'We missed each other' or 'We didn't get a chance to meet', then ask one direct question about arranging another time. Do not combine it with an unrelated old repair note. Never ask whether the visit happened, imply that it did happen, or invent why it was missed. "
                        "Do not invent a new visit date, travel plan, meeting, or availability. You may ask whether the customer would like to arrange a new time, but only state specific new timing when the supplied current context explicitly supports it. "
                        "Sound simple, purposeful, sincere, familiar, and plainspoken—not polished sales copy. Match usable outbound style without copying errors. "
                        "Choose one purpose: answer a live update; address a recorded blocker; continue an agreed action; close the loop after unanswered attempts; re-open a responsive relationship; identify the right contact; or check for a current repair need. "
                        "If outbound attempts are 3 or more with no replies, do not ask about damaged-mat volume or pitch benefits; ask whether this is still the right contact or close the loop. "
                        "If an older reply records a blocker, ask only whether it changed or who owns the decision; do not list speculative causes. "
                        "For dormant existing customers, write as an existing supplier and usually use 45 to 85 body words. Do not reintroduce NuMat or stack generic benefits. "
                        "Give the email enough substance to be worth sending. When evidence allows, include: one natural reference to the specific history or relationship; one practical reason it matters now; and one easy next step. Do not pad the email when the history is sparse. "
                        "A recorded blocker should normally produce 2 or 3 useful body sentences: acknowledge the blocker, say briefly how NuMat can help if it has changed, then ask one direct question. Do not reduce it to only 'has the situation changed?' "
                        "Use only one conversational ask. Do not ask whether the situation changed and who the right contact is in the same email. Choose the question that best fits the evidence. "
                        "Avoid empty offers such as 'we can help if you are ready'. When a useful offer is supported, name it plainly—for example reviewing likely repair candidates or comparing repair with replacement—without turning it into a pitch. "
                        "Use 2 or 3 short body sentences and exactly one clear question or next step. When the context is simple, prefer the pattern: one factual sentence followed by one direct question. Format email_body as a normal email with a blank line after the greeting and a blank line before the final 'Thanks,'. "
                        "Never quote or mechanically join CRM note fragments. Do not use 'touch base', 'reach out', 'circle back', 'just checking in', or vague 'Checking in' subjects. "
                        "Do not add pallet minimums, weights, pricing, sustainability claims, guides, process claims, or pickup promises unless directly required by a current message. "
                        "Use a short practical subject. Do not include the full customer/location label or 'Following up on [customer]'. "
                        + OUTREACH_EMAIL_STYLE_GUIDANCE
                    ),
                },
                {"role": "user", "content": json.dumps(draft_context, indent=2)},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "action_plan_email_draft",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "email_subject": {"type": "string"},
                            "email_body": {"type": "string"},
                        },
                        "required": ["email_subject", "email_body"],
                        "additionalProperties": False,
                    },
                }
            },
            max_output_tokens=500,
        )
        parsed = json.loads(response.output_text.strip())
        subject = str(parsed.get("email_subject") or "").strip()
        body = normalize_action_plan_email_signoff(parsed.get("email_body"))
        if not subject or not body:
            raise ValueError("Email-only response was missing subject or body")
        if draft_reuses_expired_relative_timing(context, subject, body):
            return build_expired_timing_safe_email(context)
        if has_stale_logistics_signal(context) and draft_looks_like_active_logistics(body):
            return {
                "email_subject": fallback["email_subject"],
                "email_body": fallback["email_body"],
            }
        if draft_violates_outreach_style(body):
            return {
                "email_subject": fallback["email_subject"],
                "email_body": fallback["email_body"],
            }
        result = {"email_subject": subject, "email_body": body}
        cache_outreach_prep(cache_key, result)
        return result
    except Exception as error:
        print(f"AI action-plan email draft failed: {error}")
        return {
            "email_subject": fallback["email_subject"],
            "email_body": fallback["email_body"],
        }


def draft_reuses_expired_relative_timing(context, subject, body):
    if not any(
        bool(item.get("relative_timing_expired"))
        for item in (context.get("recent_sales_context") or [])
        if isinstance(item, dict)
    ):
        return False

    draft_text = f"{subject} {body}".lower()
    unsupported_timing_phrases = [
        "tomorrow",
        "later this week",
        "this week",
        "next week",
        "later this month",
        "this month",
        "next month",
        "i will be in",
        "i'll be in",
        "i’m planning to be in",
        "i'm planning to be in",
        "cancelled visit",
        "canceled visit",
        "after our visit",
        "after the visit",
        "during our visit",
        "when we met",
        "visit went well",
        "planned visit",
        "possible visit",
        "proposed visit",
        "did the visit",
        "if that visit",
        "whether the visit",
        "whether we visited",
    ]
    return any(phrase in draft_text for phrase in unsupported_timing_phrases)


def build_expired_timing_safe_email(context):
    primary_contact = context.get("primary_contact") or {}
    first_name = get_contact_first_name(primary_contact.get("name"))
    greeting = f"Hi {first_name}," if first_name else "Hi,"
    return {
        "email_subject": "Current repair needs",
        "email_body": (
            f"{greeting}\n\n"
            "It has been a while since your last repair order. "
            "Do you have any mats that need repair now?\n\n"
            "Thanks,"
        ),
    }


def redact_expired_scheduling_notes(items):
    redacted = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        cleaned = dict(item)
        if cleaned.get("relative_timing_expired"):
            cleaned["timing_status"] = "expired"
            cleaned["timing_instruction"] = (
                "Any relative date or proposed visit timing in this note has expired. Preserve useful commercial facts, "
                "but do not repeat the old timing, claim the visit happened, or ask whether it happened."
            )
        redacted.append(cleaned)
    return redacted


def select_primary_outreach_fact(items):
    """Rank usable CRM facts before drafting so order age is only a fallback."""
    ranked = []
    repair_terms = (
        "wavy mat", "edge repair", "mats for repair", "mats ready", "setting aside",
        "set aside", "collecting mats", "repair candidates", "damaged mats",
    )
    blocker_terms = ("price", "freight", "budget", "approval", "decision", "replacement")
    question_terms = (
        "if you have", "if you had", "do you have", "did you have", "whether you have",
        "whether you had", "any damaged or wavy", "anything that needs repair",
    )

    for index, item in enumerate(items or []):
        if not isinstance(item, dict) or item.get("later_order_recorded"):
            continue
        text = " ".join(str(item.get(key) or "") for key in ("subject", "preview")).strip()
        lowered = text.lower()
        if not text:
            continue
        score = max(0, 20 - index)
        fact_type = "conversation"
        speech_act = "statement"
        reason = "Most recent usable sales context"
        if any(term in lowered for term in repair_terms):
            score += 100
            fact_type = "unresolved_repair_signal"
            reason = "Specific unresolved repair need outranks general order age or a missed visit"
        elif any(term in lowered for term in blocker_terms):
            score += 75
            fact_type = "recorded_blocker"
            reason = "A recorded commercial blocker is more useful than general order age"
        elif item.get("likely_order_intent"):
            score += 65
            fact_type = "order_intent"
            reason = "Unresolved order intent is more useful than general order age"
        if str(item.get("direction") or "").lower() == "inbound":
            score += 25
        elif any(term in lowered for term in question_terms):
            speech_act = "salesperson_question"
            reason = "A specific repair question from the salesperson can be asked again without claiming the customer reported a need"
        if item.get("timing_status") == "expired" and fact_type == "conversation":
            score -= 20
            fact_type = "expired_scheduling"
            reason = "Use only if no stronger current commercial fact exists"
        ranked.append((score, {
            "type": fact_type,
            "reason": reason,
            "date": item.get("date"),
            "direction": item.get("direction"),
            "speech_act": speech_act,
            "subject": item.get("subject"),
            "preview": item.get("preview"),
        }))

    return max(ranked, key=lambda value: value[0])[1] if ranked else {}


def build_primary_fact_fallback_email(context, primary_fact):
    if str(primary_fact.get("type") or "") != "unresolved_repair_signal":
        return None
    primary_contact = context.get("primary_contact") or {}
    first_name = get_contact_first_name(primary_contact.get("name"))
    greeting = f"Hi {first_name}," if first_name else "Hi,"
    text = " ".join(str(primary_fact.get(key) or "") for key in ("subject", "preview")).lower()
    repair_description = "mats for repair"
    if "wavy" in text and "edge repair" in text:
        repair_description = "wavy mats for edge repair"
    elif "wavy" in text:
        repair_description = "wavy mats"
    direction = str(primary_fact.get("direction") or "").strip().lower()
    speech_act = str(primary_fact.get("speech_act") or "statement").strip().lower()
    if direction == "outbound" and speech_act == "salesperson_question":
        if "damaged" in text and "wavy" in text:
            direct_question = "Do you have any damaged or wavy mats that need repair?"
        else:
            direct_question = "Do you have any mats that need repair?"
        fact_sentence = ""
    elif direction == "inbound":
        fact_sentence = f"You previously mentioned collecting {repair_description}."
        direct_question = "Are you still setting those aside?"
    else:
        fact_sentence = f"I had noted possible {repair_description}."
        direct_question = "Are you still seeing those?"
    return {
        "email_subject": "Mats for repair",
        "email_body": (
            f"{greeting}\n\n"
            f"{fact_sentence + ' ' if fact_sentence else ''}{direct_question}\n\n"
            "Thanks,"
        ),
    }


def redact_intent_resolved_by_later_order(items):
    redacted = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        cleaned = dict(item)
        if cleaned.get("later_order_recorded") and cleaned.get("likely_order_intent"):
            later_order_date = str(cleaned.get("later_order_date") or "").strip()
            cleaned["subject"] = "Earlier order intent — resolved by a later order"
            cleaned["preview"] = (
                f"A later order was recorded on {later_order_date}. Treat the need discussed in this "
                "earlier activity as fulfilled; do not present it as pending or ask for the same mats."
            )
        redacted.append(cleaned)
    return redacted


def normalize_action_plan_email_signoff(body):
    lines = str(body or "").strip().splitlines()
    for index, line in enumerate(lines):
        if line.strip().lower() == "thanks,":
            return "\n".join(lines[:index] + ["Thanks,"]).strip()
    return "\n".join(lines).strip()


def build_data_question_fallback(question_payload):
    status = str(question_payload.get("status") or "").strip()

    if status == "no_customer_match":
        return (
            "I couldn't confidently match that question to a customer yet. "
            "Try using the customer name and location, for example `Cintas Gadsden AL`."
        )

    if status == "no_location_match":
        return "I couldn't confidently match that question to a state or location yet. Try using a state like `Alabama` or a location like `Gadsden AL`."

    if status == "missing_data":
        return "I couldn't load enough sales data to answer that right now."

    customer = str(question_payload.get("customer") or "This customer").strip()
    intent = str(question_payload.get("intent") or "summary").strip()
    facts = question_payload.get("facts") or {}
    last_order_date = str(facts.get("last_order_date") or "").strip()
    last_order_amount = str(facts.get("last_order_amount") or "").strip()
    last_order_number = str(facts.get("last_order_number") or "").strip()
    total_spend = str(facts.get("total_spend") or "").strip()
    latest_crm_date = str(facts.get("latest_crm_date") or "").strip()
    latest_crm_subject = str(facts.get("latest_crm_subject") or "").strip()
    contacts = facts.get("active_contacts") or []
    matched_customers = facts.get("matched_customers") or []
    stale_customers = facts.get("stale_customers") or []
    location_city = str(facts.get("location_city") or "").strip()
    location_state = str(facts.get("location_state") or "").strip()
    location_state_name = str(facts.get("location_state_name") or "").strip()

    if intent in {"location", "location_count"}:
        location_label = ", ".join(part for part in [location_city, location_state or location_state_name] if part)
        if not matched_customers:
            if location_label:
                return f"I couldn't find any customers in {location_label}."
            return "I couldn't find any customers for that location."
        if intent == "location_count":
            return f"We have {len(matched_customers)} customer{'s' if len(matched_customers) != 1 else ''} in {location_label}."
        if len(matched_customers) == 1:
            only_customer = matched_customers[0].get("customer") or "that location"
            return f"Yes. We have 1 customer in {location_label}: {only_customer}."
        preview = ", ".join(
            str(item.get("customer") or "").strip()
            for item in matched_customers[:5]
            if str(item.get("customer") or "").strip()
        )
        more_count = max(0, len(matched_customers) - 5)
        more_text = f" and {more_count} more" if more_count else ""
        return f"Yes. We have {len(matched_customers)} customers in {location_label}: {preview}{more_text}."

    if intent == "top_spend":
        location_label = ", ".join(part for part in [location_city, location_state or location_state_name] if part)
        if not matched_customers:
            return f"I couldn't find any customers in {location_label}."
        top_rows = []
        for item in matched_customers[:5]:
            customer_name = str(item.get("customer") or "").strip()
            spend_value = float(item.get("total_spend") or 0)
            top_rows.append(f"{customer_name} ({spend_value:,.2f})")
        return f"The biggest spend customers in {location_label} are " + ", ".join(top_rows) + "."

    if intent == "stale_location":
        location_label = ", ".join(part for part in [location_city, location_state or location_state_name] if part)
        if not stale_customers:
            return f"I couldn't find any customers in {location_label} with no recent orders."
        preview = []
        for item in stale_customers[:5]:
            customer_name = str(item.get("customer") or "").strip()
            days_since = item.get("days_since_last_order")
            if isinstance(days_since, int):
                preview.append(f"{customer_name} ({days_since} days)")
            else:
                preview.append(customer_name)
        more_count = max(0, len(stale_customers) - 5)
        more_text = f" and {more_count} more" if more_count else ""
        return f"The customers in {location_label} with no recent orders are " + ", ".join(preview) + more_text + "."

    if intent == "last_order":
        if not last_order_date:
            return f"I couldn't find a recorded order date for {customer}."
        detail = f" That order was {last_order_number}" if last_order_number else ""
        if last_order_amount:
            detail += f" for {last_order_amount}"
        return f"{customer} last ordered from us on {last_order_date}.{detail}".strip()

    if intent == "crm":
        if not latest_crm_date:
            return f"I couldn't find a CRM activity date for {customer}."
        subject_text = f" The latest subject was `{latest_crm_subject}`." if latest_crm_subject else ""
        return f"The latest CRM activity I can see for {customer} is from {latest_crm_date}.{subject_text}".strip()

    if intent == "contacts":
        if not contacts:
            return f"I couldn't find active FileMaker contacts for {customer}."
        top_contacts = ", ".join(
            f"{item.get('name')} ({item.get('position') or 'contact'})"
            for item in contacts[:3]
            if item.get("name")
        )
        return f"The active contacts I can see for {customer} are {top_contacts}."

    if intent == "spend":
        if not total_spend:
            return f"I couldn't find a total spend figure for {customer}."
        return f"{customer} has spent {total_spend} with us across {facts.get('order_count') or 0} recorded orders."

    summary_parts = []
    if last_order_date:
        summary_parts.append(f"last order: {last_order_date}")
    if latest_crm_date:
        summary_parts.append(f"latest CRM activity: {latest_crm_date}")
    if contacts:
        summary_parts.append(f"{len(contacts)} active contact{'s' if len(contacts) != 1 else ''} on FileMaker")
    if total_spend:
        summary_parts.append(f"total spend: {total_spend}")

    if not summary_parts:
        return f"I found {customer}, but I don't have enough structured detail to answer that yet."

    return f"{customer}: " + ", ".join(summary_parts) + "."


def generate_data_question_answer(question_payload):
    fallback = build_data_question_fallback(question_payload)
    if str(question_payload.get("status") or "").strip() != "ok":
        return fallback

    intent = str(question_payload.get("intent") or "").strip()
    if intent in {"last_order", "crm", "contacts", "spend", "location", "location_count", "top_spend", "stale_location"}:
        return fallback

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return fallback

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a concise sales data assistant. "
                        "Answer the user's question only from the provided JSON facts. "
                        "Do not invent data. If the facts are incomplete, say so plainly. "
                        "Keep the answer short and practical."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "User question:\n"
                        + str(question_payload.get("question") or "").strip()
                        + "\n\nStructured facts:\n"
                        + json.dumps(question_payload, indent=2)
                    ),
                },
            ],
            max_output_tokens=180,
        )
        answer = response.output_text.strip()
        return answer or fallback
    except Exception as error:
        print(f"AI data question failed: {error}")
        return fallback


def generate_production_analysis_report(facts):
    fallback = {
        "executive_summary": ["The report contains verified production and operator results for the selected completed-day period."],
        "comparison": ["Review the current and previous-period figures together before drawing conclusions about changes."],
        "press_flow": ["Department and press results should be reviewed for sustained changes rather than isolated daily movements."],
        "quality": ["Re-cook activity is reported separately from new press throughput and should be monitored as a trend."],
        "strengths": ["Completed-day production data is available for consistent historical comparison."],
        "focus_areas": ["Investigate material variances and confirm anomalous time bookings before acting on performance figures."],
        "workforce_observations": ["Operator results should be interpreted alongside attendance, role, training and booking accuracy."],
        "operator_comments": {},
        "final_message": "Use the verified figures in this report to guide the next production review and agree practical follow-up actions.",
    }
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {**fallback, "_analysis_status": "missing_api_key"}
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a careful production operations analyst. Interpret only the supplied JSON facts. "
                        "Business context: NuMat repairs rubber-backed carpeted mats; this is a mat-repair operation, not new-product manufacturing. "
                        "The word 'mats' is the required business term and must remain unchanged. Never replace it with materials, units, products, items or goods. "
                        "total_mats_repaired_through_presses is the recorded number of mats repaired/processed through FF1, FF2 and FF3 combined. "
                        "Describe changes in that figure as mats processed or mats repaired through the presses, never as materials used or consumed. "
                        "Backlog contains new customer orders awaiting their first repair only. Re-cook work is never included in backlog and cannot cause backlog to rise. "
                        "Re-cook lineal feet is a separate quality measure for repairs that must pass through the press again. Analyse backlog and re-cook independently; "
                        "never combine them into a shared cause, consequence or corrective action unless independent supplied facts explicitly support one. "
                        "NuMat's preferred backlog is approximately two weeks. Backlog below that level means production has less new-order repair work available and should prompt sales to secure more orders. "
                        "An increase that remains below approximately two weeks is movement toward a healthier workload, not an adverse growth trend or a reason to improve turnaround. "
                        "Low backlog normally enables faster-than-usual turnaround, although it can constrain production output because the plant can repair only the customer mats it has received. "
                        "Only describe backlog as a turnaround pressure when it is materially above the two-week preference and the supplied facts support that conclusion. "
                        "Never invent causes, staffing events, bottlenecks, targets or business context. Distinguish facts from hypotheses. "
                        "Use concise British English suitable for a management production report. "
                        "Return strict JSON with keys executive_summary, comparison, press_flow, quality, strengths, focus_areas, "
                        "workforce_observations, operator_comments and final_message. All section values except operator_comments and "
                        "final_message must be arrays of short strings. operator_comments must map operator names to one cautious sentence. "
                        "Mention uncertainty when data is missing or anomalous. Do not use markdown."
                    ),
                },
                {"role": "user", "content": json.dumps(facts, indent=2)},
            ],
            max_output_tokens=2800,
        )
        parsed = json.loads(response.output_text.strip())
        if not isinstance(parsed, dict):
            return {**fallback, "_analysis_status": "invalid_response"}
        return {**fallback, **parsed, "_analysis_status": "ok"}
    except Exception as error:
        print(f"AI production analysis failed: {error}")
        return {**fallback, "_analysis_status": "error"}
