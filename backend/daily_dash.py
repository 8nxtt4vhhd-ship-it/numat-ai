from collections import defaultdict
from datetime import datetime
from io import BytesIO

from app_settings import calculate_elapsed_invoice_target, get_daily_invoice_target


BRAND_BLUE = "#245CFF"
NAVY = "#14233A"
SLATE = "#5B6B7F"
PALE_BLUE = "#EEF4FF"
PALE_GREEN = "#EAF8F3"
PALE_ORANGE = "#FFF5E6"
PALE_RED = "#FFF0F0"
BORDER = "#D7E2EF"


def _sum(rows, key):
    values = [item.get(key) for item in rows if item.get(key) is not None]
    return sum(float(value) for value in values) if values else None


def _latest_value(rows, key):
    for item in reversed(rows):
        if item.get(key) is not None:
            return item.get(key)
    return None


def _active_orders(orders, report_date):
    complete = {"complete", "completed", "closed", "cancelled", "canceled"}
    return sorted(
        [
            item for item in orders
            if str(item.get("order_date") or "") <= report_date
            and str((item.get("extra") or {}).get("Orders::Status") or "").strip().casefold() not in complete
        ],
        key=lambda item: (str(item.get("order_date") or ""), str(item.get("customer") or "")),
    )


def _new_accounts(orders, report_date, limit=5):
    first_by_customer = {}
    for item in orders:
        order_date = str(item.get("order_date") or "")
        customer = str(item.get("customer") or "").strip()
        if not customer or not order_date or order_date > report_date:
            continue
        current = first_by_customer.get(customer)
        if current is None or order_date < str(current.get("order_date") or ""):
            first_by_customer[customer] = item
    return sorted(first_by_customer.values(), key=lambda item: str(item.get("order_date") or ""), reverse=True)[:limit]


def build_daily_dash_payload(production_result, orders_result=None, finance_result=None, report_date=None, daily_invoice_target=None):
    rows = sorted(production_result.get("production_rows", []), key=lambda item: str(item.get("date") or ""))
    if report_date:
        rows = [item for item in rows if str(item.get("date") or "") <= str(report_date)]
    if not rows:
        raise ValueError("No completed production day is available for the Daily Dash.")
    latest = rows[-1]
    report_date = str(latest.get("date") or report_date or "")
    month_key = report_date[:7]
    month_rows = [item for item in rows if str(item.get("date") or "").startswith(month_key)]
    plant_operators = production_result.get("plant_operator_rows", production_result.get("operator_rows", []))
    month_operators = [item for item in plant_operators if month_key <= str(item.get("date") or "")[:7] <= month_key and str(item.get("date") or "") <= report_date]
    day_operators = [item for item in month_operators if str(item.get("date") or "") == report_date]

    def productivity(operator_rows):
        active = [item for item in operator_rows if (item.get("clocked_hours") or 0) > 0]
        booked = sum(item.get("booked_hours") or 0 for item in active)
        clocked = sum(item.get("clocked_hours") or 0 for item in active)
        return (booked / clocked * 100) if clocked else None

    invoiced = _latest_value(month_rows, "invoiced_revenue_mtd")
    if invoiced is None:
        invoiced = 0.0
    daily_invoice_target = get_daily_invoice_target() if daily_invoice_target is None else float(daily_invoice_target)
    target = calculate_elapsed_invoice_target(report_date, daily_invoice_target)
    labour_cost = _sum(month_rows, "labour_cost")
    labour_pct_mtd = (labour_cost / invoiced * 100) if labour_cost is not None and invoiced and invoiced > 0 else 0.0

    press_lf = latest.get("press_throughput_reported") or latest.get("press_throughput")
    press_revenue = latest.get("production_revenue_today")
    average_revenue = (press_revenue / press_lf) if press_revenue is not None and press_lf else None
    production = []
    for label, key in (("Sort", "sort_throughput"), ("Grind", "grind_throughput"), ("Press", "press_throughput_reported"), ("Trim", "trim_throughput"), ("Re-cook", "recook_lf")):
        production.append({"department": label, "today": latest.get(key), "mtd": _sum(month_rows, key), "unit": "LF"})
    production.append({"department": "Extrusion", "today": None, "mtd": None, "unit": "LF"})

    presses = []
    for number in (1, 2, 3):
        prefix = f"ff{number}_"
        presses.append({
            "name": f"FF{number}",
            "cycles": latest.get(prefix + "cycles"),
            "mats": latest.get(prefix + "mats"),
            "lf": latest.get(prefix + "lf"),
            "avg_lf_cycle": latest.get(prefix + "avg_lf_cycle"),
            "avg_revenue_cycle": latest.get(prefix + "avg_revenue_cycle"),
            "lost_hours": latest.get(prefix + "lost_hours"),
            "utilisation": latest.get(prefix + "utilisation"),
        })

    orders = (orders_result or {}).get("orders", []) if (orders_result or {}).get("status") == "ok" else []
    active_orders = _active_orders(orders, report_date)
    finance_result = finance_result or {}
    finance_available = finance_result.get("status") == "ok"
    return {
        "report_date": report_date,
        "generated_at": datetime.now().astimezone().isoformat(timespec="minutes"),
        "status": production_result.get("status"),
        "invoiced_revenue_mtd": invoiced,
        "monthly_invoice_target": target,
        "daily_invoice_target": daily_invoice_target,
        "invoice_target_pct": (invoiced / target * 100) if invoiced is not None and target else None,
        "production_revenue_today": press_revenue,
        "average_revenue_per_square_foot": average_revenue,
        "production": production,
        "presses": presses,
        "press_time_hours": latest.get("press_hours"),
        "mats_today": sum(item.get("mats") or 0 for item in presses),
        "mats_mtd": sum(sum(item.get(f"ff{number}_mats") or 0 for number in (1, 2, 3)) for item in month_rows),
        "cycles_today": sum(item.get("cycles") or 0 for item in presses),
        "cycles_mtd": sum(sum(item.get(f"ff{number}_cycles") or 0 for number in (1, 2, 3)) for item in month_rows),
        "labour_hours_today": sum(item.get("clocked_hours") or 0 for item in day_operators),
        "labour_hours_mtd": sum(item.get("clocked_hours") or 0 for item in month_operators),
        "labour_pct_today": latest.get("labour_percentage"),
        "labour_pct_mtd": labour_pct_mtd,
        "productivity_today": productivity(day_operators),
        "productivity_mtd": productivity(month_operators),
        "backlog": {name: latest.get(f"backlog_{name.casefold()}") for name in ("Sort", "Grind", "Press", "Trim")},
        "backlog_weeks": latest.get("backlog_weeks"),
        "incoming_skids": latest.get("incoming_skids"),
        "new_accounts": _new_accounts(orders, report_date),
        "oldest_orders": active_orders[:5],
        "aged_debt": {
            "current": finance_result.get("current"),
            "0_30": finance_result.get("zero_thirty"),
            "31_60": finance_result.get("thirtyone_sixty"),
            "61_90": finance_result.get("sixtyone_ninety"),
            "90_plus": finance_result.get("ninety_plus"),
            "total_outstanding": finance_result.get("total_outstanding"),
        },
        "average_debtor_days": finance_result.get("average_debtor_days") if finance_available else latest.get("aged_debtor_days"),
        "finance_refreshed_at": finance_result.get("last_refreshed", ""),
        "supervisor_notes": str(latest.get("supervisor_notes") or "").strip(),
        "unavailable": [item for item in (
            "" if finance_available else "Aged debt bucket values",
            "Extrusion output",
            "Sort, grind and trim revenue",
            "Average square feet per mat",
        ) if item],
    }


def build_daily_dash_pdf(payload):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import BaseDocTemplate, Frame, PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle

    output = BytesIO()
    page_size = landscape(A4)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="DashTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=24, leading=27, textColor=colors.HexColor(NAVY), alignment=TA_LEFT, spaceAfter=2*mm))
    styles.add(ParagraphStyle(name="Section", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=colors.HexColor(BRAND_BLUE), spaceBefore=2*mm, spaceAfter=2*mm))
    styles.add(ParagraphStyle(name="Small", parent=styles["BodyText"], fontSize=7.5, leading=10, textColor=colors.HexColor(SLATE)))
    styles.add(ParagraphStyle(name="BodyCompact", parent=styles["BodyText"], fontSize=8.5, leading=11, textColor=colors.HexColor(NAVY)))
    styles.add(ParagraphStyle(name="CardLabel", parent=styles["BodyText"], fontSize=7.5, leading=9, textColor=colors.HexColor(SLATE), alignment=TA_CENTER))
    styles.add(ParagraphStyle(name="CardValue", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=17, leading=19, textColor=colors.HexColor(NAVY), alignment=TA_CENTER))
    styles.add(ParagraphStyle(name="RightSmall", parent=styles["Small"], alignment=TA_RIGHT))

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor(BORDER)); canvas.line(12*mm, 10*mm, page_size[0]-12*mm, 10*mm)
        canvas.setFont("Helvetica", 7); canvas.setFillColor(colors.HexColor(SLATE))
        canvas.drawString(12*mm, 6*mm, "NuMat Systems - Daily Dash - completed production data")
        canvas.drawRightString(page_size[0]-12*mm, 6*mm, f"Page {doc.page}")
        canvas.restoreState()

    doc = BaseDocTemplate(output, pagesize=page_size, leftMargin=12*mm, rightMargin=12*mm, topMargin=10*mm, bottomMargin=13*mm, title=f"NuMat Daily Dash {payload['report_date']}")
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")
    doc.addPageTemplates([PageTemplate(id="dash", frames=[frame], onPage=footer)])

    def fmt(value, kind="number", unavailable="Not available"):
        if value is None:
            return unavailable
        if kind == "money": return f"${float(value):,.0f}"
        if kind == "money2": return f"${float(value):,.2f}"
        if kind == "percent": return f"{float(value):.1f}%"
        if kind == "decimal1": return f"{float(value):,.1f}"
        if kind == "decimal2": return f"{float(value):,.2f}"
        if kind == "hours":
            hours = int(float(value)); minutes = round((float(value)-hours)*60)
            return f"{hours}:{minutes:02d} hrs"
        return f"{float(value):,.0f}"

    def card(label, value, note="", fill=PALE_BLUE):
        content = [Paragraph(label, styles["CardLabel"]), Spacer(1, 1.5*mm), Paragraph(value, styles["CardValue"])]
        if note: content.extend([Spacer(1, 1*mm), Paragraph(note, styles["CardLabel"])])
        table = Table([[content]], colWidths=[43*mm], rowHeights=[28*mm])
        table.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor(fill)),("BOX",(0,0),(-1,-1),0.7,colors.HexColor(BORDER)),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("LEFTPADDING",(0,0),(-1,-1),3*mm),("RIGHTPADDING",(0,0),(-1,-1),3*mm),("TOPPADDING",(0,0),(-1,-1),2*mm),("BOTTOMPADDING",(0,0),(-1,-1),2*mm)]))
        return table

    def data_table(headers, rows, widths=None, font_size=8):
        content = [[Paragraph(str(value), styles["BodyCompact"]) for value in headers]]
        content += [[Paragraph(str(value), styles["BodyCompact"]) for value in row] for row in rows]
        table = Table(content, colWidths=widths, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#E8F0FA")),("TEXTCOLOR",(0,0),(-1,0),colors.HexColor("#36506D")),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("GRID",(0,0),(-1,-1),0.45,colors.HexColor(BORDER)),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("FONTSIZE",(0,0),(-1,-1),font_size),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#F8FAFD")]),("LEFTPADDING",(0,0),(-1,-1),2.5*mm),("RIGHTPADDING",(0,0),(-1,-1),2.5*mm),("TOPPADDING",(0,0),(-1,-1),1.7*mm),("BOTTOMPADDING",(0,0),(-1,-1),1.7*mm),
        ]))
        return table

    date_label = datetime.strptime(payload["report_date"], "%Y-%m-%d").strftime("%A %-d %B %Y")
    story = [Table([[Paragraph("DAILY DASH", styles["DashTitle"]), Paragraph(date_label, styles["RightSmall"])]], colWidths=[doc.width*0.7,doc.width*0.3]), Spacer(1,2*mm)]
    cards = [
        card("Invoiced revenue MTD", fmt(payload.get("invoiced_revenue_mtd"),"money"), "Target " + fmt(payload.get("monthly_invoice_target"),"money")),
        card("Plant productivity MTD", fmt(payload.get("productivity_mtd"),"percent"), "Booked hours / clocked hours", PALE_GREEN),
        card("Labour % MTD", fmt(payload.get("labour_pct_mtd"),"percent"), "Target 24%", PALE_ORANGE),
        card("Current backlog", fmt(payload.get("backlog_weeks"), "decimal2"), "weeks", PALE_ORANGE),
        card("Production value today", fmt(payload.get("production_revenue_today"),"money"), "captured at press", PALE_BLUE),
        card("Incoming", fmt(payload.get("incoming_skids")), "skids and cages", PALE_RED),
    ]
    story += [Table([cards], colWidths=[doc.width/6]*6), Spacer(1,3*mm), Paragraph("Production today and month to date",styles["Section"])]
    prod_rows=[[x["department"],fmt(x["today"]),fmt(x["mtd"]),x["unit"]] for x in payload["production"]]
    production_table=data_table(["Department","Today","MTD","Unit"],prod_rows,[31*mm,26*mm,28*mm,16*mm])
    labour_table=data_table(["Labour","Today","MTD","Target"],[
        ["Clocked hours",fmt(payload.get("labour_hours_today"),"hours"),fmt(payload.get("labour_hours_mtd"),"hours"),"-"],
        ["Labour %",fmt(payload.get("labour_pct_today"),"percent"),fmt(payload.get("labour_pct_mtd"),"percent"),"24.0%"],
        ["Productivity",fmt(payload.get("productivity_today"),"percent"),fmt(payload.get("productivity_mtd"),"percent"),"90.0%"],
    ],[29*mm,24*mm,27*mm,18*mm])
    backlog_table=data_table(["Backlog","Mats"],[[name,fmt(value)] for name,value in payload["backlog"].items()],[31*mm,24*mm])
    story += [Table([[production_table,labour_table,backlog_table]],colWidths=[105*mm,102*mm,60*mm],style=[("VALIGN",(0,0),(-1,-1),"TOP"),("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),2*mm)]),Spacer(1,2*mm),Paragraph("Press performance",styles["Section"])]
    press_rows=[]
    for item in payload["presses"]:
        press_rows.append([item["name"],fmt(item["mats"]),fmt(item["lf"]),fmt(item["cycles"]),fmt(item["avg_lf_cycle"]),fmt(item["avg_revenue_cycle"],"money"),fmt(item["lost_hours"],"hours"),fmt(item["utilisation"],"percent")])
    story += [data_table(["Press","Mats","Linear ft","Cycles","Avg LF/cycle","Avg $/cycle","Lost time","Utilisation"],press_rows,[22*mm,22*mm,27*mm,22*mm,32*mm,32*mm,28*mm,28*mm]),Spacer(1,2*mm)]
    press_summary_headers = ["Total press time","Mats today / MTD","Cycles today / MTD","Average revenue / sq ft","Average sq ft / mat"]
    press_summary_values = [[fmt(payload.get("press_time_hours"),"hours"),f"{fmt(payload.get('mats_today'))} / {fmt(payload.get('mats_mtd'))}",f"{fmt(payload.get('cycles_today'))} / {fmt(payload.get('cycles_mtd'))}",fmt(payload.get("average_revenue_per_square_foot"),"money2"),"Not available"]]
    story += [data_table(press_summary_headers,press_summary_values,[doc.width/5]*5),PageBreak()]

    finance_refresh = str(payload.get("finance_refreshed_at") or "Not available")
    story += [Table([[Paragraph("COMMERCIAL AND OPERATIONAL CONTROL",styles["DashTitle"]),Paragraph(date_label,styles["RightSmall"])]],colWidths=[doc.width*0.7,doc.width*0.3]),Spacer(1,2*mm),Paragraph("Aged debt",styles["Section"]),Paragraph("FileMaker finance data refreshed " + finance_refresh, styles["Small"]),Spacer(1,1*mm)]
    debt=payload["aged_debt"]
    story += [data_table(["Current","0-30","31-60","61-90","90+ days","Total outstanding","Average debtor days"],[[fmt(debt["current"],"money2"),fmt(debt["0_30"],"money2"),fmt(debt["31_60"],"money2"),fmt(debt["61_90"],"money2"),fmt(debt["90_plus"],"money2"),fmt(debt["total_outstanding"],"money2"),fmt(payload.get("average_debtor_days"),"decimal1")]],[36*mm]*7),Spacer(1,3*mm)]

    new_rows=[]
    for item in payload["new_accounts"]:
        extra=item.get("extra") or {}; new_rows.append([item.get("order_date") or "-",item.get("customer") or "-",extra.get("Orders::Order No") or "-",extra.get("ai_PriceList") or "-"])
    if not new_rows: new_rows=[["-","No new-account orders available","-","-"]]
    old_rows=[]
    for item in payload["oldest_orders"]:
        extra=item.get("extra") or {}; old_rows.append([extra.get("Orders::Order No") or "-",item.get("customer") or "-",extra.get("Orders::Status") or "-",item.get("order_date") or "-"])
    if not old_rows: old_rows=[["-","No open orders available","-","-"]]
    new_table=[Paragraph("Newest first-time customers",styles["Section"]),data_table(["Date","Customer","Order","Price list"],new_rows,[23*mm,70*mm,20*mm,18*mm])]
    old_table=[Paragraph("Oldest orders in house",styles["Section"]),data_table(["Order","Customer","Stage","Received"],old_rows,[20*mm,65*mm,25*mm,23*mm])]
    story += [Table([[new_table,old_table]],colWidths=[doc.width/2,doc.width/2],style=[("VALIGN",(0,0),(-1,-1),"TOP"),("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),4*mm)]),Spacer(1,4*mm),Paragraph("Supervisor comments",styles["Section"])]
    notes=payload.get("supervisor_notes") or "No supervisor comments were recorded in the production API record."
    notes_table=Table([[Paragraph(notes,styles["BodyCompact"])]],colWidths=[doc.width],rowHeights=[28*mm])
    notes_table.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#F8FAFD")),("BOX",(0,0),(-1,-1),0.6,colors.HexColor(BORDER)),("VALIGN",(0,0),(-1,-1),"TOP"),("LEFTPADDING",(0,0),(-1,-1),4*mm),("RIGHTPADDING",(0,0),(-1,-1),4*mm),("TOPPADDING",(0,0),(-1,-1),3*mm)]))
    story += [notes_table]
    doc.build(story)
    return output.getvalue()
