from collections import defaultdict
from datetime import datetime
from io import BytesIO
import math


def _escape(value):
    from xml.sax.saxutils import escape

    return escape(str(value or ""))


def _contact_state(contact):
    state = str(contact.get("activity_state") or "none").strip().lower()
    return state if state in {"recent", "older"} else "none"


def _activity_contacts(contacts):
    return [contact for contact in contacts or [] if _contact_state(contact) != "none"]


def _activity_text(contact):
    return (
        str(contact.get("subject") or "").strip(),
        str(contact.get("full_body") or "").strip(),
    )


def build_strategic_contacts_pdf(payload):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        BaseDocTemplate,
        CondPageBreak,
        Flowable,
        Frame,
        PageBreak,
        PageTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
    )

    contacts = list(payload.get("contacts") or [])
    activity_contacts = _activity_contacts(contacts)
    group_label = str(payload.get("group_label") or "Strategic Contacts").strip()
    generated_at = str(payload.get("generated_at") or datetime.now().strftime("%d %B %Y %H:%M")).strip()
    page_size = landscape(A4)
    page_width, page_height = page_size
    margin = 13 * mm
    buffer = BytesIO()
    styles = getSampleStyleSheet()
    navy = colors.HexColor("#17365D")
    blue = colors.HexColor("#245CFF")
    muted = colors.HexColor("#5B6B7C")
    border = colors.HexColor("#D9E5F2")
    recent_fill = colors.HexColor("#F0F9F3")
    recent_border = colors.HexColor("#CFE6D5")
    older_fill = colors.HexColor("#FFF6E9")
    older_border = colors.HexColor("#EDD9BD")
    neutral_fill = colors.white

    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=20,
        leading=23,
        textColor=navy,
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=muted,
    )
    section_style = ParagraphStyle(
        "Section",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
        textColor=navy,
        spaceBefore=8,
        spaceAfter=5,
    )
    body_style = ParagraphStyle(
        "Body",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=12,
        textColor=colors.HexColor("#263648"),
    )
    small_style = ParagraphStyle(
        "Small",
        parent=body_style,
        fontSize=7.5,
        leading=10,
        textColor=muted,
    )

    class ChartFlowable(Flowable):
        def __init__(self, rows, width, height):
            super().__init__()
            self.rows = rows
            self.width = width
            self.height = height

        def wrap(self, avail_width, avail_height):
            return min(self.width, avail_width), min(self.height, avail_height)

        @staticmethod
        def _wrapped_lines(canvas, text, max_width, font_name, font_size, max_lines=2):
            words = str(text or "").split()
            if not words:
                return []
            lines = []
            remaining = words
            while remaining and len(lines) < max_lines:
                line_words = []
                while remaining:
                    candidate = " ".join(line_words + [remaining[0]])
                    if line_words and canvas.stringWidth(candidate, font_name, font_size) > max_width:
                        break
                    line_words.append(remaining.pop(0))
                line = " ".join(line_words)
                if remaining and len(lines) == max_lines - 1:
                    line = f"{line} {' '.join(remaining)}".strip()
                    while line and canvas.stringWidth(f"{line}...", font_name, font_size) > max_width:
                        line = line[:-1].rstrip()
                    line = f"{line}..." if line else "..."
                    remaining = []
                lines.append(line)
            return lines

        def draw(self):
            canvas = self.canv
            width = self.width
            height = self.height
            canvas.setFillColor(navy)
            canvas.setFont("Helvetica-Bold", 20)
            canvas.drawString(0, height - 24, f"{group_label} - Strategic Contact Report")
            canvas.setFillColor(muted)
            canvas.setFont("Helvetica", 8)
            canvas.drawString(0, height - 39, f"Generated {generated_at} | Condensed organisation chart")

            counts = {state: sum(1 for item in self.rows if _contact_state(item) == state) for state in ("recent", "older", "none")}
            summary_labels = [
                ("Recent contact (60 days)", counts["recent"], recent_fill, recent_border),
                ("Older contact", counts["older"], older_fill, older_border),
                ("No contact found", counts["none"], colors.white, border),
            ]
            summary_x = width - 330
            for label, count, fill, stroke in summary_labels:
                canvas.setFillColor(fill)
                canvas.setStrokeColor(stroke)
                canvas.roundRect(summary_x, height - 43, 104, 25, 7, fill=1, stroke=1)
                canvas.setFillColor(navy)
                canvas.setFont("Helvetica-Bold", 7)
                canvas.drawString(summary_x + 7, height - 29, label)
                canvas.setFont("Helvetica-Bold", 9)
                canvas.drawRightString(summary_x + 96, height - 36, str(count))
                summary_x += 112

            grouped = defaultdict(list)
            for item in self.rows:
                grouped[str(item.get("scope_type") or "Other")].append(item)
            scope_order = ["Corporate", "National Account", "Regional", "Division", "Other"]
            scopes = sorted(grouped, key=lambda value: (scope_order.index(value) if value in scope_order else 99, value.lower()))
            columns = 5
            total_card_rows = sum(max(1, math.ceil(len(grouped[scope]) / columns)) for scope in scopes)
            available_height = height - 75
            scope_gap = 14
            card_height = min(70, max(38, (available_height - scope_gap * len(scopes)) / max(1, total_card_rows)))
            label_width = 78
            gutter = 7
            card_width = (width - label_width - gutter * (columns - 1)) / columns
            y = height - 67

            for scope in scopes:
                scope_contacts = sorted(
                    grouped[scope],
                    key=lambda item: (str(item.get("region") or ""), str(item.get("name") or "")),
                )
                canvas.setFillColor(blue)
                canvas.setFont("Helvetica-Bold", 9)
                canvas.drawString(0, y - 12, scope)
                canvas.setFillColor(muted)
                canvas.setFont("Helvetica", 7)
                canvas.drawString(0, y - 24, f"{len(scope_contacts)} contact{'s' if len(scope_contacts) != 1 else ''}")
                for index, item in enumerate(scope_contacts):
                    row = index // columns
                    column = index % columns
                    x = label_width + column * (card_width + gutter)
                    card_y = y - ((row + 1) * card_height)
                    state = _contact_state(item)
                    fill = recent_fill if state == "recent" else older_fill if state == "older" else neutral_fill
                    stroke = recent_border if state == "recent" else older_border if state == "older" else border
                    canvas.setFillColor(fill)
                    canvas.setStrokeColor(stroke)
                    canvas.roundRect(x, card_y + 3, card_width, card_height - 5, 6, fill=1, stroke=1)
                    canvas.setFillColor(navy)
                    canvas.setFont("Helvetica-Bold", 8)
                    canvas.drawString(x + 7, card_y + card_height - 16, str(item.get("name") or "Unknown")[:32])
                    canvas.setFillColor(muted)
                    canvas.setFont("Helvetica", 6.5)
                    role_lines = self._wrapped_lines(
                        canvas,
                        item.get("position") or "Role not set",
                        card_width - 14,
                        "Helvetica",
                        6.5,
                        max_lines=2,
                    )
                    role_y = card_y + card_height - 28
                    for line_index, line in enumerate(role_lines):
                        canvas.drawString(x + 7, role_y - (line_index * 8), line)
                    region_y = role_y - (len(role_lines) * 8) - 3
                    canvas.drawString(x + 7, region_y, str(item.get("region") or "Unassigned")[:30])
                    last_contact = str(item.get("last_contact_date") or "No contact recorded")
                    canvas.setFillColor(navy)
                    canvas.setFont("Helvetica-Bold", 6.5)
                    canvas.drawString(x + 7, card_y + 10, last_contact[:34])
                y -= max(1, math.ceil(len(scope_contacts) / columns)) * card_height + scope_gap

    def draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(border)
        canvas.line(margin, 10 * mm, page_width - margin, 10 * mm)
        canvas.setFillColor(muted)
        canvas.setFont("Helvetica", 7)
        canvas.drawString(margin, 6.5 * mm, f"NuMat Sales Focus | {group_label}")
        canvas.drawRightString(page_width - margin, 6.5 * mm, f"Page {doc.page}")
        canvas.restoreState()

    frame = Frame(
        margin,
        18 * mm,
        page_width - (2 * margin),
        page_height - (30 * mm),
        leftPadding=0,
        rightPadding=0,
        topPadding=0,
        bottomPadding=0,
    )
    doc = BaseDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=margin,
        rightMargin=margin,
        topMargin=margin,
        bottomMargin=13 * mm,
        title=f"{group_label} Strategic Contact Report",
        author="NuMat Sales Focus",
    )
    doc.addPageTemplates(PageTemplate(id="report", frames=[frame], onPage=draw_footer))
    story = [
        ChartFlowable(contacts, page_width - 2 * margin, page_height - 30 * mm),
        PageBreak(),
        Paragraph("Most recent contact activity", title_style),
        Paragraph(
            "The latest matched CRM communication for contacts with recorded activity. Full message text is included where available.",
            subtitle_style,
        ),
        Spacer(1, 7),
    ]

    if not activity_contacts:
        story.append(Paragraph("No matched CRM activity was found for the contacts in this chart.", body_style))

    for contact in activity_contacts:
        state = _contact_state(contact)
        state_label = "Recent - within 60 days" if state == "recent" else "Older - over 60 days"
        fill = recent_fill if state == "recent" else older_fill
        heading = Table(
            [[
                Paragraph(f"<b>{_escape(contact.get('name') or 'Unknown')}</b><br/><font color='#5B6B7C'>{_escape(contact.get('position') or 'Role not set')}</font>", body_style),
                Paragraph(f"<b>{_escape(state_label)}</b><br/>{_escape(contact.get('last_contact_date') or 'No contact recorded')}", small_style),
            ]],
            colWidths=[(page_width - 2 * margin) * 0.68, (page_width - 2 * margin) * 0.32],
        )
        heading.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), fill),
            ("BOX", (0, 0), (-1, -1), 0.7, border),
            ("INNERGRID", (0, 0), (-1, -1), 0.4, border),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        metadata = Table(
            [[
                Paragraph(f"<b>Region</b><br/>{_escape(contact.get('region') or 'Not set')}", small_style),
                Paragraph(f"<b>Email</b><br/>{_escape(contact.get('email') or 'Not set')}", small_style),
                Paragraph(f"<b>Phone</b><br/>{_escape(contact.get('phone') or 'Not set')}", small_style),
                Paragraph(f"<b>Direction</b><br/>{_escape(contact.get('direction') or 'Not available')}", small_style),
            ]],
            colWidths=[(page_width - 2 * margin) / 4] * 4,
        )
        metadata.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, border),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, border),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        activity_detail = [
            CondPageBreak(105),
            heading,
            metadata,
            Spacer(1, 4),
        ]
        subject, full_body = _activity_text(contact)
        if subject:
            activity_detail.extend([
                Paragraph(f"<b>Subject:</b> {_escape(subject)}", body_style),
                Spacer(1, 3),
            ])
        if full_body:
            activity_detail.append(Paragraph(_escape(full_body).replace("\n", "<br/>"), body_style))
        activity_detail.append(Spacer(1, 11))
        story.extend(activity_detail)

    doc.build(story)
    return buffer.getvalue()
