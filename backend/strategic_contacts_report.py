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
    manager_fill = colors.HexColor("#EAF3FF")
    manager_border = colors.HexColor("#8DB9F5")
    branch_fill = colors.HexColor("#F3F5F7")
    branch_border = colors.HexColor("#CFD6DE")
    section_fill = colors.HexColor("#FFFFFF")

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

    business_sections = list(payload.get("business_sections") or [])
    if not business_sections:
        business_sections = [
            {"key": "supply-chain", "label": "Supply Chain"},
            {"key": "production", "label": "Production"},
            {"key": "operations", "label": "Operations"},
        ]
    section_lead_enabled = bool(payload.get("section_lead_enabled"))
    section_lead_label = str(payload.get("section_lead_label") or "Division lead")

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

        def _card(self, canvas, item, x, y, width, height, fill, stroke, compact=False):
            canvas.setFillColor(fill)
            canvas.setStrokeColor(stroke)
            canvas.roundRect(x, y, width, height, 5, fill=1, stroke=1)
            inset = 7
            canvas.setFillColor(navy)
            canvas.setFont("Helvetica-Bold", 7.5 if compact else 8)
            name_lines = self._wrapped_lines(canvas, item.get("name") or "Unknown", width - 2 * inset, "Helvetica-Bold", 7.5 if compact else 8, 2)
            name_y = y + height - 13
            for line_index, line in enumerate(name_lines):
                canvas.drawString(x + inset, name_y - line_index * 8, line)
            canvas.setFillColor(muted)
            canvas.setFont("Helvetica", 6.2)
            role_y = name_y - len(name_lines) * 8 - 1
            role_lines = self._wrapped_lines(canvas, item.get("position") or "Role not set", width - 2 * inset, "Helvetica", 6.2, 1 if compact else 2)
            for line_index, line in enumerate(role_lines):
                canvas.drawString(x + inset, role_y - line_index * 7, line)
            state = _contact_state(item)
            chip_fill = recent_fill if state == "recent" else older_fill if state == "older" else colors.HexColor("#F7F9FC")
            chip_stroke = recent_border if state == "recent" else older_border if state == "older" else border
            chip_h = 15
            canvas.setFillColor(chip_fill)
            canvas.setStrokeColor(chip_stroke)
            canvas.roundRect(x + inset, y + 6, width - 2 * inset, chip_h, 4, fill=1, stroke=1)
            canvas.setFillColor(navy)
            canvas.setFont("Helvetica-Bold", 5.3)
            chip_label = "LAST" if width < 100 else "LAST CONTACT"
            canvas.drawString(x + inset + 5, y + 11, chip_label)
            value = str(item.get("last_contact_date") or "No contact recorded")
            if compact and value == "No contact recorded":
                value = "No contact"
            canvas.drawRightString(x + width - inset - 5, y + 11, value[:24])

        def _branch_row(self, canvas, item, x, y, width):
            canvas.setFillColor(colors.white)
            canvas.setStrokeColor(branch_border)
            canvas.roundRect(x, y, width, 20, 4, fill=1, stroke=1)
            canvas.setFillColor(navy)
            canvas.setFont("Helvetica-Bold", 6.4)
            name = self._wrapped_lines(canvas, item.get("name") or "Unknown", width - 12, "Helvetica-Bold", 6.4, 1)[0]
            canvas.drawString(x + 6, y + 11, name)
            canvas.setFillColor(muted)
            canvas.setFont("Helvetica", 5.5)
            role = self._wrapped_lines(canvas, item.get("position") or "Branch contact", width - 12, "Helvetica", 5.5, 1)[0]
            canvas.drawString(x + 6, y + 4, role)

        def draw(self):
            canvas = self.canv
            width = self.width
            height = self.height
            canvas.setFillColor(navy)
            canvas.setFont("Helvetica-Bold", 20)
            canvas.drawString(0, height - 24, f"{group_label} - Strategic Contact Report")
            canvas.setFillColor(muted)
            canvas.setFont("Helvetica", 8)
            canvas.drawString(0, height - 39, f"Generated {generated_at} | Organisation overview")

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

            sort_key = lambda item: (int(item.get("org_chart_order") or 0), str(item.get("name") or "").casefold())
            contact_by_id = {str(item.get("id") or ""): item for item in self.rows}
            c_suite = sorted([item for item in self.rows if item.get("org_chart_tier") == "c-suite"], key=sort_key)
            section_leads = defaultdict(list)
            regional = defaultdict(list)
            branches = defaultdict(list)
            placed_ids = {str(item.get("id") or "") for item in c_suite}
            section_keys = {str(section.get("key") or "") for section in business_sections}
            for item in self.rows:
                tier = str(item.get("org_chart_tier") or "")
                section = str(item.get("org_chart_section") or "")
                item_id = str(item.get("id") or "")
                if tier == "section-lead" and section_lead_enabled and section in section_keys:
                    section_leads[section].append(item); placed_ids.add(item_id)
                elif tier == "regional" and section in section_keys:
                    regional[section].append(item); placed_ids.add(item_id)
                elif tier == "branch" and str(item.get("org_chart_parent_id") or "") in contact_by_id:
                    branches[str(item.get("org_chart_parent_id") or "")].append(item); placed_ids.add(item_id)

            # C-suite is a standalone band, as it is on screen.
            c_top = height - 58
            c_height = 76
            canvas.setFillColor(colors.HexColor("#F7F9FC")); canvas.setStrokeColor(border)
            canvas.roundRect(0, c_top - c_height, width, c_height, 7, fill=1, stroke=1)
            canvas.setFillColor(navy); canvas.setFont("Helvetica-Bold", 9); canvas.drawString(9, c_top - 15, "C-SUITE")
            canvas.setFillColor(muted); canvas.setFont("Helvetica", 6.5); canvas.drawString(9, c_top - 26, "Standalone strategic contacts")
            c_card_w, c_card_h = 132, 58
            max_c = max(0, int((width - 95) // (c_card_w + 7)))
            for index, item in enumerate(c_suite[:max_c]):
                self._card(canvas, item, 92 + index * (c_card_w + 7), c_top - c_height + 9, c_card_w, c_card_h, colors.white, border, True)
            if len(c_suite) > max_c:
                canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 7); canvas.drawRightString(width - 9, c_top - c_height + 12, f"+{len(c_suite) - max_c} more")

            unassigned = sorted(
                [item for item in self.rows if str(item.get("id") or "") not in placed_ids],
                key=sort_key,
            )
            section_top = c_top - c_height - 10
            section_bottom = 68 if unassigned else 4
            section_height = section_top - section_bottom
            section_gap = 8
            section_width = (width - section_gap * (len(business_sections) - 1)) / max(1, len(business_sections))
            for section_index, section in enumerate(business_sections):
                key = str(section.get("key") or "")
                label = str(section.get("label") or key.replace("-", " ").title())
                x = section_index * (section_width + section_gap)
                canvas.setFillColor(section_fill); canvas.setStrokeColor(border)
                canvas.roundRect(x, section_bottom, section_width, section_height, 7, fill=1, stroke=1)
                canvas.setFillColor(colors.HexColor("#F4F7FB"))
                canvas.roundRect(x + 1, section_top - 27, section_width - 2, 26, 6, fill=1, stroke=0)
                canvas.setFillColor(navy); canvas.setFont("Helvetica-Bold", 9)
                canvas.drawString(x + 9, section_top - 17, label)
                managers = sorted(regional[key], key=sort_key)
                canvas.setFillColor(blue); canvas.setFont("Helvetica-Bold", 6.5)
                canvas.drawRightString(x + section_width - 9, section_top - 17, f"{len(managers)} REGIONAL")
                cursor_y = section_top - 34
                if section_lead_enabled:
                    canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 5.8)
                    canvas.drawCentredString(x + section_width / 2, cursor_y - 2, section_lead_label.upper())
                    lead_y = cursor_y - 56
                    leads = sorted(section_leads[key], key=sort_key)
                    if leads:
                        self._card(canvas, leads[0], x + section_width * .24, lead_y, section_width * .52, 48, manager_fill, manager_border, True)
                    else:
                        canvas.setFillColor(colors.HexColor("#F7F9FC")); canvas.setStrokeColor(border)
                        canvas.roundRect(x + section_width * .24, lead_y, section_width * .52, 48, 5, fill=1, stroke=1)
                        canvas.setFillColor(muted); canvas.setFont("Helvetica", 6.2)
                        canvas.drawCentredString(x + section_width / 2, lead_y + 21, "No division lead assigned")
                    cursor_y = lead_y - 17
                    canvas.setStrokeColor(manager_border); canvas.setLineWidth(.8)
                    canvas.line(x + section_width / 2, lead_y, x + section_width / 2, cursor_y + 7)
                    canvas.line(x + section_width * .22, cursor_y + 7, x + section_width * .78, cursor_y + 7)
                    canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 5.3)
                    canvas.drawCentredString(x + section_width / 2, cursor_y - 1, "REGIONAL MANAGERS REPORT TO DIVISION LEAD")
                    cursor_y -= 8

                inner_gap = 7
                group_w = (section_width - 18 - inner_gap) / 2
                available = cursor_y - section_bottom - 7
                row_count = max(1, math.ceil(len(managers) / 2))
                group_h = min(104, max(66, (available - inner_gap * max(0, row_count - 1)) / row_count))
                max_rows = max(1, int((available + inner_gap) // (group_h + inner_gap)))
                visible_managers = managers[:max_rows * 2]
                for manager_index, manager in enumerate(visible_managers):
                    row, column = divmod(manager_index, 2)
                    group_x = x + 9 + column * (group_w + inner_gap)
                    group_top = cursor_y - row * (group_h + inner_gap)
                    children = sorted(branches[str(manager.get("id") or "")], key=sort_key)
                    branch_space = max(0, group_h - 58)
                    manager_y = group_top - 54
                    self._card(canvas, manager, group_x, manager_y, group_w, 54, manager_fill, manager_border, True)
                    if branch_space >= 18:
                        canvas.setFillColor(branch_fill); canvas.setStrokeColor(branch_border)
                        canvas.roundRect(group_x, group_top - group_h, group_w, branch_space - 3, 4, fill=1, stroke=1)
                        visible_children = min(len(children), max(0, int((branch_space - 7) // 22)))
                        for child_index, child in enumerate(children[:visible_children]):
                            self._branch_row(canvas, child, group_x + 4, group_top - 77 - child_index * 22, group_w - 8)
                        if len(children) > visible_children:
                            canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 5.5)
                            canvas.drawRightString(group_x + group_w - 5, group_top - group_h + 5, f"+{len(children) - visible_children} branch contacts")
                hidden = len(managers) - len(visible_managers)
                if hidden > 0:
                    canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 6.5)
                    canvas.drawRightString(x + section_width - 9, section_bottom + 8, f"+{hidden} regional managers shown in contact detail")

            if unassigned:
                canvas.setFillColor(colors.HexColor("#F3F5F7")); canvas.setStrokeColor(branch_border)
                canvas.roundRect(0, 4, width, 55, 6, fill=1, stroke=1)
                canvas.setFillColor(navy); canvas.setFont("Helvetica-Bold", 8)
                canvas.drawString(9, 43, "UNASSIGNED")
                canvas.setFillColor(muted); canvas.setFont("Helvetica", 6)
                canvas.drawString(9, 32, "Awaiting placement")
                tray_card_w = 125
                max_unassigned = max(0, int((width - 98) // (tray_card_w + 6)))
                for index, item in enumerate(unassigned[:max_unassigned]):
                    card_x = 92 + index * (tray_card_w + 6)
                    canvas.setFillColor(colors.white); canvas.setStrokeColor(branch_border)
                    canvas.roundRect(card_x, 10, tray_card_w, 42, 4, fill=1, stroke=1)
                    canvas.setFillColor(navy); canvas.setFont("Helvetica-Bold", 7)
                    canvas.drawString(card_x + 6, 36, str(item.get("name") or "Unknown")[:28])
                    canvas.setFillColor(muted); canvas.setFont("Helvetica", 5.7)
                    canvas.drawString(card_x + 6, 25, str(item.get("position") or "Role not set")[:34])
                    canvas.setFillColor(navy); canvas.setFont("Helvetica-Bold", 5.5)
                    canvas.drawString(card_x + 6, 15, str(item.get("last_contact_date") or "No contact")[:24])
                if len(unassigned) > max_unassigned:
                    canvas.setFillColor(muted); canvas.setFont("Helvetica-Bold", 6)
                    canvas.drawRightString(width - 8, 9, f"+{len(unassigned) - max_unassigned} more")

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
