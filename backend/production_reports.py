from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
import re

from production import build_operator_summary, build_production_kpi_payload, filter_production_period


def safe_number(value, digits=1):
    return None if value is None else round(float(value), digits)


def build_production_anomalies(payload):
    anomalies = []
    for item in payload.get("rows", []):
        checks = [
            ("time_booked_today", item.get("time_booked_today"), lambda value: value < 0 or value > 200, "Expected between 0 and 200 hours", "Check the total booked-time calculation and decimal placement."),
            ("utilisation_percentage", item.get("utilisation_percentage"), lambda value: value < 0 or value > 100, "Expected between 0% and 100%", "Check the plant utilisation calculation and source bookings."),
            ("labour_hours", item.get("labour_hours"), lambda value: value < 0 or value > 200, "Expected between 0 and 200 hours", "Check labour-hour entries for the day."),
        ]
        for field, value, invalid, reason, action in checks:
            if value is not None and invalid(float(value)):
                anomalies.append({"type": "Production", "severity": "Review", "date": item.get("date"), "name": "Plant", "record_id": item.get("record_id"), "field": field, "value": value, "reason": reason, "action": action})
        for label, key in (("Sort", "sort_throughput"), ("Grind", "grind_throughput"), ("Press", "press_throughput"), ("Trim", "trim_throughput")):
            if item.get(key) is None:
                anomalies.append({"type": "Production", "severity": "Information", "date": item.get("date"), "name": label, "record_id": item.get("record_id"), "field": key, "value": None, "reason": "No throughput value recorded", "action": "Confirm this department had no activity or correct the missing value."})

    for item in payload.get("operator_rows", []):
        checks = [
            ("booked_hours", item.get("booked_hours"), lambda value: value < 0 or value > 24, "Booked time is negative or above 24 hours", "Check the operator booking entries and decimal placement."),
            ("clocked_hours", item.get("clocked_hours"), lambda value: value < 0 or value > 24, "Clocked time is negative or above 24 hours", "Check the time-clock entry."),
            ("productivity", item.get("productivity"), lambda value: value < 0 or value > 250, "Productivity is outside 0–250%", "Confirm whether unusually high productivity is valid or caused by booking data."),
            ("target_achieved", item.get("target_achieved"), lambda value: value < 0 or value > 250, "Target achieved is outside 0–250%", "Check the LF target calculation and applicability for this operator."),
        ]
        for field, value, invalid, reason, action in checks:
            if value is not None and invalid(float(value)):
                anomalies.append({"type": "Operator", "severity": "Review", "date": item.get("date"), "name": item.get("name"), "record_id": item.get("record_id"), "field": field, "value": value, "reason": reason, "action": action})
        if (item.get("booked_hours") or 0) > 0 and not item.get("clocked_hours"):
            anomalies.append({"type": "Operator", "severity": "Review", "date": item.get("date"), "name": item.get("name"), "record_id": item.get("record_id"), "field": "clocked_hours", "value": item.get("clocked_hours"), "reason": "Booked time exists without clocked time", "action": "Check the operator clock-in record."})
    return sorted(anomalies, key=lambda item: (str(item.get("date") or ""), item.get("type") or "", item.get("name") or ""), reverse=True)


def build_anomaly_workbook(payload):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.worksheet.table import Table, TableStyleInfo

    anomalies = build_production_anomalies(payload)
    workbook = Workbook()
    summary = workbook.active
    summary.title = "Summary"
    summary.append(["Production Data Quality Report"])
    summary.append(["Period", period_label(payload)])
    summary.append(["Generated", datetime.now().strftime("%Y-%m-%d %H:%M")])
    summary.append(["Total anomalies", len(anomalies)])
    summary.append(["Review", sum(item["severity"] == "Review" for item in anomalies)])
    summary.append(["Information", sum(item["severity"] == "Information" for item in anomalies)])
    summary.append([])
    summary.append(["Purpose", "Use this report to investigate unusual or missing FileMaker production entries. Values are flagged for review; they are not automatically assumed to be incorrect."])
    summary["A1"].font = Font(size=18, bold=True, color="245CFF")
    summary.column_dimensions["A"].width = 22
    summary.column_dimensions["B"].width = 95
    summary["B8"].alignment = Alignment(wrap_text=True, vertical="top")

    sheet = workbook.create_sheet("Anomalies")
    headers = ["Severity", "Type", "Date", "Person / Area", "Record ID", "Field", "Value", "Why flagged", "Suggested check", "Correction status", "Correction notes"]
    sheet.append(headers)
    for item in anomalies:
        sheet.append([item.get("severity"), item.get("type"), item.get("date"), item.get("name"), item.get("record_id"), item.get("field"), item.get("value"), item.get("reason"), item.get("action"), "Open", ""])
    if anomalies:
        table = Table(displayName="ProductionAnomalies", ref=f"A1:K{len(anomalies) + 1}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        sheet.add_table(table)
    header_fill = PatternFill("solid", fgColor="245CFF")
    for cell in sheet[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    widths = [13, 13, 13, 22, 13, 24, 14, 38, 48, 18, 42]
    for index, width in enumerate(widths, start=1):
        sheet.column_dimensions[chr(64 + index)].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    sheet.freeze_panes = "A2"

    definitions = workbook.create_sheet("Rules")
    definitions.append(["Rule", "Reason"])
    rules = [
        ("Booked time below 0 or above 24 hours", "Likely correction, booking or decimal-placement issue."),
        ("Clocked time below 0 or above 24 hours", "Outside a plausible daily clocking range."),
        ("Productivity or target outside 0–250%", "May be valid exceptional performance, but warrants review."),
        ("Plant time above 200 hours", "Potential aggregate calculation or decimal-placement issue."),
        ("Missing department throughput", "Could mean no activity or a missing entry; confirmation is required."),
    ]
    for row in rules:
        definitions.append(row)
    definitions.column_dimensions["A"].width = 45
    definitions.column_dimensions["B"].width = 85
    for cell in definitions[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
    for row in definitions.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    output = BytesIO()
    workbook.save(output)
    return output.getvalue(), len(anomalies)


def build_operator_productivity_workbook(operator, period_start, period_end, bookings, clockings, operator_summary=None):
    from openpyxl import Workbook
    from openpyxl.formatting.rule import CellIsRule
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo

    operator_summary = operator_summary or {}
    navy = "17324D"
    blue = "245CFF"
    pale_blue = "EAF2FF"
    pale_red = "FDECEC"
    red = "B42318"
    pale_amber = "FFF4D6"
    amber = "9A6700"
    pale_green = "E7F6EF"
    green = "177245"
    muted = "5E7184"
    border = Border(bottom=Side(style="thin", color="D9E2EC"))

    def parse_timestamp(value):
        text = str(value or "").strip()
        for pattern in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, pattern)
            except ValueError:
                continue
        return None

    def parse_date(value):
        try:
            return datetime.strptime(str(value or ""), "%Y-%m-%d").date()
        except ValueError:
            return None

    def style_title(sheet, title, subtitle):
        sheet.sheet_view.showGridLines = False
        sheet["A2"] = title
        sheet["A2"].font = Font(name="Arial", size=16, bold=True, color=blue)
        sheet["A3"] = subtitle
        sheet["A3"].font = Font(name="Arial", size=10, italic=True, color=muted)
        sheet["A4"] = ""
        sheet.row_dimensions[2].height = 24

    def style_headers(sheet, row, columns):
        for cell in sheet[row][:columns]:
            cell.fill = PatternFill("solid", fgColor=navy)
            cell.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        sheet.row_dimensions[row].height = 28

    def style_body(sheet, min_row, max_row, columns):
        for row in sheet.iter_rows(min_row=min_row, max_row=max_row, min_col=1, max_col=columns):
            for cell in row:
                cell.font = Font(name="Arial", size=10, color="1E293B")
                cell.alignment = Alignment(vertical="center")
                cell.border = border

    daily = defaultdict(lambda: {"clocked": 0.0, "booked": 0.0})
    departments = defaultdict(lambda: {"hours": 0.0, "bookings": 0})
    for item in bookings:
        date_key = str(item.get("date") or "")
        daily[date_key]["booked"] += float(item.get("hours") or 0)
        department = str(item.get("department") or "Unassigned").strip() or "Unassigned"
        departments[department]["hours"] += float(item.get("hours") or 0)
        departments[department]["bookings"] += 1
    for item in clockings:
        daily[str(item.get("date") or "")]["clocked"] += float(item.get("clocked_hours") or 0)

    daily_rows = []
    for date_key, values in sorted(daily.items()):
        clocked = values["clocked"]
        booked = values["booked"]
        lost = max(clocked - booked, 0)
        overbooked = max(booked - clocked, 0)
        daily_rows.append({
            "date": parse_date(date_key) or date_key,
            "clocked": clocked,
            "booked": booked,
            "lost": lost,
            "overbooked": overbooked,
            "productivity": booked / clocked if clocked else None,
            "status": "Lost time" if lost > 0.005 else "Overbooked" if overbooked > 0.005 else "Reconciled",
        })

    total_clocked = sum(item["clocked"] for item in daily_rows)
    total_booked = sum(item["booked"] for item in daily_rows)
    total_lost = sum(item["lost"] for item in daily_rows)
    total_overbooked = sum(item["overbooked"] for item in daily_rows)
    days_with_lost_time = sum(item["lost"] > 0.005 for item in daily_rows)

    workbook = Workbook()
    summary = workbook.active
    summary.title = "Summary"
    style_title(summary, f"{operator} productivity report", f"{datetime.strptime(period_start, '%Y-%m-%d').strftime('%m/%d/%Y')} to {datetime.strptime(period_end, '%Y-%m-%d').strftime('%m/%d/%Y')}")
    kpis = [
        ("Clocked time", total_clocked),
        ("Booked time", total_booked),
        ("Lost / unbooked time", total_lost),
        ("Overbooked time", total_overbooked),
        ("Productivity", total_booked / total_clocked if total_clocked else None),
        ("Days with lost time", days_with_lost_time),
    ]
    for column, (label, value) in enumerate(kpis, start=1):
        summary.cell(5, column, label)
        summary.cell(6, column, value)
        summary.cell(5, column).font = Font(name="Arial", size=9, bold=True, color=muted)
        summary.cell(5, column).alignment = Alignment(horizontal="center")
        summary.cell(6, column).font = Font(name="Arial", size=14, bold=True, color=red if label == "Lost / unbooked time" and total_lost else navy)
        summary.cell(6, column).alignment = Alignment(horizontal="center")
        summary.cell(6, column).fill = PatternFill("solid", fgColor=pale_red if label == "Lost / unbooked time" and total_lost else pale_blue)
        summary.cell(6, column).number_format = "0.0%" if label == "Productivity" else "0" if label == "Days with lost time" else "0.00"
    summary["A8"] = "How lost time is calculated"
    summary["A8"].font = Font(name="Arial", size=11, bold=True, color=navy)
    summary["A9"] = "Lost / unbooked time is the positive difference between paid clocked time and productive booked time for each day. Use the daily rows below, then review the matching Time Bookings and Clockings tabs."
    summary["A9"].alignment = Alignment(wrap_text=True, vertical="top")
    summary["A9"].font = Font(name="Arial", size=10, color=muted)
    summary.merge_cells("A9:F9")
    summary.row_dimensions[9].height = 42
    summary.append([])
    header_row = 11
    summary.append(["Date", "Clocked time", "Booked time", "Lost / unbooked time", "Overbooked time", "Productivity", "Status"])
    for item in daily_rows:
        summary.append([item["date"], item["clocked"], item["booked"], item["lost"], item["overbooked"], item["productivity"], item["status"]])
    style_headers(summary, header_row, 7)
    if daily_rows:
        style_body(summary, header_row + 1, header_row + len(daily_rows), 7)
        table = Table(displayName="OperatorDailyReconciliation", ref=f"A{header_row}:G{header_row + len(daily_rows)}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        summary.add_table(table)
        summary.conditional_formatting.add(f"D{header_row + 1}:D{header_row + len(daily_rows)}", CellIsRule(operator="greaterThan", formula=["0.005"], fill=PatternFill("solid", fgColor=pale_red), font=Font(color=red, bold=True)))
        summary.conditional_formatting.add(f"E{header_row + 1}:E{header_row + len(daily_rows)}", CellIsRule(operator="greaterThan", formula=["0.005"], fill=PatternFill("solid", fgColor=pale_amber), font=Font(color=amber, bold=True)))
        for row in range(header_row + 1, header_row + len(daily_rows) + 1):
            summary.cell(row, 1).number_format = "mm/dd/yyyy"
            for column in (2, 3, 4, 5):
                summary.cell(row, column).number_format = "0.00"
            summary.cell(row, 6).number_format = "0.0%"
            status_cell = summary.cell(row, 7)
            if status_cell.value == "Lost time":
                status_cell.fill = PatternFill("solid", fgColor=pale_red); status_cell.font = Font(name="Arial", size=10, bold=True, color=red)
            elif status_cell.value == "Overbooked":
                status_cell.fill = PatternFill("solid", fgColor=pale_amber); status_cell.font = Font(name="Arial", size=10, bold=True, color=amber)
            else:
                status_cell.fill = PatternFill("solid", fgColor=pale_green); status_cell.font = Font(name="Arial", size=10, bold=True, color=green)
    summary.freeze_panes = f"A{header_row + 1}"
    for column, width in {"A": 14, "B": 16, "C": 16, "D": 22, "E": 18, "F": 14, "G": 16}.items():
        summary.column_dimensions[column].width = width

    department_sheet = workbook.create_sheet("Departments")
    style_title(department_sheet, "Time by department", f"{operator} booked time")
    department_sheet.append(["Department", "Booked time", "Share of booked time", "Bookings"])
    department_header = 5
    for department, values in sorted(departments.items(), key=lambda item: (-item[1]["hours"], item[0].casefold())):
        department_sheet.append([department, values["hours"], values["hours"] / total_booked if total_booked else None, values["bookings"]])
    style_headers(department_sheet, department_header, 4)
    if departments:
        style_body(department_sheet, 6, 5 + len(departments), 4)
        table = Table(displayName="OperatorDepartmentSummary", ref=f"A5:D{5 + len(departments)}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        department_sheet.add_table(table)
        for row in range(6, 6 + len(departments)):
            department_sheet.cell(row, 2).number_format = "0.00"
            department_sheet.cell(row, 3).number_format = "0.0%"
    department_sheet.freeze_panes = "A6"
    for column, width in {"A": 28, "B": 18, "C": 22, "D": 14}.items():
        department_sheet.column_dimensions[column].width = width

    booking_sheet = workbook.create_sheet("Time Bookings")
    style_title(booking_sheet, "Time booking records", f"{operator} source records")
    booking_sheet.append(["Date", "Department", "Start", "Finish", "Booked time", "Sales order", "Notes", "Record ID"])
    booking_header = 5
    for item in sorted(bookings, key=lambda row: (row.get("date") or "", row.get("start_time") or "")):
        booking_sheet.append([parse_date(item.get("date")) or item.get("date"), item.get("department") or "Unassigned", parse_timestamp(item.get("start_time")) or item.get("start_time"), parse_timestamp(item.get("finish_time")) or item.get("finish_time"), float(item.get("hours") or 0), item.get("sales_order") or "", item.get("notes") or "", item.get("record_id") or ""])
    style_headers(booking_sheet, booking_header, 8)
    if bookings:
        style_body(booking_sheet, 6, 5 + len(bookings), 8)
        table = Table(displayName="OperatorTimeBookings", ref=f"A5:H{5 + len(bookings)}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        booking_sheet.add_table(table)
        for row in range(6, 6 + len(bookings)):
            booking_sheet.cell(row, 1).number_format = "mm/dd/yyyy"
            booking_sheet.cell(row, 3).number_format = "mm/dd/yyyy h:mm AM/PM"
            booking_sheet.cell(row, 4).number_format = "mm/dd/yyyy h:mm AM/PM"
            booking_sheet.cell(row, 5).number_format = "0.00"
    booking_sheet.freeze_panes = "A6"
    for column, width in {"A": 14, "B": 24, "C": 22, "D": 22, "E": 16, "F": 18, "G": 42, "H": 14}.items():
        booking_sheet.column_dimensions[column].width = width

    clocking_sheet = workbook.create_sheet("Clockings")
    style_title(clocking_sheet, "Clocking records", f"{operator} paid-time source records")
    clocking_sheet.append(["Date", "Clock in", "Clock out", "Paid time", "Notes", "Record ID"])
    clocking_header = 5
    for item in sorted(clockings, key=lambda row: (row.get("date") or "", row.get("clock_in") or "")):
        clocking_sheet.append([parse_date(item.get("date")) or item.get("date"), parse_timestamp(item.get("clock_in")) or item.get("clock_in"), parse_timestamp(item.get("clock_out")) or item.get("clock_out"), float(item.get("clocked_hours") or 0), item.get("notes") or "", item.get("record_id") or ""])
    style_headers(clocking_sheet, clocking_header, 6)
    if clockings:
        style_body(clocking_sheet, 6, 5 + len(clockings), 6)
        table = Table(displayName="OperatorClockings", ref=f"A5:F{5 + len(clockings)}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        clocking_sheet.add_table(table)
        for row in range(6, 6 + len(clockings)):
            clocking_sheet.cell(row, 1).number_format = "mm/dd/yyyy"
            clocking_sheet.cell(row, 2).number_format = "mm/dd/yyyy h:mm AM/PM"
            clocking_sheet.cell(row, 3).number_format = "mm/dd/yyyy h:mm AM/PM"
            clocking_sheet.cell(row, 4).number_format = "0.00"
    clocking_sheet.freeze_panes = "A6"
    for column, width in {"A": 14, "B": 22, "C": 22, "D": 16, "E": 42, "F": 14}.items():
        clocking_sheet.column_dimensions[column].width = width

    for sheet in workbook.worksheets:
        sheet.sheet_properties.pageSetUpPr.fitToPage = True
        sheet.page_setup.fitToWidth = 1
        sheet.page_setup.fitToHeight = 0
        sheet.sheet_properties.tabColor = blue if sheet.title == "Summary" else pale_blue

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def build_press_analysis_workbook(period_start, period_end, production_rows, cycles, mats=None):
    from openpyxl import Workbook
    from openpyxl.formatting.rule import CellIsRule
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo

    navy = "17324D"
    blue = "245CFF"
    pale_blue = "EAF2FF"
    pale_amber = "FFF4D6"
    amber = "9A6700"
    muted = "5E7184"
    border = Border(bottom=Side(style="thin", color="D9E2EC"))

    def parse_date(value):
        try:
            return datetime.strptime(str(value or ""), "%Y-%m-%d").date()
        except ValueError:
            return value or ""

    def parse_timestamp(value):
        text = str(value or "").strip()
        for pattern in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, pattern)
            except ValueError:
                continue
        return text

    def title(sheet, heading, subtitle):
        sheet.sheet_view.showGridLines = False
        sheet["A2"] = heading
        sheet["A2"].font = Font(name="Arial", size=16, bold=True, color=blue)
        sheet["A3"] = subtitle
        sheet["A3"].font = Font(name="Arial", size=10, italic=True, color=muted)
        sheet["A4"] = ""
        sheet.row_dimensions[2].height = 24

    def headers(sheet, row, count):
        for cell in sheet[row][:count]:
            cell.fill = PatternFill("solid", fgColor=navy)
            cell.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        sheet.row_dimensions[row].height = 30

    def body(sheet, first, last, count):
        if last < first:
            return
        for row in sheet.iter_rows(min_row=first, max_row=last, min_col=1, max_col=count):
            for cell in row:
                cell.font = Font(name="Arial", size=10, color="1E293B")
                cell.alignment = Alignment(vertical="center")
                cell.border = border

    mats = mats or []
    period_text = f"{datetime.strptime(period_start, '%Y-%m-%d').strftime('%m/%d/%Y')} to {datetime.strptime(period_end, '%Y-%m-%d').strftime('%m/%d/%Y')}"
    workbook = Workbook()
    summary = workbook.active
    summary.title = "Summary"
    title(summary, "Press analysis", period_text)
    summary["A5"] = "Selected-period totals"
    summary["A5"].font = Font(name="Arial", size=11, bold=True, color=navy)
    metrics = [
        ("Completed production days", f"=COUNTA('Daily Press'!A6:A{max(6, len(production_rows) + 5)})", "0"),
        ("Total dashboard linear feet", f"=SUM('Daily Press'!E6:E{max(6, len(production_rows) + 5)})", "#,##0"),
        ("Average daily linear feet", f'=IF(A6=0,"n.a.",B6/A6)', "#,##0"),
        ("Cycle records", f"=COUNTA('Cycle Detail'!A6:A{max(6, len(cycles) + 5)})", "0"),
        ("Cycle-detail linear feet", f"=SUM('Cycle Detail'!K6:K{max(6, len(cycles) + 5)})", "#,##0"),
        ("Orders shared by operators", f"=COUNTA('Shared Orders'!A6:A{max(6, len(cycles) + 5)})", "0"),
    ]
    for column, (label, formula, number_format) in enumerate(metrics, start=1):
        summary.cell(5, column, label)
        summary.cell(6, column, formula)
        summary.cell(5, column).font = Font(name="Arial", size=9, bold=True, color=muted)
        summary.cell(5, column).alignment = Alignment(horizontal="center", wrap_text=True)
        summary.cell(6, column).font = Font(name="Arial", size=14, bold=True, color=navy)
        summary.cell(6, column).alignment = Alignment(horizontal="center")
        summary.cell(6, column).fill = PatternFill("solid", fgColor=pale_blue)
        summary.cell(6, column).number_format = number_format
        summary.column_dimensions[summary.cell(1, column).column_letter].width = 22
    summary["A8"] = "How to investigate a changing figure"
    summary["A8"].font = Font(name="Arial", size=11, bold=True, color=navy)
    summary["A9"] = "Daily Press reproduces the dashboard's completed-day FF1, FF2 and FF3 totals. Cycle Detail shows cycle attribution. Mat Analysis explains the size and value mix by operator. Shared Orders is a review list, not proof of an error."
    summary.merge_cells("A9:F9")
    summary["A9"].font = Font(name="Arial", size=10, color=muted)
    summary["A9"].alignment = Alignment(wrap_text=True, vertical="top")
    summary.row_dimensions[9].height = 48

    daily = workbook.create_sheet("Daily Press")
    title(daily, "Daily press performance", "These figures match the completed-day data used by the dashboard")
    daily.append(["Date", "FF1 linear feet", "FF2 linear feet", "FF3 linear feet", "Total linear feet", "FF1 mats", "FF2 mats", "FF3 mats", "Total mats", "FF1 cycles", "FF2 cycles", "FF3 cycles", "Total cycles", "Press repair value"])
    for index, item in enumerate(sorted(production_rows, key=lambda row: row.get("date") or ""), start=6):
        daily.append([
            parse_date(item.get("date")), item.get("ff1_lf"), item.get("ff2_lf"), item.get("ff3_lf"),
            f"=SUM(B{index}:D{index})", item.get("ff1_mats"), item.get("ff2_mats"), item.get("ff3_mats"),
            f"=SUM(F{index}:H{index})", item.get("ff1_cycles"), item.get("ff2_cycles"), item.get("ff3_cycles"),
            f"=SUM(J{index}:L{index})", sum(item.get(f"ff{press}_revenue") or 0 for press in (1, 2, 3)),
        ])
    headers(daily, 5, 14)
    body(daily, 6, len(production_rows) + 5, 14)
    if production_rows:
        table = Table(displayName="DailyPressPerformance", ref=f"A5:N{len(production_rows) + 5}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        daily.add_table(table)
        for row in range(6, len(production_rows) + 6):
            daily.cell(row, 1).number_format = "mm/dd/yyyy"
            for column in range(2, 14):
                daily.cell(row, column).number_format = "#,##0"
            daily.cell(row, 14).number_format = '"$"#,##0.00'
    daily.freeze_panes = "B6"
    for column, width in {"A": 14, "B": 16, "C": 16, "D": 16, "E": 18, "F": 12, "G": 12, "H": 12, "I": 12, "J": 12, "K": 12, "L": 12, "M": 13, "N": 18}.items():
        daily.column_dimensions[column].width = width

    cycle_sheet = workbook.create_sheet("Cycle Detail")
    title(cycle_sheet, "Cycle detail", "Current FileMaker cycle records for the selected period")
    cycle_headers = ["Date", "Start", "Finish", "Order reference", "Cycle number", "Press", "Operator", "Extra operator", "Mats", "Edges", "Cycle linear feet", "FF1 linear feet", "FF2 linear feet", "FF3 linear feet", "Order LF summary", "Repair value", "Cycle time (hours)", "Record ID", "Modified"]
    cycle_sheet.append(cycle_headers)
    for item in sorted(cycles, key=lambda row: (row.get("date") or "", row.get("start_time") or "", row.get("cycle_number") or "")):
        cycle_sheet.append([
            parse_date(item.get("date")), parse_timestamp(item.get("start_time")), parse_timestamp(item.get("finish_time")),
            item.get("order_reference") or "", item.get("cycle_number") or "", item.get("machine") or "", item.get("operator") or "",
            item.get("extra_operator") or "", item.get("mat_count"), item.get("edges"), item.get("linear_feet"), item.get("ff1_lf"),
            item.get("ff2_lf"), item.get("ff3_lf"), item.get("order_linear_feet_summary"), item.get("repair_value"),
            item.get("cycle_hours"), item.get("record_id") or "", item.get("modified_at") or "",
        ])
    headers(cycle_sheet, 5, len(cycle_headers))
    body(cycle_sheet, 6, len(cycles) + 5, len(cycle_headers))
    if cycles:
        table = Table(displayName="PressCycleDetail", ref=f"A5:S{len(cycles) + 5}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        cycle_sheet.add_table(table)
        for row in range(6, len(cycles) + 6):
            cycle_sheet.cell(row, 1).number_format = "mm/dd/yyyy"
            cycle_sheet.cell(row, 2).number_format = "mm/dd/yyyy h:mm AM/PM"
            cycle_sheet.cell(row, 3).number_format = "mm/dd/yyyy h:mm AM/PM"
            for column in (4, 5, 6, 7, 8, 18, 19):
                cycle_sheet.cell(row, column).alignment = Alignment(horizontal="left", vertical="center")
            for column in range(9, 16):
                cycle_sheet.cell(row, column).number_format = "#,##0"
            cycle_sheet.cell(row, 16).number_format = '"$"#,##0.00'
            cycle_sheet.cell(row, 17).number_format = "0.00"
    cycle_sheet.freeze_panes = "G6"
    for column, width in {"A": 13, "B": 22, "C": 22, "D": 18, "E": 16, "F": 10, "G": 22, "H": 22, "I": 10, "J": 10, "K": 17, "L": 15, "M": 15, "N": 15, "O": 17, "P": 15, "Q": 18, "R": 12, "S": 20}.items():
        cycle_sheet.column_dimensions[column].width = width

    cycle_lookup = {
        str(item.get("cycle_number") or "").strip(): item
        for item in cycles if str(item.get("cycle_number") or "").strip()
    }
    mat_groups = defaultdict(lambda: {"mats": 0, "linear_feet": 0.0, "square_feet": 0.0, "repair_value": 0.0, "sizes": defaultdict(int)})
    for item in mats:
        cycle = cycle_lookup.get(str(item.get("cycle_number") or "").strip(), {})
        operator = str(cycle.get("operator") or "Unassigned").strip() or "Unassigned"
        group = mat_groups[operator]
        group["mats"] += 1
        group["linear_feet"] += float(item.get("linear_feet") or 0)
        group["square_feet"] += float(item.get("square_feet") or 0)
        group["repair_value"] += float(item.get("repair_value") or 0)
        size = str(item.get("mat_size") or "Not recorded").strip() or "Not recorded"
        group["sizes"][size] += 1

    mat_sheet = workbook.create_sheet("Mat Analysis")
    title(mat_sheet, "Mat size and value analysis", "Mat-level FileMaker records allocated to operators through their press cycle")
    mat_sheet.append(["Operator", "Mats", "Linear feet", "Average LF per mat", "Square feet", "Average sq ft per mat", "Repair value", "Value per mat", "Value per LF", "Most common sizes"])
    for operator, values in sorted(mat_groups.items(), key=lambda pair: (-pair[1]["repair_value"], pair[0])):
        common_sizes = ", ".join(f"{size} ({count})" for size, count in sorted(values["sizes"].items(), key=lambda pair: (-pair[1], pair[0]))[:5])
        mat_sheet.append([
            operator, values["mats"], values["linear_feet"], values["linear_feet"] / values["mats"] if values["mats"] else None,
            values["square_feet"], values["square_feet"] / values["mats"] if values["mats"] else None,
            values["repair_value"], values["repair_value"] / values["mats"] if values["mats"] else None,
            values["repair_value"] / values["linear_feet"] if values["linear_feet"] else None, common_sizes,
        ])
    headers(mat_sheet, 5, 10)
    summary_last_row = len(mat_groups) + 5
    body(mat_sheet, 6, summary_last_row, 10)
    if mat_groups:
        summary_table = Table(displayName="MatMixByOperator", ref=f"A5:J{summary_last_row}")
        summary_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        mat_sheet.add_table(summary_table)
        for row in range(6, summary_last_row + 1):
            mat_sheet.cell(row, 2).number_format = "0"
            for column in (3, 4, 5, 6):
                mat_sheet.cell(row, column).number_format = "#,##0.0"
            for column in (7, 8, 9):
                mat_sheet.cell(row, column).number_format = '"$"#,##0.00'

    detail_header_row = summary_last_row + 3
    detail_headers = ["Press date", "Order", "Cycle", "Press", "Operator", "Mat number", "Mat size", "Mat type", "Outcome", "Height", "Width", "Square feet", "Linear feet", "Repair value", "Repairs", "Re-cooks", "Record ID"]
    for column, label in enumerate(detail_headers, start=1):
        mat_sheet.cell(detail_header_row, column, label)
    for row_number, item in enumerate(sorted(mats, key=lambda row: (row.get("press_date") or "", row.get("cycle_number") or "", row.get("mat_number") or "")), start=detail_header_row + 1):
        cycle = cycle_lookup.get(str(item.get("cycle_number") or "").strip(), {})
        values = [parse_date(item.get("press_date")), item.get("order_reference"), item.get("cycle_number"), cycle.get("machine"), cycle.get("operator"), item.get("mat_number"), item.get("mat_size"), item.get("mat_type"), item.get("mat_outcome"), item.get("height"), item.get("width"), item.get("square_feet"), item.get("linear_feet"), item.get("repair_value"), item.get("repair_count"), item.get("recook_count"), item.get("record_id")]
        for column, value in enumerate(values, start=1):
            mat_sheet.cell(row_number, column, value)
    headers(mat_sheet, detail_header_row, len(detail_headers))
    detail_last_row = detail_header_row + len(mats)
    body(mat_sheet, detail_header_row + 1, detail_last_row, len(detail_headers))
    if mats:
        detail_table = Table(displayName="PressMatDetail", ref=f"A{detail_header_row}:Q{detail_last_row}")
        detail_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        mat_sheet.add_table(detail_table)
        for row in range(detail_header_row + 1, detail_last_row + 1):
            mat_sheet.cell(row, 1).number_format = "mm/dd/yyyy"
            for column in (10, 11, 12, 13, 15, 16):
                mat_sheet.cell(row, column).number_format = "#,##0.0"
            mat_sheet.cell(row, 14).number_format = '"$"#,##0.00'
    mat_sheet.freeze_panes = f"A{detail_header_row + 1}"
    for column, width in {"A": 14, "B": 14, "C": 16, "D": 10, "E": 22, "F": 18, "G": 14, "H": 16, "I": 14, "J": 11, "K": 11, "L": 14, "M": 14, "N": 16, "O": 12, "P": 12, "Q": 14}.items():
        mat_sheet.column_dimensions[column].width = width

    shared = defaultdict(lambda: {"operators": set(), "presses": set(), "cycles": 0, "linear_feet": 0.0})
    for item in cycles:
        order = str(item.get("order_reference") or "").strip()
        date_key = str(item.get("date") or "").strip()
        operator = str(item.get("operator") or "").strip()
        if not order or not date_key or not operator:
            continue
        entry = shared[(date_key, order)]
        entry["operators"].add(operator)
        if item.get("machine"):
            entry["presses"].add(str(item.get("machine")))
        entry["cycles"] += 1
        entry["linear_feet"] += float(item.get("linear_feet") or 0)
    shared_rows = [
        (date_key, order, values)
        for (date_key, order), values in shared.items()
        if len(values["operators"]) > 1
    ]
    shared_rows.sort(key=lambda item: (item[0], item[1]))
    review = workbook.create_sheet("Shared Orders")
    title(review, "Orders shared by operators", "Order/day combinations with cycle records assigned to more than one operator")
    review.append(["Date", "Order reference", "Operators", "Presses", "Cycle records", "Cycle-detail linear feet", "Review note"])
    for date_key, order, values in shared_rows:
        review.append([parse_date(date_key), order, ", ".join(sorted(values["operators"])), ", ".join(sorted(values["presses"])), values["cycles"], values["linear_feet"], "Confirm the work was intentionally shared between operators."])
    headers(review, 5, 7)
    body(review, 6, len(shared_rows) + 5, 7)
    if shared_rows:
        table = Table(displayName="SharedOrderReview", ref=f"A5:G{len(shared_rows) + 5}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True, showFirstColumn=False, showLastColumn=False)
        review.add_table(table)
        review.conditional_formatting.add(f"E6:E{len(shared_rows) + 5}", CellIsRule(operator="greaterThan", formula=["0"], fill=PatternFill("solid", fgColor=pale_amber), font=Font(color=amber, bold=True)))
        for row in range(6, len(shared_rows) + 6):
            review.cell(row, 1).number_format = "mm/dd/yyyy"
            for column in (2, 3, 4, 7):
                review.cell(row, column).alignment = Alignment(horizontal="left", vertical="top" if column == 7 else "center", wrap_text=column == 7)
            review.cell(row, 5).number_format = "0"
            review.cell(row, 6).number_format = "#,##0"
    review.freeze_panes = "A6"
    for column, width in {"A": 14, "B": 18, "C": 38, "D": 18, "E": 15, "F": 24, "G": 54}.items():
        review.column_dimensions[column].width = width

    for sheet in workbook.worksheets:
        sheet.sheet_properties.pageSetUpPr.fitToPage = True
        sheet.page_setup.fitToWidth = 1
        sheet.page_setup.fitToHeight = 0
        sheet.sheet_properties.tabColor = blue if sheet.title == "Summary" else pale_blue

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def summarize_rows(rows, operator_rows):
    revenue = [item.get("production_revenue_today") for item in rows if item.get("production_revenue_today") is not None]
    press_lf = [item.get("press_throughput") for item in rows if item.get("press_throughput") is not None]
    booked = sum(item.get("booked_hours") or 0 for item in operator_rows if (item.get("clocked_hours") or 0) > 0)
    clocked = sum(item.get("clocked_hours") or 0 for item in operator_rows if (item.get("clocked_hours") or 0) > 0)
    return {
        "production_days": len(rows),
        "total_revenue": sum(revenue),
        "average_daily_revenue": sum(revenue) / len(revenue) if revenue else None,
        "total_press_lf": sum(press_lf),
        "average_press_lf": sum(press_lf) / len(press_lf) if press_lf else None,
        "total_mats_repaired_through_presses": sum(sum(item.get(f"ff{press}_mats") or 0 for press in (1, 2, 3)) for item in rows),
        "total_cycles": sum(sum(item.get(f"ff{press}_cycles") or 0 for press in (1, 2, 3)) for item in rows),
        "recook_lf": sum(item.get("recook_lf") or 0 for item in rows),
        "labour_hours": sum(item.get("labour_hours") or 0 for item in rows),
        "plant_productivity": booked / clocked * 100 if clocked else None,
        "latest_backlog_weeks": rows[-1].get("backlog_weeks") if rows else None,
        "department_average_lf": {
            key: safe_number(sum(values) / len(values), 0) if values else None
            for key, values in {
                label: [item.get(field) for item in rows if item.get(field) is not None]
                for label, field in (("Sort", "sort_throughput"), ("Grind", "grind_throughput"), ("Press", "press_throughput"), ("Trim", "trim_throughput"))
            }.items()
        },
    }


def build_ai_report_facts(result, days):
    current_rows, current_operators = filter_production_period(result, days=days)
    plant_operator_rows = result.get("plant_operator_rows", result.get("operator_rows", []))
    if current_rows:
        current_end = datetime.strptime(current_rows[-1]["date"], "%Y-%m-%d")
        current_start = current_end - timedelta(days=max(1, days) - 1)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=max(1, days) - 1)
        previous_rows = [item for item in result.get("production_rows", []) if previous_start.strftime("%Y-%m-%d") <= item["date"] <= previous_end.strftime("%Y-%m-%d")]
        previous_operators = [item for item in result.get("operator_rows", []) if previous_start.strftime("%Y-%m-%d") <= item["date"] <= previous_end.strftime("%Y-%m-%d")]
        current_plant_operators = [item for item in plant_operator_rows if current_start.strftime("%Y-%m-%d") <= item["date"] <= current_end.strftime("%Y-%m-%d")]
        previous_plant_operators = [item for item in plant_operator_rows if previous_start.strftime("%Y-%m-%d") <= item["date"] <= previous_end.strftime("%Y-%m-%d")]
    else:
        current_start = current_end = previous_start = previous_end = None
        previous_rows, previous_operators = [], []
        current_plant_operators, previous_plant_operators = [], []
    operators = build_operator_summary(current_operators)
    facts = {
        "period": {"start": current_start.strftime("%Y-%m-%d") if current_start else "", "end": current_end.strftime("%Y-%m-%d") if current_end else "", "rolling_days": days},
        "current": summarize_rows(current_rows, current_plant_operators),
        "previous_period": {"start": previous_start.strftime("%Y-%m-%d") if previous_start else "", "end": previous_end.strftime("%Y-%m-%d") if previous_end else "", **summarize_rows(previous_rows, previous_plant_operators)},
        "operators": [{key: safe_number(value) if isinstance(value, float) else value for key, value in item.items()} for item in operators],
        "business_context": {
            "preferred_backlog_weeks": 2.0,
            "backlog_interpretation": "Backlog below approximately two weeks means production has less new-order repair work available and should prompt sales activity to secure more orders. Growth toward two weeks is positive, not a turnaround concern. A low backlog normally enables faster-than-usual customer turnaround.",
        },
        "data_notes": ["Today is excluded.", "Plant productivity is total booked productive hours divided by total plant clocked hours and includes all plant clockings.", "Backlog contains new customer orders awaiting their first repair only; re-cook work is never included in backlog.", "The preferred backlog is approximately two weeks. A backlog below that level can constrain production because only received customer mats can be repaired, and should prompt the sales team to secure more orders.", "Backlog growth that remains below approximately two weeks is movement toward a healthier workload and must not be described as harming turnaround. Low backlog normally means orders can be returned faster than usual.", "Re-cook activity is a separate quality measure and is separate from press throughput.", "Press throughput is FF1 + FF2 + FF3 total LF.", "Trudy Dunlap, Kelly Bainbridge, Lois Horace, Temp1 and Temp2 are excluded only from the individual operator analysis; their plant clockings remain included in overall plant productivity."],
    }
    return facts


def build_ai_report_facts_for_calendar_period(result, period_start, period_end, previous_start, previous_end):
    def within(items, start, end):
        return [
            item for item in items
            if start <= str(item.get("date") or "") <= end
        ]

    production_rows = result.get("production_rows", [])
    operator_rows = result.get("operator_rows", [])
    plant_operator_rows = result.get("plant_operator_rows", operator_rows)
    current_rows = within(production_rows, period_start, period_end)
    current_operators = within(operator_rows, period_start, period_end)
    current_plant_operators = within(plant_operator_rows, period_start, period_end)
    previous_rows = within(production_rows, previous_start, previous_end)
    previous_operators = within(operator_rows, previous_start, previous_end)
    previous_plant_operators = within(plant_operator_rows, previous_start, previous_end)
    operators = build_operator_summary(current_operators)
    return {
        "period": {
            "start": period_start,
            "end": period_end,
            "rolling_days": None,
            "calendar_month": True,
        },
        "current": summarize_rows(current_rows, current_plant_operators),
        "previous_period": {
            "start": previous_start,
            "end": previous_end,
            **summarize_rows(previous_rows, previous_plant_operators),
        },
        "operators": [
            {key: safe_number(value) if isinstance(value, float) else value for key, value in item.items()}
            for item in operators
        ],
        "business_context": {
            "preferred_backlog_weeks": 2.0,
            "backlog_interpretation": "Backlog below approximately two weeks means production has less new-order repair work available and should prompt sales activity to secure more orders. Growth toward two weeks is positive, not a turnaround concern. A low backlog normally enables faster-than-usual customer turnaround.",
        },
        "data_notes": [
            "The current and previous periods are complete calendar months.",
            "Today is excluded.",
            "Plant productivity is total booked productive hours divided by total plant clocked hours and includes all plant clockings.",
            "Backlog contains new customer orders awaiting their first repair only; re-cook work is never included in backlog.",
            "The preferred backlog is approximately two weeks. A backlog below that level can constrain production because only received customer mats can be repaired, and should prompt the sales team to secure more orders.",
            "Backlog growth that remains below approximately two weeks is movement toward a healthier workload and must not be described as harming turnaround. Low backlog normally means orders can be returned faster than usual.",
            "Re-cook activity is a separate quality measure and is separate from press throughput.",
            "Press throughput is FF1 + FF2 + FF3 total LF.",
            "Trudy Dunlap, Kelly Bainbridge, Lois Horace, Temp1 and Temp2 are excluded only from the individual operator analysis; their plant clockings remain included in overall plant productivity.",
        ],
    }


def period_label(payload):
    rows = payload.get("rows", [])
    return f"{rows[0]['date']} to {rows[-1]['date']}" if rows else "No completed production dates"


def clean_filename(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "")).strip("-")


def build_ai_analysis_docx(facts, analysis):
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Inches, Pt, RGBColor

    document = Document()
    section = document.sections[0]
    section.top_margin = Inches(0.65); section.bottom_margin = Inches(0.65); section.left_margin = Inches(0.7); section.right_margin = Inches(0.7)
    styles = document.styles
    styles["Normal"].font.name = "Arial"; styles["Normal"].font.size = Pt(10)
    for name, size, color in (("Title", 24, "245CFF"), ("Heading 1", 15, "1F2933"), ("Heading 2", 12, "245CFF")):
        styles[name].font.name = "Arial"; styles[name].font.size = Pt(size); styles[name].font.color.rgb = RGBColor.from_string(color); styles[name].font.bold = True
    title = document.add_paragraph(style="Title"); title.add_run("Numat Production Analysis")
    subtitle = document.add_paragraph(f"{facts['period']['start']} to {facts['period']['end']} · Completed production days only")
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    current = facts["current"]
    document.add_heading("1. Overall production summary", level=1)
    table = document.add_table(rows=2, cols=4); table.style = "Light Shading Accent 1"
    metrics = [("Production days", current["production_days"]), ("Average daily value", f"${current['average_daily_revenue']:,.0f}" if current["average_daily_revenue"] is not None else "—"), ("Average press LF", f"{current['average_press_lf']:,.0f}" if current["average_press_lf"] is not None else "—"), ("Plant productivity", f"{current['plant_productivity']:.1f}%" if current["plant_productivity"] is not None else "—")]
    for col, (label, value) in enumerate(metrics):
        table.cell(0, col).text = label; table.cell(1, col).text = str(value)
        table.cell(0, col).vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER; table.cell(1, col).vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
    add_section(document, "Executive interpretation", analysis.get("executive_summary", []))
    add_section(document, "2. Previous-period context", analysis.get("comparison", []))
    add_section(document, "3. Press and production flow", analysis.get("press_flow", []))
    add_section(document, "4. Quality and re-cook activity", analysis.get("quality", []))
    add_section(document, "5. Key strengths", analysis.get("strengths", []))
    document.add_page_break()
    add_section(document, "6. Key focus areas", analysis.get("focus_areas", []), numbered=True)
    document.add_heading("Operator performance", level=1)
    op_table = document.add_table(rows=1, cols=6); op_table.style = "Light Shading Accent 1"
    op_table.autofit = False
    column_widths_dxa = [900, 1800, 800, 1500, 1200, 4024]
    for cell, label in zip(op_table.rows[0].cells, ["Rank", "Name", "Days", "Productivity", "LF target", "Comment"]): cell.text = label
    header_properties = op_table.rows[0]._tr.get_or_add_trPr()
    header_properties.append(OxmlElement("w:tblHeader"))
    comments = analysis.get("operator_comments", {}) if isinstance(analysis.get("operator_comments"), dict) else {}
    for rank, item in enumerate(sorted(facts.get("operators", []), key=lambda value: -(value.get("target_achieved") or 0)), start=1):
        cells = op_table.add_row().cells
        values = [rank, item.get("name"), item.get("days"), f"{item.get('productivity'):.1f}%" if item.get("productivity") is not None else "—", f"{item.get('target_achieved'):.1f}%" if item.get("target_achieved") is not None else "—", comments.get(item.get("name"), "")]
        for cell, value in zip(cells, values): cell.text = str(value)
    for row in op_table.rows:
        cant_split = OxmlElement("w:cantSplit")
        row._tr.get_or_add_trPr().append(cant_split)
    table_properties = op_table._tbl.tblPr
    table_width = OxmlElement("w:tblW"); table_width.set(qn("w:type"), "dxa"); table_width.set(qn("w:w"), str(sum(column_widths_dxa))); table_properties.append(table_width)
    table_layout = OxmlElement("w:tblLayout"); table_layout.set(qn("w:type"), "fixed"); table_properties.append(table_layout)
    grid = op_table._tbl.tblGrid
    for child in list(grid): grid.remove(child)
    for width in column_widths_dxa:
        grid_column = OxmlElement("w:gridCol"); grid_column.set(qn("w:w"), str(width)); grid.append(grid_column)
    for row in op_table.rows:
        for cell, width in zip(row.cells, column_widths_dxa):
            cell_width = cell._tc.get_or_add_tcPr().get_or_add_tcW()
            cell_width.set(qn("w:type"), "dxa"); cell_width.set(qn("w:w"), str(width))
            for paragraph in cell.paragraphs:
                paragraph.paragraph_format.space_before = Pt(0)
                paragraph.paragraph_format.space_after = Pt(0)
                for run in paragraph.runs:
                    run.font.size = Pt(8.5)
    add_section(document, "Workforce observations", analysis.get("workforce_observations", []))
    document.add_heading("Final message", level=1)
    document.add_paragraph(str(analysis.get("final_message") or "The verified production figures are presented above for management review."))
    footer = section.footer.paragraphs[0]
    footer.text = "AI interpretation uses verified Production Analysis figures only; review recommendations alongside operational context."
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    footer.runs[0].italic = True
    footer.runs[0].font.size = Pt(8)
    footer.runs[0].font.color.rgb = RGBColor(91, 107, 124)
    output = BytesIO(); document.save(output); return output.getvalue()


def add_section(document, heading, content, numbered=False):
    document.add_heading(heading, level=1)
    items = content if isinstance(content, list) else [content]
    for item in items:
        text = str(item or "").strip()
        if text:
            document.add_paragraph(text, style="List Number" if numbered else "List Bullet")
