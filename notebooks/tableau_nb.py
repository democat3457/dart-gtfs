# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: 3.10.13
#     language: python
#     name: python3
# ---

# %%
from tableauscraper import TableauScraper as TS
import pandas as pd
import numpy as np

# %%
# url = "https://tableau.dart.org/t/Public/views/DARTscorecard/DARTScorecard"
url = "https://tableau.dart.org/t/Public/views/DARTscorecard/RidershipPerformance"
subsheetName = "by Route for DART Bus Service"

ts = TS()
ts.loads(url)

# %%
ridership_filters = {
    "total": ("Total Ridership Measure Values", "TOTAL_RIDERSHIP"),
    "weekday": ("Weekday Avg Measure Values", "AVG_WKDAY_RIDERSHIP"),
    "saturday": ("Saturday Avg Measure Values", "AVG_SAT_RIDERSHIP"),
    "sunday": ("Sunday Avg Measure Values", "AVG_SUN_RIDERSHIP")
}
RIDERSHIP_FILTER = ridership_filters["weekday"]

# %%
workbook = ts.getWorkbook()
dashboard = workbook.getWorksheet(RIDERSHIP_FILTER[0]).select("MEASURE_CODE", RIDERSHIP_FILTER[1])
serviceByRoute = dashboard.getWorksheet(subsheetName)

# %%
workbook.getSheets()

# %%
# ds = dashboard.getWorksheet("Weekday Avg Measure Values").select("MEASURE_CODE", "AVG_WKDAY_RIDERSHIP")

for ws_name in dashboard.getWorksheetNames():
    print(ws_name, dashboard.getWorksheet(ws_name).getSelectableItems())

# %%
for n in dashboard.worksheets:
    print(n.name)
    print(n.data)
    print()
