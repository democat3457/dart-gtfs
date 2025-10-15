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
url = "https://tableau.dart.org/t/Public/views/DARTscorecard/DARTScorecard"
sheetName = "Ridership label KPI"
subsheetName = "by Route for DART Bus Service"

ts = TS()
ts.loads(url)

workbook = ts.getWorkbook()
ridershipSheet = ts.getWorksheet(sheetName)
dashboard = ridershipSheet.select("Ridership label", "Ridership Performance")

serviceByRoute = dashboard.getWorksheet(subsheetName)

# %%
for n in dashboard.worksheets:
    print(n.name)
    print(n.data)
    print()
