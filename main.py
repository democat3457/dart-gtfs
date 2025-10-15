import math
from operator import itemgetter
from pathlib import Path
from tableauscraper import TableauScraper as TS
import pandas as pd
import numpy as np
from gtfslib import GTFS
import re
import colorsys

url = "https://tableau.dart.org/t/Public/views/DARTscorecard/DARTScorecard"
sheetName = "Ridership label KPI"
subsheetName = "by Route for DART Bus Service"

data_folder = Path("data")
export_folder = Path("export")
output_file = Path("map.html")

min_color = np.array((0, 1.0, 1.0))
max_color = np.array((0.333, 1.0, 0.8))

data_folder.mkdir(parents=True, exist_ok=True)
export_folder.mkdir(parents=True, exist_ok=True)

print("Loading tableau...")
ts = TS()
ts.loads(url)

workbook = ts.getWorkbook()
ridershipSheet = ts.getWorksheet(sheetName)
dashboard = ridershipSheet.select("Ridership label", "Ridership Performance")

serviceByRoute = dashboard.getWorksheet(subsheetName)
data: pd.DataFrame = serviceByRoute.data
data.to_csv(export_folder / "tableau.csv")

columns = [
    "Route-value",
    "MEASURE_CODE-value",
    "SERVICE_CATEGORY-value",
    "SUM(MEASURE_VALUE)-value",
]

filtered = data.filter(columns, axis="columns")
def get_route_name(name):
    m = re.search(r"\((.*)\)", name)
    if m:
        return m.group(1)
    if name == "TI":
        return "TI SHUTTLE"
    if name == "UTS":
        return "UT SOUTHWESTERN"

    print("unable to find route with name", name)
    return name

measure_values = filtered["SUM(MEASURE_VALUE)-value"]
measure_min = np.min(measure_values)
measure_max = np.max(measure_values)


print("Loading GTFS...")
gtfs = GTFS(data_folder / "dart_gtfs.zip")
routes = gtfs.routes
routes.to_csv(export_folder / "gtfs_routes.csv")

map_routes = []
for index, row in filtered.iterrows():
    route_name = get_route_name(row['Route-value'])
    matching_routes = routes[routes["route_long_name"] == route_name]
    if len(matching_routes) == 0:
        matching_routes = routes[routes["route_long_name"].str.contains(route_name)]
    matching_route_ids = matching_routes["route_id"]
    riders = int(row["SUM(MEASURE_VALUE)-value"])
    scaled_riders = (riders - measure_min) / (measure_max - measure_min) # between 0-1
    # scaled_riders = math.sqrt(scaled_riders) # boost lower values
    color_hsv = (max_color - min_color) * scaled_riders + min_color
    color_rgb = [ round(i*255) for i in colorsys.hsv_to_rgb(*color_hsv) ]
    color_hex = f"#{color_rgb[0]:02x}{color_rgb[1]:02x}{color_rgb[2]:02x}"
    for rid in matching_route_ids:
        map_routes.append((rid, color_hex, riders))

map_routes.sort(key=itemgetter(2))

print(f"Generating map into {output_file.name}")
map = gtfs.get_map(list(map(itemgetter(0), map_routes)), list(map(itemgetter(1), map_routes)))
# TODO add ridership numbers to map
map.save(str(output_file.resolve()))
