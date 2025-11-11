import math
from operator import itemgetter
from pathlib import Path
import folium
from tableauscraper import TableauScraper as TS
import pandas as pd
import numpy as np
from gtfslib import GTFS, CoordsUtil, Projections
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

def scale_color(scaled_num):
    color_hsv = (max_color - min_color) * scaled_num + min_color
    color_rgb = [round(i * 255) for i in colorsys.hsv_to_rgb(*color_hsv)]
    color_hex = f"#{color_rgb[0]:02x}{color_rgb[1]:02x}{color_rgb[2]:02x}"
    return color_hex

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

data["SUM(MEASURE_VALUE)-value"] //= 30 # monthly to daily average

filtered = data[data["SUM(MEASURE_VALUE)-value"]>0].filter(columns, axis="columns")
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

print(measure_min, measure_max)

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
    scaled_riders = math.sqrt(scaled_riders) # boost lower values
    color_hex = scale_color(scaled_riders)
    for rid in matching_route_ids:
        map_routes.append((rid, riders, { "daily_ridership": riders, "color": color_hex, "route_desc": None, "route_type": None, "route_text_color": None, "route_url": None }))

map_routes.sort(key=itemgetter(1))

print(f"Generating map into {output_file.name}")
route_map = gtfs.get_map({ route[0]: route[2] for route in map_routes })

import restapi
SERVER_URL = (
    "https://geospatial.nctcog.org/server/rest/services"
    # + "/Transportation/TrafficCounts/MapServer"
)
ags = restapi.ArcServer(SERVER_URL)
traffic_counts_service = ags.getService("TrafficCounts")
traffic_counts = traffic_counts_service.layer(0)
query = traffic_counts.query(where="Date>=Date'2021-01-01'", exceed_limit=True)

import geopandas as gpd
traffic_gdf = gpd.GeoDataFrame.from_features(query.json["features"])
traffic_gdf.set_crs(Projections.WGS84, inplace=True)

from folium import plugins

filtered_route_gdf = gtfs.routes[gtfs.routes["route_id"].isin(list(map(itemgetter(0), map_routes)))]


traffic_counts_x_routes = gpd.sjoin(
    traffic_gdf,
    CoordsUtil.buffer_points(10, filtered_route_gdf).to_crs(Projections.WGS84),
)

min_count = math.sqrt(traffic_counts_x_routes["Count24hr"].min())
max_count = math.sqrt(traffic_counts_x_routes["Count24hr"].max())

for idx, row in traffic_counts_x_routes.iterrows():
    count = row["Count24hr"]
    road = (row["Roadway"] or "Unnamed Road")
    label = row["LABEL"] or ""
    if pd.isna(count):
        continue
    logcount = math.sqrt(count)
    folium.CircleMarker(
        location=[row.geometry.y, row.geometry.x],
        radius=math.ceil((logcount - min_count) / (max_count - min_count) * 7) + 3,
        color="#0000ff",
        fill=True,
        fill_color=scale_color((math.sqrt(logcount) - math.sqrt(min_count)) / (math.sqrt(max_count) - math.sqrt(min_count))),
        fill_opacity=0.7,
        popup=folium.Popup(f"{road}<br>{label}", max_width=300),
    ).add_to(route_map)

route_map.save(str(output_file.resolve()))
