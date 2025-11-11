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
import restapi

# %%
SERVER_URL = (
    "https://geospatial.nctcog.org/server/rest/services"
    # + "/Transportation/TrafficCounts/MapServer"
)
ags = restapi.ArcServer(SERVER_URL)

# %%
traffic_counts_service = ags.getService("TrafficCounts")

# %%
traffic_counts = traffic_counts_service.layer(0)
query = traffic_counts.query(where="Date>=Date'2024-01-01'", exceed_limit=True)

# %%
import geopandas as gpd
traffic_gdf = gpd.GeoDataFrame.from_features(query.json["features"])

# from folium import plugins
# traffic_counts_x_routes = gpd.sjoin(
#     traffic_gdf,)

# %%
traffic_gdf.columns, traffic_gdf.loc[0]

# %%
query.count, query[0]
