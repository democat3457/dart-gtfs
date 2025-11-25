# DART GTFS data: https://www.dart.org/about/about-dart/fixed-route-schedule
# https://www.dart.org/transitdata/latest/google_transit.zip

from collections import defaultdict
from datetime import datetime
from enum import Enum
import functools
from pathlib import Path
import random
from typing import Optional
import gtfs_kit as gk
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import folium
import geopandas as gpd
import shapely.geometry as sg
import shapely.ops as so

class RouteType(Enum):
    LIGHT_RAIL = 0
    SUBWAY = 1
    RAIL = 2
    BUS = 3
    FERRY = 4
    CABLE_TRAM = 5
    AERIAL_LIFT = 6
    FUNICULAR = 7
    TROLLEY = 11
    MONORAIL = 12

    @classmethod
    def all(cls):
        return list(cls)

class Projections:
    WGS84 = 'EPSG:4326'
    GMAPS = 'EPSG:3857'

class CoordsUtil:
    @staticmethod
    def _to_projected_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if not gdf.crs.is_projected:
            gdf = gdf.to_crs(gdf.estimate_utm_crs())
        return gdf

    @staticmethod
    def buffer_points(distance_meters: float, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        old_crs = None
        gdf = CoordsUtil._to_projected_crs(gdf)
        proj_geoseries: gpd.GeoSeries = gdf.buffer(distance_meters)
        if old_crs:
            proj_geoseries = proj_geoseries.to_crs(old_crs)
        return gpd.GeoDataFrame(geometry=proj_geoseries)

    @staticmethod
    def coord_distance(gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame) -> float:
        gdf1 = CoordsUtil._to_projected_crs(gdf1)
        gdf2 = CoordsUtil._to_projected_crs(gdf2)
        return gdf1.distance(gdf2, align=False).iloc[0]

class GTFS:
    def __init__(self, gtfs_file: Path):
        self._feed = gk.read_feed(gtfs_file, dist_units="mi")
        self._feed_info: pd.Series = self.feed.feed_info.loc[0]
        self._georoutes = self.feed.get_routes(as_gdf=True, use_utm=True)
        self._geostops = self.feed.get_stops(as_gdf=True, use_utm=True)
        self._merged_trips_and_stoptimes: DataFrameGroupBy[tuple, True] | None = None
        self._trip_activities_by_dates: dict[tuple[str], pd.DataFrame] = dict()
        self._stops_by_id: gpd.GeoDataFrame = self.stops.set_index("stop_id", drop=False)
        self._routes_by_id: gpd.GeoDataFrame = self.routes.set_index("route_id", drop=False)
        self._trips_by_id: pd.DataFrame = self.feed.trips.set_index("trip_id", drop=False)

    @property
    def feed(self):
        return self._feed

    @property
    def feed_info(self):
        return self._feed_info

    @functools.cached_property
    def start_date(self):
        return datetime.strptime(self.feed_info["feed_start_date"], "%Y%m%d").date()

    @functools.cached_property
    def end_date(self):
        return datetime.strptime(self.feed_info["feed_end_date"], "%Y%m%d").date()

    @property
    def routes(self) -> gpd.GeoDataFrame:
        return self._georoutes

    @property
    def stops(self) -> gpd.GeoDataFrame:
        return self._geostops

    @functools.cached_property
    def route_to_type(self) -> dict[str, RouteType]:
        return {
            r_id: RouteType(r_type)
            for r_id, r_type in self._routes_by_id["route_type"].T.to_dict().items()
        }

    @functools.cached_property
    def trip_to_route(self) -> dict[str, str]:
        return {
            t_id: r_id
            for t_id, r_id in self._trips_by_id["route_id"].T.to_dict().items()
        }

    @functools.cached_property
    def stop_routes(self) -> dict[str, set[str]]:
        dct = defaultdict(set)
        for _, row in self.feed.stop_times.iterrows():
            dct[row["stop_id"]].add(
                str(self.trip_to_route[row["trip_id"]])
            )
        return dct
    
    @functools.cached_property
    def route_stops(self) -> dict[str, set[str]]:
        dct = defaultdict(set)
        for stop_id, route_ids in self.stop_routes.items():
            for route_id in route_ids:
                dct[route_id].add(stop_id)
        return dct

    @functools.cached_property
    def stop_names(self) -> dict[str, str]:
        dct = dict()
        for _, row in self.stops.iterrows():
            dct[row["stop_id"]] = row["stop_name"]
        return dct

    def get_stop(self, stop_id: str | int) -> gpd.GeoDataFrame:
        df = self._stops_by_id.loc[[str(stop_id)]].copy()
        df.index.set_names('', inplace=True)
        return df

    def get_map(self, route_ids: Optional[dict[str, dict]] = None, show_stops: bool = False) -> folium.Map:
        """
        Return a Folium map showing the given routes and (optionally) their stops.
        If ``route_ids`` is not given, add all routes to the map.
        If any of the given route IDs are not found in the feed, then raise a ValueError.
        
        Adapted from gtfs_kit
        """
        if route_ids is None:
            route_ids = { r_id: {} for r_id in self.routes.route_id.loc[:]}

        # Compile route IDs
        route_id_list = sorted(route_ids.items())
        if not len(route_id_list):
            raise ValueError("Route IDs or route short names must be given")

        # Initialize map
        my_map = folium.Map(tiles=folium.TileLayer("cartodbpositron", name="Carto DB Positron"), prefer_canvas=True)

        # Collect route bounding boxes to set map zoom later
        bboxes = []

        route_group = folium.FeatureGroup(name="Routes")
        # Create a feature group for each route and add it to the map
        for i, (route_id, props) in enumerate(route_id_list):
            collection = self.feed.routes_to_geojson(
                route_ids=[route_id], include_stops=show_stops
            )

            # Use route short name for group name if possible; otherwise use route ID
            route_name = route_id
            for f in collection["features"]:
                if "route_short_name" in f["properties"]:
                    route_name = f["properties"]["route_short_name"]
                    break

            group = folium.FeatureGroup(name=f"Route {route_name}")
            color = (
                props["color"]
                if "color" in props
                else "#%06x" % random.randint(0, 0xFFFFFF)
            )

            for f in collection["features"]:
                prop = f["properties"]
                prop.update(props)
                prop = { k: v for k, v in prop.items() if v is not None }

                # Add stop
                if f["geometry"]["type"] == "Point":
                    lon, lat = f["geometry"]["coordinates"]
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=8,
                        fill=True,
                        color=color,
                        weight=1,
                        popup=folium.Popup(gk.helpers.make_html(prop)),
                    ).add_to(group)

                # Add path
                else:
                    prop["color"] = color
                    path = folium.GeoJson(
                        f,
                        name=prop["route_short_name"],
                        style_function=lambda x: {"color": x["properties"]["color"]},
                    )
                    path.add_child(folium.Popup(gk.helpers.make_html(prop)))
                    path.add_to(group)
                    bboxes.append(sg.box(*sg.shape(f["geometry"]).bounds))

            group.add_to(route_group)

        route_group.add_to(my_map)

        # Fit map to bounds
        bounds = so.unary_union(bboxes).bounds
        bounds2 = [bounds[1::-1], bounds[3:1:-1]]  # Folium expects this ordering
        my_map.fit_bounds(bounds2)

        return my_map

    def get_stops_in_area(self, area: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Return the subset of ``feed.stops`` that contains all stops that lie
        within the given GeoDataFrame of polygons.
        
        Adapted from the gtfs_kit.Feed.get_stops_in_area method to reduce amount of
        CRS changes performed.
        """
        if self.stops.crs != area.crs:
            area = area.to_crs(self.stops.crs)
        return self.stops.merge(
            gpd.sjoin(self.stops, area)
            .filter(["stop_id"])
        )

    def build_stop_timetable(self, stop_id: str, dates: list[str]) -> pd.DataFrame:
        """
        Return a DataFrame containing the timetable for the given stop ID
        and dates (YYYYMMDD date strings)

        Return a DataFrame whose columns are all those in ``feed.trips`` plus those in
        ``feed.stop_times`` plus ``'date'``, and the stop IDs are restricted to the given
        stop ID.
        The result is sorted by date then departure time.
        
        Adapted from the gtfs_kit.Feed.build_stop_timetable method to use caching of key
        variables and optimize fetching of stops by ID.
        """
        dates = self.feed.subset_dates(dates)
        if not dates:
            return pd.DataFrame()

        if self._merged_trips_and_stoptimes is None:
            merged = pd.merge(
                self.feed.trips, self.feed.stop_times
            )
            self._merged_trips_and_stoptimes = merged.groupby(["stop_id"], sort=False)
        t = self._merged_trips_and_stoptimes.get_group((stop_id,))

        tuple_dates = tuple(dates)
        if tuple_dates not in self._trip_activities_by_dates:
            self._trip_activities_by_dates[tuple_dates] = self.feed.compute_trip_activity(dates)
        a = self._trip_activities_by_dates[tuple_dates]

        frames = []
        for date in dates:
            # Slice to stops active on date
            ids = a.loc[a[date] == 1, "trip_id"]
            f = t[t["trip_id"].isin(ids)].copy()
            f["date"] = date
            frames.append(f)

        f = pd.concat(frames)
        return f.sort_values(["date", "departure_time"])
