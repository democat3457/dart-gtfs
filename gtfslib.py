# DART GTFS data: https://www.dart.org/about/about-dart/fixed-route-schedule
# https://www.dart.org/transitdata/latest/google_transit.zip

from collections import defaultdict
from pathlib import Path
import gtfs_kit as gk
import numpy as np
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import folium
import geopandas as gpd
from shapely.geometry import Point

class Projections:
    WGS84 = 'EPSG:4326'
    GMAPS = 'EPSG:3857'
    DALLAS = 'EPSG:6583'

class CoordsUtil:
    @staticmethod
    def buffer_points(distance_meters: float, *lat_lon_list: tuple[float, float]) -> gpd.GeoDataFrame:
        gdf = gpd.GeoDataFrame(
            {"geometry": [ Point(lon, lat) for lat, lon in lat_lon_list ]}, crs=Projections.WGS84
        )
        proj_geoseries = gdf.to_crs(Projections.DALLAS).buffer(distance_meters).to_crs(Projections.WGS84)
        return gpd.GeoDataFrame(geometry=proj_geoseries)

    @staticmethod
    def coord_distance(lat_lon_1: tuple[float, float], lat_lon_2: tuple[float, float]) -> float:
        points_df = gpd.GeoDataFrame(
            {"geometry": [ Point(lon, lat) for lat, lon in (lat_lon_1, lat_lon_2) ]}, crs=Projections.WGS84
        )
        points_df = points_df.to_crs(Projections.DALLAS)
        points_df2 = points_df.shift()  # We shift the dataframe by 1 to align pnt1 with pnt2
        return points_df.distance(points_df2).iloc[1]

class GTFS:
    def __init__(self, gtfs_file: Path):
        self._feed = gk.read_feed(gtfs_file, dist_units="mi")
        self._merged_trips_and_stoptimes: DataFrameGroupBy[tuple, True] | None = None
        self._trip_activities_by_dates: dict[tuple[str], pd.DataFrame] = dict()

    @property
    def feed(self):
        return self._feed

    @property
    def routes(self) -> pd.DataFrame:
        return self._feed.routes

    def get_description_value(self, key: str) -> str:
        descrip = self._feed.describe()
        return descrip[descrip["indicator"] == key]["value"].iat[0]

    def get_stop(self, stop_id: str | int) -> pd.Series:
        return self._feed.stops[self._feed.stops["stop_id"] == str(stop_id)].iloc[0]

    def get_map(self, route_ids:list[str]=None, color_palette:list[str]=None) -> folium.Map:
        if route_ids is None:
            route_ids = self.routes.route_id.loc[:]
        kwargs = dict()
        if color_palette is not None:
            kwargs["color_palette"] = color_palette
        return self._feed.map_routes(route_ids, show_stops=False, **kwargs)

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
        dates = self._feed.subset_dates(dates)
        if not dates:
            return pd.DataFrame()

        if self._merged_trips_and_stoptimes is None:
            merged = pd.merge(
                self._feed.trips, self._feed.stop_times
            )
            self._merged_trips_and_stoptimes = merged.groupby(["stop_id"], sort=False)
        t = self._merged_trips_and_stoptimes.get_group((stop_id,))

        tuple_dates = tuple(dates)
        if tuple_dates not in self._trip_activities_by_dates:
            self._trip_activities_by_dates[tuple_dates] = self._feed.compute_trip_activity(dates)
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
