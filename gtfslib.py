# DART GTFS data: https://www.dart.org/about/about-dart/fixed-route-schedule
# https://www.dart.org/transitdata/latest/google_transit.zip

from pathlib import Path
import gtfs_kit as gk
import pandas as pd
import folium

class GTFS:
    def __init__(self, gtfs_file: Path):
        self._feed = gk.read_feed(gtfs_file, dist_units="mi")

    @property
    def feed(self):
        return self._feed

    @property
    def routes(self) -> pd.DataFrame:
        return self._feed.routes
    
    def get_description_value(self, key: str) -> str:
        descrip = self._feed.describe()
        return descrip[descrip["indicator"] == key]["value"].iat[0]

    def get_map(self, route_ids:list[str]=None, color_palette:list[str]=None) -> folium.Map:
        if route_ids is None:
            route_ids = self.routes.route_id.loc[:]
        kwargs = dict()
        if color_palette is not None:
            kwargs["color_palette"] = color_palette
        return self._feed.map_routes(route_ids, show_stops=False, **kwargs)
