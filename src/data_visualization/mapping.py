import folium
import geojson
from pandas import DataFrame

"""
This was chosen to put as a function to produce a very specific map
"""


def generate_chloropleth_map(geo_json: str, map_data: DataFrame, cols: list = ["CIK", "Change"]) -> folium.Map:
    """
    This function builds the chloropleth style map from a geojson file, a corresponding agg file, and a column set.

    :param geo_json: A geojson style string that has the geometry filed and a link to the identifier column in map_data
    :param map_data: A dataframe that has an id column and the field to shade by
    :param cols: A list of columns to shade by
    :return:
    """
    m = folium.Map(location=[48, -102], zoom_start=3)

    folium.Choropleth(
        geo_data=geojson.loads(geo_json),
        name="choropleth",
        data=map_data,
        columns=cols,
        key_on="feature.properties.CIK",
        fill_color="YlGn",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Net Market Cap Change By City",
    ).add_to(m)

    folium.LayerControl().add_to(m)

    return m