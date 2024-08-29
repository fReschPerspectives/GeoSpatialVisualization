import json
from typing import Any

import numpy as np
import pandas as pd
import geopandas as gpd
from states import states

"""
The purpose of this module is to centrally consolidate data transformation methods that will return 
dataframes or other data objects that will be used directly as inputs for visualizations.
"""


def forge_geojson(df: pd.DataFrame) -> Any:
    """
    This function serves to take a dataframe and transform it into a geojson file.

    '{
    "type": "FeatureCollection",
    "features": [
        {
            "properties": {"name": "Alabama"},
            "id": "AL",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-87.359296, 35.00118], ...]]
                }
            },
        {
            "properties": {"name": "Alaska"},
            "id": "AK",
            "type": "Feature",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[[[-131.602021, 55.117982], ... ]]]
                }
            },
        ...
        ]
    }'

    :param df: dataframe that at the very least a geometry column
    :return: the geojson representation from above
    """
    cols: list = df.columns.tolist()
    df = df[df["geometry"].notna()]

    for col in cols:
        df[f"properties_{col}"] = df[col].apply(lambda x: str(f""" "{col}" : "{x}" """))

    df['type'] = """{"type": "Feature", "properties": {"""
    df["suffix"] = """}"""
    df['constructed_json'] = df['type']
    for col in cols:
        df['constructed_json'] = df['constructed_json'].str.cat(df[f"properties_{col}"], sep=',')
    df['constructed_json'] = df['constructed_json'].apply(lambda x: x.replace('"properties": {,', '"properties": {'))
    df['constructed_json'] = df['constructed_json'].apply(lambda x: x.replace(' , "geometry"', '}, "geometry"'))
    df['constructed_json'] = df['constructed_json'].str.cat(df['suffix'], sep='')
    #some additional cleanup from quote marks
    df['constructed_json'] = df['constructed_json'].apply(lambda x: x.replace('"geometry" : "{', '"geometry" : {'))
    df['constructed_json'] = df['constructed_json'].apply(lambda x: x.replace('}" ', '}'))
    df['constructed_json'] = df['constructed_json'].apply(lambda x: x.replace('\'', '"'))

    consructed_list: list = df['constructed_json'].to_list()

    middle_string: str = str(consructed_list)
    # fix the middle part of the string from the list nonsense
    middle_string.replace("""[(\'""", """""").replace('''"]\',)''', '''''').replace("\\\\","").replace("""[\'""", """""").replace("""\']""", """""")
    # fix
    prefix: str = """{"type": "FeatureCollection", "features": ["""
    suffix: str = """]}"""

    return f"""{prefix}{middle_string}{suffix}""".replace("""[\'""", """""").replace("""\']""", """""").replace("""\', \'""", ""","""), df[cols]


def join_ticker_data_to_geodata(sp_companies: pd.DataFrame, ticker_data: dict, geo_data: str):
    """
    This function serves to deserializes the json style string of the geographic data and to combine it with the
    s&p 500 company list by Headquarters City, combining it with the relevant market information

    :param sp_companies: Dataframe of S&P Company data, must include Symbol and Headquarter information
    :param ticker_data: Yahoo finance dataframe that has close data for all the relevant symbols
    :param geo_data: jsonlike string of all the city polygon information
    :return: a geojson object that can be used to create a map from
    """

    # First step is to sp_company headquarter information into city name and state, then derive fips code
    # The following command will split the string on a comma-- if a country is present, a third column is populated
    sp_cols = sp_companies.columns.tolist()
    sp_companies = pd.concat([sp_companies, sp_companies["Headquarters Location"].str.split(', ', expand=True)], axis=1)
    if len(sp_companies.columns) == 10:
        new_cols: list = ["City Name", "State"]
    elif len(sp_companies.columns) == 11:
        new_cols: list = ["City Name", "State", "Country"]
    # Appending the new cols to the original column list and reassign to the expanded dataframe
    [sp_cols.append(col) for col in new_cols]
    sp_companies.columns = sp_cols

    # Given we have a state name now, must go and find its fips code
    state_info: pd.DataFrame = states.get_states_df()

    # Now simply join sp company data to the state on State name
    prepped_sp_data: pd.DataFrame = pd.merge(sp_companies, state_info, left_on="State",  right_on="name", how="left")
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "New York" if x == "New York City" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Minneapolis" if x == "Saint Paul" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Indianapolis city (balance)" if x == "Indianapolis" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Sterling" if x == "Dulles" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Tysons" if x == "Tysons Corner" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Wallingford Center" if x == "Wallingford" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Hartford" if x == "Bloomfield" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Hartford" if x == "Farmington" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Harrison" if x == "Purchase" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Rochester" if x == "Penfield" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Trenton" if x == "Ewing" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Hackensack" if x == "Teaneck" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Morristown" if x == "Parsippany" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Beaverton" if x == "Washington County" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Boise City" if x == "Boise" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Nashville-Davidson metropolitan government (balance)" if x == "Nashville" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Reading" if x == "North Reading" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Boston" if x == "Acton" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Mayfield Heights" if x == "Mayfield Village" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Baltimore" if x == "Hunt Valley" else x)
    prepped_sp_data["City Name"] = prepped_sp_data["City Name"].apply(lambda x: "Philadelphia" if x == "Wayne" else x)

    # geo_data is a string resulting from a json.dumps() call, needs to be deserialized into something useful
    geo_data_deserialized = json.loads(geo_data)

    # Only need to iterate over states present in the sp_company data
    fips_iterator = prepped_sp_data[(prepped_sp_data["fips"].notnull())]["fips"].unique()
    city_collection: list = []

    for fips in fips_iterator:
        state_sp_data = prepped_sp_data[prepped_sp_data["fips"] == fips]
        cities_of_interest = state_sp_data["City Name"].unique()

        # the outermost key is the state fips code as a str; i.e. Colorado is fips code "08"
        # each state, then is a dictionary of "type", "features", and "bbox"
        # features is a list of dictionaries, traversed to obtain the city name ('NAME') & geometry ('geometry')
        # geometry itself is a dictionary of 'type' and 'coordinates'
        # return only the relevant cities geospatial encoding so that it can be added to the sp market data
        state_geo_data = geo_data_deserialized.get(fips)
        city_geo_data = state_geo_data.get(f"features")
        city_data = [ {"state_fips":fips, "city" : city.get("properties").get("NAME"), "geometry" : city.get("geometry")} for city in city_geo_data if city.get("properties").get("NAME") in cities_of_interest ]
        for city in city_data:
            city_collection.append(city)

    city_data_df: pd.DataFrame = pd.DataFrame(city_collection)
    state_sp_data_with_geo: pd.DataFrame = pd.merge(prepped_sp_data, city_data_df, how="left", left_on=["fips", "City Name"], right_on=["state_fips", "city"])

    # Print some checks after grabbing the geolocation
    print(f"""The number of securities with a US geometry is: {len(state_sp_data_with_geo[state_sp_data_with_geo["state_fips"].notnull()]["Security"])}""")
    print(f"""The number of securities in the US is: {len(state_sp_data_with_geo[state_sp_data_with_geo["name"].notnull()]["Security"])}""")
    print(f"""This indicates that {len(state_sp_data_with_geo[state_sp_data_with_geo["name"].notnull()]["Security"]) -len(state_sp_data_with_geo[state_sp_data_with_geo["state_fips"].notnull()]["Security"])} companies failed to be geoenhanced""")

    # With all the S&P data now enriched with geolocation data, time to aggregate up to the city level, figuring out which cities had the biggest change in value over the day
    # ticker data will have a terrible structure due to the mult-indexing ('<Metric>', '<Ticker>') : {Timestamp('<date> 00:00:00'): <value>}
    ticker_keys: list = list(ticker_data)
    open_keys: list = [key for key in ticker_keys if str(key).__contains__('Open')]
    close_keys: list = [key for key in ticker_keys if str(key).__contains__('Adj Close')]
    volume_keys: list = [key for key in ticker_keys if str(key).__contains__('Volume')]

    symbols: list = prepped_sp_data["Symbol"].tolist()
    change_by_symbol: list = []

    for symbol in symbols:
        open_value_key = [o for o in open_keys if o[1] == symbol][0]
        close_value_key = [c for c in close_keys if c[1] == symbol][0]
        volume_value_key = [v for v in volume_keys if v[1] == symbol][0]

        open_value: float = list(ticker_data.get(open_value_key).values())[0]
        close_value: float = list(ticker_data.get(close_value_key).values())[0]
        volume_value: float = list(ticker_data.get(volume_value_key).values())[0]
        change_value: float = close_value - open_value
        agg_change: float = change_value * volume_value

        change_record: dict = {"Symbol": symbol, "Open": open_value, "Close": close_value, "Volume": volume_value, "Change": agg_change}
        change_by_symbol.append(change_record)

    change_by_symbol_df: pd.DataFrame = pd.DataFrame(change_by_symbol)

    # combine the daily change by symbol to the prepped sp dataframe
    pre_agg_df: pd.DataFrame = state_sp_data_with_geo.merge(change_by_symbol_df, how="left", on="Symbol")
    pre_agg_df: pd.DataFrame = pre_agg_df.drop_duplicates(keep="first", subset=['Symbol', 'Security', 'City Name', 'State'])

    # now this data frame can be aggregated by city, give an id to the city, setting up the final geojson prep to output
    agg_df: pd.DataFrame = pre_agg_df[['Headquarters Location', 'City Name', 'State', 'Change', 'CIK']].groupby(['Headquarters Location', 'City Name', 'State']).agg('sum').reset_index()

    # go back ang get the geometry column and necessary joining key
    geo_df: pd.DataFrame = pre_agg_df[['Headquarters Location', 'City Name', 'State', "geometry"]]

    final_df: pd.DataFrame = agg_df.merge(geo_df, how="left", on=['Headquarters Location', 'City Name', 'State'])

    # this final df can then be converted to the necessary geoJSON structure
    map_geometry, map_data = forge_geojson(final_df)

    return map_geometry, map_data.drop("geometry", axis=1).drop_duplicates()
