import io
import json
import os
import pandas as pd
import pandas_datareader as web
import requests
import shapefile
import yfinance as yf
import zipfile
from bs4 import BeautifulSoup
from requests import Response
from zipfile import ZipFile

"""
The purpose of this module is to centrally consolidate data getter methods that will return 
a source of data for data transformation methods.
"""


def process_wikipedia_table(url: str) -> list:
    """
    This function works for getting data from wikipedia tables that are of the type wikitable sortable.

    :param url: a wikipedia url to go scrub a table from
    :return: a list of lists of the displayed wikipedia table
    """

    res: str = requests.get(url).text
    soup: BeautifulSoup = BeautifulSoup(res,'lxml')
    table: BeautifulSoup.Tag = soup.find("table", class_="wikitable sortable")
    table_as_list_of_lists: list = []
    try:
        for items in table.find_all("tr")[:]:
            data: list = [' '.join(item.text.split()) for item in items.find_all(['th', 'td'])]
            table_as_list_of_lists.append(data)
    except AttributeError:
        pass
    return table_as_list_of_lists


def retrieve_sp_500() -> pd.DataFrame:
    """
    This is a simple getter function that scrapes Wikipedia for the contents of the table that represent 
    the stocks of the 500 largest companies that compose the S&P 500. Note that it may contain MORE than
    500 entries since there can be multiple classes of stocks of any given company. At the time of 
    composition (2024-08-15), this table was 503 records long
    

    :return: A pandas dataframe containing companies from SP 500.
    """

    url: str = f"https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table_as_list_of_lists: list = process_wikipedia_table(url = url)
    return pd.DataFrame(table_as_list_of_lists[1:], columns=table_as_list_of_lists[0])


def retrieve_us_city_shape_files() -> str:
    """
    This function will fetch the zip files from the US census records on boundaries of US cities 
    (https://www2.census.gov/geo/tiger/TIGER2019/PLACE/). The zip files are by STATE_FIPS code
    (two-digit code following the year 2019 in the file names). The intended output of this retrieval
    will be combined with S&P 500 company headquarters information to ultimately perform visualization
    on the largest market cap by city in the US. State FIPS codes can be found here:
    https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code .
    
    :return: A json string representation of US City boundaries represented as polygons on Earth's surface
    """

    wiki_url: str = f"https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code"
    state_fips_codes: list = process_wikipedia_table(url = wiki_url)
    state_fips_df: pd.DataFrame = pd.DataFrame(state_fips_codes[1:], columns=state_fips_codes[0])
    state_fips_iterator: list = state_fips_df["Numeric code"]
    geojson_dict: dict = dict()

    try:
        os.mkdir(f"{os.getcwd()}/tmp")
    except FileExistsError:
        pass

    for fip in state_fips_iterator:
        try:
            url_base: str = f"https://www2.census.gov/geo/tiger/TIGER2019/PLACE/"
            file_name_structure: str = f"tl_2019_{fip}_place.zip"
            zip_file_url: str = url_base + file_name_structure
            print(f"Retrieving {fip} from {zip_file_url}")
            r: Response = requests.get(zip_file_url)
            z: ZipFile = zipfile.ZipFile(io.BytesIO(r.content))
            z.extract(file_name_structure.replace(".zip", ".shp"), path = f"""{os.getcwd()}/tmp/""")
            z.extract(file_name_structure.replace(".zip", ".dbf"), path=f"""{os.getcwd()}/tmp/""")
            geometry: shapefile.Reader = shapefile.Reader(f"""{os.getcwd()}/tmp/{file_name_structure.replace(".zip", ".shp")}""")
            ## geometry.records() has the link between the shape # and the Name of the city and interpolated lat longs
            ## geometry.shapes() has the polygons we will need, the __geo_interface__ will make this conveniently geojson for plotting
            geojson_data = geometry.__geo_interface__
            geojson_dict[fip] = geojson_data
        except Exception as e:
            print(f"Exception: {e}")
            pass

    return json.dumps(geojson_dict)


def retrieve_ticker_data(sp_data: pd.DataFrame) -> dict:
    """
    This function serves to a getter to pull stock data for a relevant list of S&P 500 companies

    :param sp_data: A dataframe of stock tickers
    :return: Pandas dataframe representation of ticker data from
    """

    symbols = sp_data["Symbol"].unique()
    symbols_as_string = symbols.__str__().replace("\n", "").replace("' '", " ").replace("']", "").replace("['", "")

    data = yf.download(symbols_as_string, period="1d")

    return data.to_dict()