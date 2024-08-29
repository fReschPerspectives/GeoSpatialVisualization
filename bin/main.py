from data_manipulation import fetch
from data_manipulation import transform
from data_visualization import mapping

import logging
import os

# create logger
logger = logging.getLogger('market_change')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

if __name__ == "__main__":
    sp_data = fetch.retrieve_sp_500()
    logger.info("Successfully fetched sp data")

    sp_market_data = fetch.retrieve_ticker_data(sp_data)
    logger.info("Successfully fetched ticker data")

    geo_data = fetch.retrieve_us_city_shape_files()
    logger.info("Successfully fetched us_city_shape_files")

    map_ready_geo_data, map_data = transform.join_ticker_data_to_geodata(sp_data, sp_market_data, geo_data)
    logger.info("Successfully joined us_city_shape_files, market data")

    m = mapping.generate_chloropleth_map(map_ready_geo_data, map_data)
    logger.info("Successfully generated chloropleth map")

    try:
        os.mkdir(f"{os.environ['HOME']}/Documents/maps")
    except FileExistsError:
        pass
    m.save(f"{os.environ['HOME']}/Documents/maps/chloropleth_map.html")