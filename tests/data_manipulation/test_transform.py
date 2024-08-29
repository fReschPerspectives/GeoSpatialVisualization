import unittest
from unittest.mock import patch
import pandas as pd
import json
from data_manipulation.transform import join_ticker_data_to_geodata

class TestTransform(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.sp_companies = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT'],
            'Security': ['Apple Inc.', 'Microsoft Corp.'],           
            'GICS Sector': ['Information Technology', 'Information Technology'],
            'GICS Sub-Industry': ['Technology Hardware, Storage & Peripherals', 'Systems Software'],
            'Headquarters Location': ['Cupertino, California', 'Redmond, Washington'],
            'Date Added': ['1982-11-30', '1991-03-31'],
            'CIK': ['0000320193', '0000789019'],
            'Founded': ['1977', '1975']
        })

        self.ticker_data = {
            ('Open', 'AAPL'): {pd.Timestamp('2023-01-01'): 150.0},
            ('Adj Close', 'AAPL'): {pd.Timestamp('2023-01-01'): 155.0},
            ('Volume', 'AAPL'): {pd.Timestamp('2023-01-01'): 1000000},
            ('Open', 'MSFT'): {pd.Timestamp('2023-01-01'): 250.0},
            ('Adj Close', 'MSFT'): {pd.Timestamp('2023-01-01'): 255.0},
            ('Volume', 'MSFT'): {pd.Timestamp('2023-01-01'): 2000000}
        }

        self.geo_data = json.dumps({
            "06": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"NAME": "Cupertino"},
                        "geometry": {"type": "Point", "coordinates": [-122.0322, 37.3229]}
                    }
                ]
            },
            "53": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"NAME": "Redmond"},
                        "geometry": {"type": "Point", "coordinates": [-122.1215, 47.6740]}
                    }
                ]
            }
        })

    def test_join_ticker_data_to_geodata(self):
        map_geometry, map_data = join_ticker_data_to_geodata(self.sp_companies, self.ticker_data, self.geo_data)

        # Check if the returned map_geometry is not empty
        self.assertTrue(map_geometry)

        # Check if the map_data DataFrame is not empty
        self.assertFalse(map_data.empty)

        # Check if the map_data DataFrame contains the expected columns
        expected_columns = ['Headquarters Location', 'City Name', 'State', 'Change', 'CIK']
        self.assertTrue(all(column in map_data.columns for column in expected_columns))

        # Check if the map_data DataFrame contains the expected number of rows
        self.assertEqual(len(map_data), 2)

if __name__ == '__main__':
    unittest.main()