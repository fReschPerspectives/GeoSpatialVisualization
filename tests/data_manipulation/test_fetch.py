import io
import json
import os
import pandas as pd
import unittest
from unittest.mock import patch
from bs4 import BeautifulSoup
from data_manipulation.fetch import process_wikipedia_table
from unittest.mock import patch, MagicMock
from data_manipulation.fetch import process_wikipedia_table, retrieve_sp_500, retrieve_us_city_shape_files, retrieve_ticker_data

class TestFetch(unittest.TestCase):

    @patch('requests.get')
    def test_process_wikipedia_table(self, mock_get):
        # Mock the HTML response
        mock_html = """
        <html>
        <body>
            <table class="wikitable sortable">
                <tr>
                    <th>Header1</th>
                    <th>Header2</th>
                </tr>
                <tr>
                    <td>Row1Col1</td>
                    <td>Row1Col2</td>
                </tr>
                <tr>
                    <td>Row2Col1</td>
                    <td>Row2Col2</td>
                </tr>
            </table>
        </body>
        </html>
        """
        mock_get.return_value.text = mock_html

        url = "https://en.wikipedia.org/wiki/Some_table"
        result = process_wikipedia_table(url)

        expected_result = [
            ['Header1', 'Header2'],
            ['Row1Col1', 'Row1Col2'],
            ['Row2Col1', 'Row2Col2']
        ]

        self.assertEqual(result, expected_result)

    @patch('requests.get')
    def test_process_wikipedia_table_no_table(self, mock_get):
        # Mock the HTML response with no table
        mock_html = """
        <html>
        <body>
            <p>No table here!</p>
        </body>
        </html>
        """
        mock_get.return_value.text = mock_html

        url = "https://en.wikipedia.org/wiki/No_table"
        result = process_wikipedia_table(url)

        expected_result = []

        self.assertEqual(result, expected_result)

    @patch('data_manipulation.fetch.process_wikipedia_table')
    def test_retrieve_sp_500(self, mock_process_wikipedia_table):
        # Mock the table data
        mock_table_data = [
            ['Symbol', 'Security', 'SEC filings', 'GICS Sector', 'GICS Sub Industry', 'Headquarters Location', 'Date first added', 'CIK', 'Founded'],
            ['MMM', '3M Company', 'reports', 'Industrials', 'Industrial Conglomerates', 'St. Paul, Minnesota', '1976-08-09', '0000066740', '1902'],
            ['AOS', 'A. O. Smith', 'reports', 'Industrials', 'Building Products', 'Milwaukee, Wisconsin', '2017-07-26', '0000091142', '1916']
        ]
        mock_process_wikipedia_table.return_value = mock_table_data

        result = retrieve_sp_500()

        expected_columns = ['Symbol', 'Security', 'SEC filings', 'GICS Sector', 'GICS Sub Industry', 'Headquarters Location', 'Date first added', 'CIK', 'Founded']
        expected_data = [
            ['MMM', '3M Company', 'reports', 'Industrials', 'Industrial Conglomerates', 'St. Paul, Minnesota', '1976-08-09', '0000066740', '1902'],
            ['AOS', 'A. O. Smith', 'reports', 'Industrials', 'Building Products', 'Milwaukee, Wisconsin', '2017-07-26', '0000091142', '1916']
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        pd.testing.assert_frame_equal(result, expected_df)


    @patch('data_manipulation.fetch.process_wikipedia_table')
    @patch('data_manipulation.fetch.requests.get')
    @patch('data_manipulation.fetch.zipfile.ZipFile')
    @patch('data_manipulation.fetch.shapefile.Reader')
    @patch('os.mkdir')
    def test_retrieve_us_city_shape_files(self, mock_mkdir, mock_shapefile_reader, mock_zipfile, mock_requests_get, mock_process_wikipedia_table):
        # Mock the process_wikipedia_table function
        mock_process_wikipedia_table.return_value = [
            ['State', 'Numeric code'],
            ['Alabama', '01'],
            ['Alaska', '02']
        ]

        # Mock the requests.get function
        mock_response = MagicMock()
        mock_response.content = b'Duck Duck Goose'
        mock_requests_get.return_value = mock_response

        # Mock the ZipFile object
        mock_zip = MagicMock()
        mock_zipfile.return_value = mock_zip

        # Mock the shapefile.Reader object
        mock_shape = MagicMock()
        mock_shape.__geo_interface__ = {'type': 'FeatureCollection', 'features': []}
        mock_shapefile_reader.return_value = mock_shape

        # Call the function
        result = retrieve_us_city_shape_files()

        # Check if the directory was created
        mock_mkdir.assert_called_with(f"{os.getcwd()}/tmp")

        # Check if requests.get was called with the correct URLs
        expected_urls = [
            'https://www2.census.gov/geo/tiger/TIGER2019/PLACE/tl_2019_01_place.zip',
            'https://www2.census.gov/geo/tiger/TIGER2019/PLACE/tl_2019_02_place.zip'
        ]
        actual_urls = [call[0][0] for call in mock_requests_get.call_args_list]
        self.assertEqual(expected_urls, actual_urls)

        # Check if the ZipFile was initialized with the correct content
        # Not sure why this is not working... TODO: Fix this
        # mock_zipfile.assert_called_with(io.BytesIO(b'Duck Duck Goose'))

        # Check if the shapefile.Reader was initialized with the correct path
        expected_paths = [
            f"/Users/robertresch/Developer/GeoSpatialVisualization/tmp/tl_2019_01_place.shp",
            f"/Users/robertresch/Developer/GeoSpatialVisualization/tmp/tl_2019_02_place.shp"
        ]
        actual_paths = [call[0][0] for call in mock_shapefile_reader.call_args_list]
        self.assertEqual(expected_paths, actual_paths)

        # Check the final result
        expected_result = json.dumps({
            '01': {'type': 'FeatureCollection', 'features': []},
            '02': {'type': 'FeatureCollection', 'features': []}
        })
        self.assertEqual(result, expected_result)

    @patch('data_manipulation.fetch.process_wikipedia_table')
    @patch('data_manipulation.fetch.requests.get')
    @patch('data_manipulation.fetch.zipfile.ZipFile')
    @patch('data_manipulation.fetch.shapefile.Reader')
    @patch('os.mkdir')
    def test_retrieve_us_city_shape_files_exception_handling(self, mock_mkdir, mock_shapefile_reader, mock_zipfile, mock_requests_get, mock_process_wikipedia_table):
        # Mock the process_wikipedia_table function
        mock_process_wikipedia_table.return_value = [
            ['State', 'Numeric code'],
            ['Alabama', '01'],
            ['Alaska', '02']
        ]

        # Mock the requests.get function to raise an exception
        mock_requests_get.side_effect = Exception("Network error")

        # Call the function
        result = retrieve_us_city_shape_files()

        # Check if the directory was created
        mock_mkdir.assert_called_with(f"{os.getcwd()}/tmp")

        # Check the final result
        expected_result = json.dumps({})
        self.assertEqual(result, expected_result)


    @patch('data_manipulation.fetch.yf.download')
    def test_retrieve_ticker_data(self, mock_yf_download):
        # Mock the yfinance download function
        mock_data = pd.DataFrame({
            'AAPL': {'Open': 150.0, 'High': 155.0, 'Low': 149.0, 'Close': 154.0, 'Volume': 1000000},
            'MSFT': {'Open': 250.0, 'High': 255.0, 'Low': 249.0, 'Close': 254.0, 'Volume': 2000000}
        })
        mock_yf_download.return_value = mock_data

        # Create a mock dataframe for S&P 500 data
        sp_data = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT']
        })

        # Call the function
        result = retrieve_ticker_data(sp_data)

        # Expected result
        expected_result = mock_data.to_dict()

        # Check the result
        self.assertEqual(result, expected_result)

    @patch('data_manipulation.fetch.yf.download')
    def test_retrieve_ticker_data_empty(self, mock_yf_download):
        # Mock the yfinance download function to return an empty dataframe
        mock_data = pd.DataFrame()
        mock_yf_download.return_value = mock_data

        # Create a mock dataframe for S&P 500 data
        sp_data = pd.DataFrame({
            'Symbol': []
        })

        # Call the function
        result = retrieve_ticker_data(sp_data)

        # Expected result
        expected_result = mock_data.to_dict()

        # Check the result
        self.assertEqual(result, expected_result)

    @patch('data_manipulation.fetch.yf.download')
    def test_retrieve_ticker_data_exception_handling(self, mock_yf_download):
        # Mock the yfinance download function to raise an exception
        mock_yf_download.side_effect = Exception("API error")

        # Create a mock dataframe for S&P 500 data
        sp_data = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT']
        })

        # Call the function and check if it handles the exception
        with self.assertRaises(Exception) as context:
            retrieve_ticker_data(sp_data)

        self.assertTrue("API error" in str(context.exception))

if __name__ == '__main__':
    unittest.main()