"""
Unit test for the process module.
"""

import dotenv
dotenv.load_dotenv('.env')
import unittest
import logging
import process


#collaboration_space_id="ekmdkbfa"

#default_settings.load_settings(".env")


class Test(unittest.TestCase):
    """
    collection of test related to event processing

    """

    def test_data_quality_check(self):
        """
        Try the process to check data quality
        """
        test_event = {
            'type': 'CHECK_DATA_QUALITY'
        }

        process.event_processor(test_event)
    
#     def test_scoring(self):
#         """
#         Try the process to calculate score for one IBAN - not encrypted
#         """
#         test_event = {
#         "type": "SCORE",
#         "accounts": [
#             "cM8WNktMbOgrDiAjma9JIyRYgple1inKhvEjv8EgsUwTXVYLWSaPmr1xMy8RH5cj80T8anOlNjXZmYC2uSGTtTUYGh+Kq0ZuJPsqniiUW6N+KOZ7/h1D2KDeSAxXw3VFQmjQCD+qq82vGYXphMPmrTqVky/P+uB/WGy0/RHhKCPBzyDnaIgSQojwxfUryUXQjrkiQYES7/2GZhvvwKLulSjJJnlQYt/0XWMb2Tr712HcpcPuJs15ZjkxJLMlrNSoYud3eSneQOQEitcXanwY/gyaOHJpArj3kSfQKB+TIGI6rYunnfn9Whz58wgFCwaMdTKba0gzBWpNbrYZawzi8A==",
#             "xSEEz7qrxfKen9xUYfeLWErIbTkSW1fcYXflZDVsMEDYmCdVAa1fpBO9f6hfpTxE11IsIlTSxo1CJ/wLiu83HIM7/Be+lY45vkgMKp0hUpEo1BaXssPDiTPq6taQGKE1rmOLe19S+Iq5rGW+gPA7LFl9tL/7IqgdmFW2yZu9IpBW0Cq/5D4/GGzeFXlw7wqKJojDiaHbB8hy3mIE5Vay4QxlJpJZoyACDhDHnXBGihtRxFfv/3kgUY2K/CCpSEDBt81JEZQjSURVLUDA0wA/csIhndPZG1vqGZs7QtYhorhmea8HTD6cn6FtRrNi1ud2huUjnp3WHkh9krfBU8YGrQ==",
#             "VlacnpQ/ifP1DfPQqIAk39VWcHydG9FRCtANscLCt9Z6UJ9XhUZOXrDse/fnVZpqd4SIJGXVaAIU5kVCVP0bdOdCpJ+wZEn97K3VoO9UKQqdSEYOHFw5jQlcTDcokfoODt8xdOb/cj4vJw9FfmuPInCvdj3g/iydgOpNkUEPegDTcnE5FdwKqsErNBwH/TUY1Z5Apw8ykqkbKMRdjgDwOU1DYy86jn/so5aHXZ5v7rMLnp6PpfUVaTvt4GTtaya83iRJRoIO0diBxbwKWx+/JTMbqmbMmvCHDyvewYq7kt+a4XUc0847vJHHdKQ2G7Ydc3oaeZ1EHwj4alg8ck5x3Q=="
#    ]
#     }

        # process.event_processor(test_event)