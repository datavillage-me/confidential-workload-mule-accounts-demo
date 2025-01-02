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
    
    def test_get_suspicious_accounts(self):
        """
        Try the process to get suspicious account
        """
        test_event = {
        "type": "GET_SUSPICIOUS_ACCOUNTS",
        "bank_id": "IZXVGB23BWP"
        }
        process.event_processor(test_event)