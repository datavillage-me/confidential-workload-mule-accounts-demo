"""
Unit test for the process module.
"""

import dotenv
dotenv.load_dotenv('.env')
import unittest
import logging
import process


class Test(unittest.TestCase):
    """
    collection of test related to event processing

    """

    # def test_data_quality_check(self):
    #     """
    #     Try the process to check data quality
    #     """
    #     test_event = {
    #         'type': 'CHECK_DATA_QUALITY'
    #     }
    #     process.event_processor(test_event)
    
    # def test_get_suspicious_accounts(self):
    #     """
    #     Try the process to get suspicious account
    #     """
    #     test_event = {
    #     "type": "GET_SUSPICIOUS_ACCOUNTS",
    #     "bank_id": "IZXVGB23BWP"
    #     }
    #     process.event_processor(test_event)
    
    def test_get_suspicious_accounts(self):
        """
        Try the process to check mule account
        """
        test_event = {
        "type": "CHECK_MULE_ACCOUNT",
        "account_number": "ewogICJwYXNzcGhyYXNlIjogIkh6OVQ2NjNHcG1wSnpPNkYrVkMrMFcvYXBXMGcvVnYwTlkyaU1QWU56MFRJZ3k0UDhJQVI2QkJJNERzek15UzZuUURPMkFyM0dWeitXdTN5UHJ2V0plaG5Oa0pOb2xWNUtVWWVvYTVlTHJDZG40RXdPQUVRL0U0NFltV3NXN1pGRFNidHJyUDEwcW1ubTdwWlV0Z2lERTFLMmpwRWRBSWxrQVNacWdsNVIwSXJJOUhURmtEeDJJT25hSStPeXZwY1ZySHgxTEZjZGVkdXRnSElZNHI1Z2hWTk1WcEo5OFN5NksxZmNKQWMwWGJjSWZOSXM1OTBqKzAvVENGMmYwTjhJRXhzU0hQb290SlFIM1dLYmhkcldHMU5FUEdaMlA5a1djNWloL3VzZnJHL3MyZVlqZStvLzdneHhnTTgxNTFmbkJJbFdsc3RManZseWJZRHJNQnpIZz09IiwKICAiY29udGVudCI6ICJVMkZzZEdWa1gxOWtIaEFFbVRpTWdRSStoaVl3bEZjSzhSbmNBWElhd0phZWVnSExRai9rcG1RK1RZNFVxNVBMIgp9",
        "caller_bank_id": "IZXVGB23BWP"
        }
        process.event_processor(test_event)