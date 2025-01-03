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
        "account_number": "ewogICJwYXNzcGhyYXNlIjogIkpPQ0Zsbjh5V2dNS1Y2d0tQNUo5UDhVSEZwVDlkV1ZEbStqLzhKcm1rQkd3c1N3bkMxc1hDY0w1WnpZV2JaRW1hYTNTWklEZW0vWDhYbzZ3Q2t3cUtOaXF4VE9EalcwWTE4MThpTE11V0RFMDc2b2VseHowRVYyQmlscExoZjFkSWN6RDhCWEJ6U2IrS3VSZlZMSVo1NG5QSEpiTVBnNnJTSnRqSUNzN1ZjVllhQkFhSElicE1XbDlNaGV2Rm0veHc0V1U4UkNYMGZxYWZzVWFiUFJLRVUxWmlKc0U5a0hqMndDLzZtblZha21sY1pQQ0VUbnMwN1hQNURleHJxa3dEemZkZ25PTEU2eExmd1NwRzQ3YUFCVzJCOTVGZVdHNU8zQ0lKVXlCR2J5SE1sSFVJTGZXa2l0UFRJa1cxejhLaFIyaFFBa1dxanowaXlEZGlJQVhWUT09IiwKICAiY29udGVudCI6ICJVMkZzZEdWa1gxOEE4WWZjMEFZK0dLRDhBOXdGUlZRbndXalRzcldib2dEaktPdStlOXJHa1Y3eWY1SWxWWit6Igp9",
        "caller_bank_id": "IZXVGB23BWP"
        }
        process.event_processor(test_event)