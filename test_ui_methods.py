import unittest
import boto3
import mongomock
from moto import mock_s3, mock_sqs
from run import retrieve_scheduled_report, date_convert_for_api, QueueMessage, QueueJob
from settings import *
from pymongo import *


# Queue Job object returns desired format for SQS send Message
class TestQueueJobObject(unittest.TestCase):

    def setUp(self):
        self.queue_job = QueueJob("report_09042017_10042017", "09-04-2017", "10-04-2017", "12-04-2017 14:15:00", "Queued", "")

    def test_compare_jobs(self):
        print(self.queue_job)
        self.assertEqual(self.queue_job.start_date, "09-04-2017")
        self.assertEqual(self.queue_job.end_date, "10-04-2017")
        self.assertEqual(self.queue_job.job_id, "report_09042017_10042017")
        self.assertEqual(self.queue_job.created_date, "12-04-2017 14:15:00")
        self.assertEqual(self.queue_job.status, "Queued")
        self.assertEqual(self.queue_job.file_link, "")

    def test_retrieve_jobs(self):
        collection_name = mongomock.MongoClient().db.collection
        obj = dict(start_date="09-04-2017", end_date="10-04-2017", job_id="report_09042017_10042017", created_date="12-04-2017 14:15:00", status="Queued", file_link="")
        collection_name.insert(obj)
        #for obj in objects:
            #obj['_id'] = collection_name.insert(obj)

        job_list = retrieve_scheduled_report(collection_name)
        self.assertEqual(job_list[0].start_date, "09-04-2017")

# Date conversion from input date format to date format compliant for api call
class DateConversionTest(unittest.TestCase):
    
    def setUp(self):
        self.input_date = "09-04-2017"

    def test_date_conversion(self):
        self.assertEqual(date_convert_for_api(self.input_date), "04/09/17")

class QueueMessageObjectTest(unittest.TestCase):

    def setUp(self):
        self.queue_message = QueueMessage("09-04-2017", "10-04-2017", "report_09042017_10042017", "12-04-2017 14:15:00")

    def test_compare_messages(self):
        self.assertEqual(self.queue_message.start_date, "09-04-2017")
        self.assertEqual(self.queue_message.end_date, "10-04-2017")
        self.assertEqual(self.queue_message.job_id, "report_09042017_10042017")
        self.assertEqual(self.queue_message.created_date, "12-04-2017 14:15:00")


if __name__ == '__main__':
    unittest.main()
