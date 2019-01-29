# -*- coding: utf-8 -*-

import boto3
import json
import time
from collections import OrderedDict
import re
import csv
from datetime import datetime, date, timedelta
import urllib3
import requests
from flask import Flask, render_template, url_for, request, redirect, g, jsonify, flash, session, Markup
from run import app
from pymongo import *
from bs4 import BeautifulSoup
import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from settings import *

client = MongoClient(MONGODB_DATABASE['uri'])
db = client[MONGODB_DATABASE['database_name']]

in_progress = False

def get_country_codes():
    payload = dict(
        api_key=API_KEY,
        )

    endpoint_string = 'http://' + ADMIN_DOMAIN_URL + '/api/1/get.asmx/Currencies'
    soup = requests.post(endpoint_string,json=payload)
    r = soup.json()

    country_codes = {}

    for x in r["d"]["currencies"]:
        abbreviation = x["currency_abbr"]
        currency_id = str(x["currency_id"])
        country_codes[currency_id] = abbreviation

    return country_codes

country_codes = get_country_codes()

def return_currency_name(country_id, country_codes):
    country_id = str(country_id)
    country_name = ''.join({value for key, value in country_codes.items() if country_id == key})
    return country_name


def receive_message():

    in_progress = True
    client = boto3.client('sqs')
    queue_size_response = client.get_queue_attributes(QueueUrl= SQS_QUEUE['url'],
                                                AttributeNames=['ApproximateNumberOfMessages'])
    queue_size = queue_size_response["Attributes"]["ApproximateNumberOfMessages"]
    if queue_size != "0":
        response = OrderedDict(client.receive_message(QueueUrl = SQS_QUEUE['url'],
                                            AttributeNames=['Body'],
                                            MaxNumberOfMessages=1))
        return response
    elif queue_size == "0":
        response = "No Messages in Queue"
        return response

def delete_message(receipt_handle):
    client = boto3.client('sqs')
    response = client.delete_message(QueueUrl= SQS_QUEUE['url'],
                                     ReceiptHandle=receipt_handle)
    return response

def enumerate_dates(start, end):
    start_date = datetime(start.year, start.month, start.day)
    end_date = datetime(end.year, end.month, end.day)
    delta = end_date - start_date

    for i in range(delta.days + 1):
        next_start_date = start_date + timedelta(days=1)
        for i in range(48):
            next_start_hour = start_date + timedelta(minutes=30)
            yield start_date, next_start_hour
            start_date = next_start_hour
        start_date=next_start_date

def date_convert_for_csv(date):
    extract_integers = re.findall('\d+', date)
    date_string = ''.join(extract_integers)
    if len(date_string) > 10:
        date_string = date_string[:10] + '.' + date_string[10:]
        date_result = (datetime.utcfromtimestamp(float(date_string)) + timedelta(hours=1)).strftime("%d-%m-%YT%H:%M:%S.%f")
        return date_result
    else:
        timestamp_parsed = (datetime.utcfromtimestamp(int(date_string))+ timedelta(hours=1)) + '.000000'
        date_result = timestamp_parsed.strftime("%d-%m-%YT%H:%M:%S.%f")
        return date_result

def conversion_time_delta(conversion_date, click_date):
    conversion_date = datetime.strptime(conversion_date, "%d-%m-%YT%H:%M:%S.%f")
    click_date = datetime.strptime(click_date, "%d-%m-%YT%H:%M:%S.%f")

    if click_date > conversion_date:
        time_delta = "1"
        return time_delta

    elif conversion_date > click_date:
        time_delta = conversion_date - click_date
        if time_delta.seconds < 1:
            time_delta = "1"
            return time_delta
        else:
            return time_delta

def s3_job(filename):
# expire 86400 seconds is 24 hours
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('temp.csv', S3_BUCKET['name'], '%s.csv' % filename)

    client = boto3.client('s3')
    url = client.generate_presigned_url('get_object',
                                        Params={'Bucket': S3_BUCKET['name'],'Key': '%s.csv' % filename},
                                        ExpiresIn=86400)
    return url

def local_job(filename):
    os.rename('temp.csv', '%s.csv' % filename)
    fn = '%s.csv' % filename
    return fn


def execute_call(response):

    BAD_GATEWAY_ERROR = 502
    RATE_LIMITED_ERROR = 429
    MAX_NUM_SECONDS_TO_SLEEP = 30
    MAX_NUM_OF_ALLOWED_RETRIES = 10

    body = (response["Messages"][0]["Body"]).replace("'", "\"")
    load_body = json.loads(body)

    start_date = load_body['start_date']
    end_date = load_body['end_date']
    job_id = load_body['job_id']
    created_date = load_body['created_date']
    receipt_handle = response["Messages"][0]["ReceiptHandle"]
    delete_message(receipt_handle)

    start_datetime = datetime.strptime(start_date, "%m/%d/%y")
    end_datetime = datetime.strptime(end_date, "%m/%d/%y")

    collection_name = db[MONGODB_DATABASE['collection_name']]
    collection_name.update_one({"created_date": created_date}, {"$set": {"status": "In Progress"}})

    num_retries = 0
    response = ''

    try:
        with open('temp.csv', 'wb') as text_file:
            writer = csv.writer(text_file)
            writer.writerow(('Conversion ID',
                            'Last Updated',
                            'Conversion Date',
                            'Click Date',
                            'Click to Conversion Time',
                            'Affiliate ID',
                            'Affiliate Name',
                            'Advertiser ID',
                            'Advertiser Name',
                            'Offer ID',
                            'Offer Name',
                            'Creative',
                            'Sub ID',
                            'Sub ID 2',
                            'Sub ID 3',
                            'Sub ID 4',
                            'Sub ID 5',
                            'Type',
                            'Price Paid Currency',
                            'Paid',
                            'Price Received Currency',
                            'Received',
                            'Pixel',
                            'Transaction ID',
                            'IP Address',
                            'Click IP Address',
                            'Country',
                            'Conversion Referrer',
                            'Referrer',
                            'Conversion User Agent',
                            'Click User Agent',
                            'Disposition',
                            'Region',
                            'Language',
                            'Provider Name',
                            'Device',
                            'Operating System',
                            'OS(Major Version)',
                            'OS(Minor Version)',
                            'Browser',
                            'Browser(Major Version)',
                            'Browser(Minor Versions)',
                            'Conversion Score',
                            'Paid Unbilled',
                            'Received Unbilled',
                            'Click Request Session ID',
                            'Event Name',
                            'Price Format',
                            'Tracking ID',
                            'UDID'))

            date_generator = enumerate_dates(start_datetime, end_datetime)

            for start, end in date_generator:
                start = datetime.strftime(start, '%m/%d/%Y %H:%M')
                end = datetime.strftime(end, '%m/%d/%Y %H:%M')

                payload = dict(
                    api_key=API_KEY,
                    start_date=start,
                    end_date=end,
                    conversion_type='all',
                    event_type='all',
                    event_id=0,
                    channel_id=0,
                    source_affiliate_id=0,
                    brand_advertiser_id=0,
                    site_offer_id=0,
                    site_offer_contract_id=0,
                    source_affiliate_tag_id=0,
                    brand_advertiser_tag_id=0,
                    site_offer_tag_id=0,
                    campaign_id=0,
                    creative_id=0,
                    price_format_id=0,
                    disposition_type='all',
                    disposition_id=0,
                    source_affiliate_billing_status='all',
                    brand_advertiser_billing_status='all',
                    test_filter='non_tests',
                    source_type='all',
                    payment_percentage_filter='both',
                    start_at_row=0,
                    row_limit=100000,
                    sort_field='event_conversion_date',
                    sort_descending='false')

                endpoint_string = 'http://' + ADMIN_DOMAIN_URL + '/api/17/reports.asmx/EventConversions'

                logger.info('%s - Calling API : start: %s, End: %s', datetime.now(), start, end)

                while True:
                    if num_retries > MAX_NUM_OF_ALLOWED_RETRIES:
                        logger.error('Tried more than {} times without success. Skipping report {} .'
                                .format(MAX_NUM_OF_ALLOWED_RETRIES, job_id))
                        return

                    try:
                        response = requests.post(endpoint_string,json=payload, stream=True)
                        if response.status_code == BAD_GATEWAY_ERROR:
                            logger.info('Bad Gateway Error. Waiting for {} seconds and will try again.'
                                    .format(str(MAX_NUM_SECONDS_TO_SLEEP)))
                            time.sleep(MAX_NUM_SECONDS_TO_SLEEP)
                            num_retries += 1
                            continue

                        if response.status_code == RATE_LIMITED_ERROR:
                            logger.info('Throttle detected. Waiting for {} seconds and will try again.'
                                    .format(str(MAX_NUM_SECONDS_TO_SLEEP)))
                            time.sleep(MAX_NUM_SECONDS_TO_SLEEP)
                            num_retries += 1
                            continue

                        if response.status_code != 200:
                            logger.error('Error with status code {}. Skipping this report {}'
                                    .format(response.status_code, job_id))
                            logger.info('response = '.format(response))
                            return

                        break

                    except (requests.ConnectionError, urllib3.exceptions.MaxRetryError) as error:
                        logger.error("ConnectionError: {0}".format(error))
                        logger.info("Sleeping for {} seconds...".format(MAX_NUM_SECONDS_TO_SLEEP))
                        time.sleep(MAX_NUM_SECONDS_TO_SLEEP)
                        num_retries += 1
                        continue
                    else:
                        return

                logger.info("%s - Processing ...", datetime.now())

                soup_text = response.text
                response = json.loads(soup_text)

                for c in response["d"]["event_conversions"]:
                    c["event_conversion_date"] = date_convert_for_csv(c["event_conversion_date"])
                    c["last_updated"] = date_convert_for_csv(c["last_updated"])
                    c["paid"]["currency_id"] = return_currency_name(c["paid"]["currency_id"], country_codes)
                    c["received"]["currency_id"] = return_currency_name(c["received"]["currency_id"], country_codes)

                    if c["click_date"]:
                        c["click_date"] = date_convert_for_csv(c["click_date"])

                    if c["event_conversion_date"] and c["click_date"]:
                        time_delta = conversion_time_delta(c["event_conversion_date"], c["click_date"])
                    else:
                        time_delta = ""

                    # encodings
                    conversion_id = c["event_conversion_id"]
                    last_updated = c["last_updated"]
                    conversion_date = c["event_conversion_date"]
                    click_date = c["click_date"]
                    affiliate_id = c["source_affiliate"]["source_affiliate_id"]
                    affiliate_name = c["source_affiliate"]["source_affiliate_name"].encode('utf-8','ignore')
                    advertiser_id = c["brand_advertiser"]["brand_advertiser_id"]
                    advertiser_name = c["brand_advertiser"]["brand_advertiser_name"].encode('utf-8', 'ignore')
                    offer_id = c["site_offer"]["site_offer_id"]
                    offer_name = c["site_offer"]["site_offer_name"].encode('utf-8', 'ignore')
                    creative_id = c["creative"]["creative_id"]
                    sub_id = c["sub_id_1"].encode('utf-8', 'ignore')
                    sub_id_2 = c["sub_id_2"].encode('utf-8', 'ignore')
                    sub_id_3 = c["sub_id_3"].encode('utf-8', 'ignore')
                    sub_id_4 = c["sub_id_4"].encode('utf-8', 'ignore')
                    sub_id_5 = c["sub_id_5"].encode('utf-8', 'ignore')
                    source_type = c["source_type"]
                    paid_currency_id = c["paid"]["currency_id"]
                    paid_amount = c["paid"]["amount"]
                    received_currency_id = c["received"]["currency_id"]
                    received_amount = c["received"]["amount"]
                    pixel_dropped = c["pixel_dropped"]
                    conversion_ip_address = c["event_conversion_ip_address"]
                    click_ip_address = c["click_ip_address"]
                    transaction_id = c["transaction_id"].encode('utf-8', 'ignore')
                    event_conversion_score = c["event_conversion_score"]
                    paid_unbilled_amount = c["paid_unbilled"]["amount"]
                    received_unbilled_amount = c["received_unbilled"]["amount"]
                    click_request_session_id = c["click_request_session_id"]
                    event_name = c["event_info"]["event_name"].encode('utf-8', 'ignore')
                    price_format = c["price_format"]["price_format_name"].encode('utf-8', 'ignore')
                    tracking_id = c["tracking_id"]

                    event_conversion_user_agent = ''
                    if not c["event_conversion_user_agent"] is None:
                        event_conversion_user_agent = c["event_conversion_user_agent"].encode('utf-8', 'ignore')

                    click_user_agent = ''
                    if not c["click_user_agent"] is None:
                        click_user_agent = c["click_user_agent"].encode('utf-8', 'ignore')

                    conversion_referrer = ''
                    if not c['event_conversion_referrer_url'] is None:
                        conversion_referrer = c["event_conversion_referrer_url"].encode('utf-8', 'ignore')

                    referrer = ''
                    if not c["click_referrer_url"] is None:
                        referrer = c["click_referrer_url"].encode('utf-8', 'ignore')

                    country_code = ''
                    if not c['country'] is None:
                        country_code = c['country']['country_code'].encode('utf-8', 'ignore')

                    region = ''
                    if not c['region'] is None:
                        region = c['region']['region_name'].encode('utf-8', 'ignore')

                    language = ''
                    if not c['language'] is None:
                        language = c['language']['language_name'].encode('utf-8', 'ignore')

                    isp = ''
                    if not c['isp'] is None:
                        isp = c['isp']['isp_name'].encode('utf-8', 'ignore')

                    device = ''
                    if not c['device'] is None:
                        device = c['device']['device_name'].encode('utf-8', 'ignore')

                    operating_system = ''
                    os_major = ''
                    os_minor = ''
                    if not c['operating_system'] is None:
                        operating_system = c['operating_system']['operating_system_name'].encode('utf-8', 'ignore')
                        os_major = c['operating_system']['operating_system_version']['version_name']
                        os_minor = c['operating_system']['operating_system_version_minor']['version_name']

                    browser = ''
                    browser_major = ''
                    browser_minor = ''
                    if not c['browser'] is None:
                        browser = c['browser']['browser_name'].encode('utf-8', 'ignore')
                        browser_major = c['browser']['browser_version']['version_name']
                        browser_minor = c['browser']['browser_version_minor']['version_name']

                    current_disposition = ''
                    if not c['current_disposition'] is None:
                        current_disposition = c["current_disposition"]["disposition_type"]["disposition_type_name"].encode('utf-8', 'ignore')

                    udid = ''
                    if not c['udid'] is None:
                        udid = c['udid'].encode('utf-8', 'ignore')

                    writer.writerow((conversion_id,
                                    last_updated,
                                    conversion_date,
                                    click_date,
                                    time_delta,
                                    affiliate_id,
                                    affiliate_name,
                                    advertiser_id,
                                    advertiser_name,
                                    offer_id,
                                    offer_name,
                                    creative_id,
                                    sub_id,
                                    sub_id_2,
                                    sub_id_3,
                                    sub_id_4,
                                    sub_id_5,
                                    source_type,
                                    paid_currency_id,
                                    paid_amount,
                                    received_currency_id,
                                    received_amount,
                                    pixel_dropped,
                                    transaction_id,
                                    conversion_ip_address,
                                    click_ip_address,
                                    country_code,
                                    conversion_referrer,
                                    referrer,
                                    event_conversion_user_agent,
                                    click_user_agent,
                                    current_disposition,
                                    region,
                                    language,
                                    isp,
                                    device,
                                    operating_system,
                                    os_major,
                                    os_minor,
                                    browser,
                                    browser_major,
                                    browser_minor,
                                    event_conversion_score,
                                    paid_unbilled_amount,
                                    received_unbilled_amount,
                                    click_request_session_id,
                                    event_name,
                                    price_format,
                                    tracking_id,
                                    udid))

                #logger.info('%s - Processing complete', datetime.now())

        file_link = s3_job(job_id)
        #file_link = local_job(job_id)
        logger.info('REPORT SUCCESSFULLY CREATED: %s', file_link)

        collection_name = db[MONGODB_DATABASE['collection_name']]
        collection_name.update_one({"created_date": created_date}, {"$set": {"status": "Success", "file_link": file_link }})
        in_progress = False

    except (KeyboardInterrupt, Exception):
        collection_name = db[MONGODB_DATABASE['collection_name']]
        collection_name.update_one({"created_date": created_date}, {"$set": {"status": "Failed"}})
        in_progress = False
        raise

start_time = time.time()

if __name__ == "__main__":
    while True:
        if in_progress == True:
            time.sleep(60.0 - ((time.time() - start_time) % 60.0))
        else:
            response = receive_message()
            if response == "No Messages in Queue":
                logger.info(response)
                time.sleep(60.0 - ((time.time() - start_time) % 60.0))
            else:
                logger.info(time.strftime("%H:%M:%S"))
                execute_call(response)
                time.sleep(60.0 - ((time.time() - start_time) % 60.0))
