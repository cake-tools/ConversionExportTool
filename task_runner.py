# -*- coding: utf-8 -*-

import boto3
import json
import time
from collections import OrderedDict
import re
import csv
from datetime import datetime, date, timedelta
import requests
from flask import Flask, render_template, url_for, request, redirect, g, jsonify, flash, session, Markup
from run import app
from pymongo import *
from bs4 import BeautifulSoup
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


def execute_call(response):

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


    try:
        with open('temp.csv', 'w') as text_file:
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
                print(start, end)
                payload = dict(
                    api_key=API_KEY,
                    start_date=start,
                    end_date=end,
                    conversion_type='all',
                    event_type='macro_event_conversions',
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
                    start_at_row=0,
                    row_limit=100000,
                    sort_field='event_conversion_date',
                    sort_descending='false')

                endpoint_string = 'http://' + ADMIN_DOMAIN_URL + '/api/15/reports.asmx/EventConversions'
                soup = requests.post(endpoint_string,json=payload)
                print('processing API response')
                soup_text = soup.text
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

                    country_code = ''
                    if not c['country'] is None:
                        country_code = c['country']['country_code']

                    region = ''
                    if not c['region'] is None:
                        region = c['region']['region_code']

                    language = ''
                    if not c['language'] is None:
                        language = c['language']['language_name']

                    isp = ''
                    if not c['isp'] is None:
                        isp = c['isp']['isp_name']

                    device = ''
                    if not c['device'] is None:
                        device = c['device']['device_name']

                    operating_system = ''
                    os_major = ''
                    os_minor = ''
                    if not c['operating_system'] is None:
                        operating_system = c['operating_system']['operating_system_name']
                        os_major = c['operating_system']['operating_system_version']['version_name']
                        os_minor = c['operating_system']['operating_system_version_minor']['version_name']

                    browser = ''
                    browser_major = ''
                    browser_minor = ''
                    if not c['browser'] is None:
                        browser = c['browser']['browser_name']
                        browser_major = c['browser']['browser_version']['version_name']
                        browser_minor = c['browser']['browser_version_minor']['version_name']

                    current_disposition = ''
                    if not c['current_disposition'] is None:
                        current_disposition = c["current_disposition"]["disposition_type"]["disposition_type_name"]


                    writer.writerow((c["event_conversion_id"],
                                    c["last_updated"],
                                    c["event_conversion_date"],
                                    c["click_date"],
                                    time_delta,
                                    c["source_affiliate"]["source_affiliate_id"],
                                    c["source_affiliate"]["source_affiliate_name"],
                                    c["brand_advertiser"]["brand_advertiser_id"],
                                    c["brand_advertiser"]["brand_advertiser_name"],
                                    c["site_offer"]["site_offer_id"],
                                    c["site_offer"]["site_offer_name"],
                                    c["creative"]["creative_id"],
                                    c["sub_id_1"],
                                    c["sub_id_2"],
                                    c["sub_id_3"],
                                    c["sub_id_4"],
                                    c["sub_id_5"],
                                    c["source_type"],
                                    c["paid"]["currency_id"],
                                    c["paid"]["amount"],
                                    c["received"]["currency_id"],
                                    c["received"]["amount"],
                                    c["pixel_dropped"],
                                    c["transaction_id"],
                                    c["event_conversion_ip_address"],
                                    c["click_ip_address"],
                                    country_code,
                                    c["event_conversion_referrer_url"],
                                    c["click_referrer_url"],
                                    c["event_conversion_user_agent"],
                                    c["click_user_agent"],
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
                                    c["event_conversion_score"],
                                    c["paid_unbilled"]["amount"],
                                    c["received_unbilled"]["amount"],
                                    c["click_request_session_id"],
                                    c["event_info"]["event_name"],
                                    c["price_format"]["price_format_name"],
                                    c["tracking_id"],
                                    c["udid"]))
                print('Processing complete')


        file_link = s3_job(job_id)
        print('REPORT SUCCESSFULLY CREATED')
        print(file_link)

        collection_name = db[MONGODB_DATABASE['collection_name']]
        collection_name.update_one({"created_date": created_date}, {"$set": {"status": "Success", "file_link": file_link }})
        in_progress = False

    except Exception:
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
                print(response)
                time.sleep(60.0 - ((time.time() - start_time) % 60.0))
            else:
                execute_call(response)
                time.sleep(60.0 - ((time.time() - start_time) % 60.0))
