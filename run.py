from flask import Flask, render_template, url_for, request, redirect, g, jsonify, flash, session, Markup
from bs4 import BeautifulSoup
from flask_pymongo import PyMongo
from functools import wraps
from datetime import datetime
import requests
import csv
import boto3
import ckapi
import validation

# FLASK CONFIG ###########################
app = Flask(__name__)
app.secret_key = '\xe0\xc9\x92\x02\xde98?yubr4\xc3\xdb\xf4\xbaWPrvPx\xe0'


with app.app_context():

    app.config['MONGO_DBNAME'] = " " # for example: 'job_table'
    app.config['MONGO_URI'] = " " # For example: 'mongodb://test:pass@ds1123121018.mlab.com:112131/job_table'

    mongo = PyMongo(app)
    db_collection_name = mongo.db.collection_name # replace 'collection_name' with the name of your mongoDB collection name
    sqs_queue_name = " " # within the quotes fill in with the name of your SQS queue. For example: 'ConversionReportQueue'


    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return redirect(url_for('index', next=request.url))
            return f(*args, **kwargs)
        return decorated_function


    @app.route('/', methods=['GET', 'POST'])
    def index():

        if request.method == 'POST':
            raw_url = request.form['admin']
            session['admin_domain'] = raw_url.replace('https', '').replace(':', '').replace('/', '').replace('http', '').replace(':', '').replace('/', '')
            username = request.form['username']
            password = request.form['password']

            session['api_key'] = ckapi.get_api_key(
                session['admin_domain'], username, password)

            if session['api_key']:
                session['logged_in'] = True
                session['username'] = username
                return redirect(url_for('welcome'))

            else:
                flash_str = "Remote authentication failed. Please try again."
                flash(flash_str)
                return redirect(url_for('index'))

        else:
            if session.get('logged_in'):
                return redirect(url_for('welcome'))
            else:
                return render_template('login.html')

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for('index'))


    @app.route('/welcome', methods=['GET', 'POST'])
    @login_required
    def welcome():
        if request.method == 'POST':
            start_date = request.form['start_date']
            end_date = request.form['end_date']

            start_date_convert = date_convert_for_api(start_date) #date format mm/dd/yyyy for running api
            end_date_convert = date_convert_for_api(end_date) #date format mm/dd/yyyy for running api

            job_id = 'report_{}_{}'.format(start_date.replace('-', ''), end_date.replace('-', ''))
            created_date = str(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))

            total_row_count = find_total_rows(session['admin_domain'], session['api_key'], start_date_convert, end_date_convert)
            tier = determine_tier(total_row_count)

            queue_message = QueueMessage(start_date_convert, end_date_convert, job_id, tier, total_row_count, created_date)
            sqs_job(queue_message)


            db_collection_name.insert({"job_id": job_id, "created_date": created_date,  "start_date": start_date, "end_date": end_date, "status": "Queued", "file_link": ""})

            message = "Job has been scheduled"

            job_list = retrieve_scheduled_report()
            return render_template('welcome.html', message=message, jobs=job_list)
        else:
            job_list = retrieve_scheduled_report()
            return render_template('welcome.html', jobs=job_list)

    def retrieve_scheduled_report():
        job_list = []
        for i in db_collection_name.find({}, {'_id':0}).sort('created_date', -1):
            job_temp = Job(i['job_id'], i['start_date'], i['end_date'], i['created_date'], i['status'], i['file_link'])
            job_list.append(job_temp)
        return job_list

    def date_convert_for_api(date):
        date_result = datetime.strptime(date, '%d-%m-%Y').strftime('%m/%d/%y')
        return date_result

    def find_total_rows(admin_domain, api_key, start_date, end_date):

        payload = dict(
            api_key=api_key,
            start_date=start_date,
            end_date=end_date,
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
            test_filter='both',
            start_at_row=0,
            row_limit=1,
            sort_field='event_conversion_id',
            sort_descending='false')

        endpoint_string = 'http://' + admin_domain + '/api/15/reports.asmx/EventConversions'
        soup = requests.get(endpoint_string,params=payload)
        bsoup = BeautifulSoup(soup.text)
        row_count = bsoup.find('row_count').get_text()
        return row_count

    def determine_tier(row_count):
        row_count = int(row_count)
        if row_count > 50000:
            tier = 'high'
            return tier
        elif row_count > 12000 and row_count < 50000:
            tier = 'medium'
            return tier
        elif row_count < 12000:
            tier = 'low'
            return tier


    class QueueMessage(object):

        def __init__(self, start_date, end_date, job_id, tier, total_row_count, created_date):
            self.start_date = start_date
            self.end_date = end_date
            self.job_id = job_id
            self.tier = tier
            self.total_row_count = total_row_count
            self.created_date = created_date

    def sqs_job(queue_message):
        message_content = str({
            "start_date": "%s" % queue_message.start_date,
            "end_date": "%s" % queue_message.end_date,
            "job_id": "%s" %queue_message.job_id,
            "tier": "%s" % queue_message.tier,
            "total_row_count": "%s" % queue_message.total_row_count,
            "created_date": "%s" % queue_message.created_date
        })

        client = boto3.client('sqs')
        client.create_queue(QueueName = sqs_queue_name)
        r = client.get_queue_url(QueueName = sqs_queue_name)
        queue_url = r['QueueUrl']
        client.send_message(QueueUrl=queue_url, MessageBody=message_content, DelaySeconds=10)


    class Job(object):

        def __init__(self, job_id, start_date, end_date, created_date, status, file_link):
            self.job_id = job_id
            self.start_date = start_date
            self.end_date = end_date
            self.created_date = created_date
            self.status = status
            self.file_link = file_link


    @app.route('/report', methods=['GET'])
    def report():
        job_list = []
        for i in db_collection_name.find({}, {'_id':0}).sort('created_date', -1):
            job_temp = Job(i['job_id'], i['start_date'], i['end_date'], i['created_date'], i['status'], i['file_link'])
            job_list.append(job_temp)

        return render_template('report.html', jobs=job_list)



if __name__ == "__main__":
    app.run(debug=False)
