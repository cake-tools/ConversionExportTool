from flask import Flask, render_template, url_for, request, redirect, g, jsonify, flash, session, Markup
from bs4 import BeautifulSoup
from pymongo import *
from functools import wraps
from datetime import datetime
import requests
import csv
import boto3
import ckapi
import validation
from settings import *

# FLASK CONFIG ###########################
app = Flask(__name__)
app.secret_key = APP_SECRET_KEY


# with app.app_context():

client = MongoClient(MONGODB_DATABASE['uri'])
db = client[MONGODB_DATABASE['database_name']]
collection = db[MONGODB_DATABASE['collection_name']]


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
        admin_domain = request.form['admin']
        username = request.form['username']
        password = request.form['password']

        session['api_key'] = ckapi.get_api_key(
            admin_domain, username, password)

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
def welcome():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']

        start_date_convert = date_convert_for_api(start_date) #date format mm/dd/yyyy for running api
        end_date_convert = date_convert_for_api(end_date) #date format mm/dd/yyyy for running api

        job_id = 'report_{}_{}'.format(start_date.replace('-', ''), end_date.replace('-', ''))
        created_date = str(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))

        queue_message = QueueMessage(start_date_convert, end_date_convert, job_id, created_date)
        sqs_job(queue_message)

        #collection_name = db[MONGODB_DATABASE['collection_name']]
        collection.insert({"job_id": job_id, "created_date": created_date,  "start_date": start_date, "end_date": end_date, "status": "Queued", "file_link": ""})

        message = "Job has been scheduled"

        job_list = retrieve_scheduled_report(collection)
        return render_template('welcome.html', message=message, jobs=job_list)
    else:
        job_list = retrieve_scheduled_report(collection)
        return render_template('welcome.html', jobs=job_list)

class QueueJob(object):

    def __init__(self, job_id, start_date, end_date, created_date, status, file_link):
        self.job_id = job_id
        self.start_date = start_date
        self.end_date = end_date
        self.created_date = created_date
        self.status = status
        self.file_link = file_link

class QueueMessage(object):

    def __init__(self, start_date, end_date, job_id, created_date):
        self.start_date = start_date
        self.end_date = end_date
        self.job_id = job_id
        self.created_date = created_date

# TODO: experiment using an ORM so you don't have to create a list, directly pass Mongo query response to template

def retrieve_scheduled_report(collection):
    job_list = []

    for i in collection.find({}, {'_id':0}).sort('created_date', -1):
        job_temp = QueueJob(i['job_id'], i['start_date'], i['end_date'], i['created_date'], i['status'], i['file_link'])
        job_list.append(job_temp)
    return job_list

def date_convert_for_api(date):
    date_result = datetime.strptime(date, '%d-%m-%Y').strftime('%m/%d/%y')
    return date_result


def sqs_job(queue_message):
    message_content = str({
        "start_date": "%s" % queue_message.start_date,
        "end_date": "%s" % queue_message.end_date,
        "job_id": "%s" %queue_message.job_id,
        "created_date": "%s" % queue_message.created_date
    })

    client = boto3.client('sqs')
    client.create_queue(QueueName = SQS_QUEUE['name'])
    r = client.get_queue_url(QueueName = SQS_QUEUE['name'])
    queue_url = r['QueueUrl']
    client.send_message(QueueUrl=queue_url, MessageBody=message_content, DelaySeconds=10)



if __name__ == "__main__":
    app.run(debug=False)
