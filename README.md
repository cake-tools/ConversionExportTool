# CAKE Conversion Export Tool


In order to run the CAKE Conversion Export Tool, you will need to follow the directions below:

## Requirements
* `Python 3.4`
* `MongoDB`

## Guidelines & Caveats:
  * This tool is not meant for LARGE date ranges or LARGE report sizes. Whenever possible, please keep your report queries within one or two days maximum. Remember, you have the ability to queue multiple jobs!
  * Please consider that it may take hours to complete one queued job.
  * Use the format 'dd-mm-yyyy' for both the start and end date (the dashes must be included)

## Instructions
1. First execute the following command in your terminal `pip install -r requirements.txt`. This will install all the necessary Python packages to run the tool.

2. Configure your AWS credentials
  * In your terminal run `aws config`. It will ask you a few questions requiring you to provide an AWS access key and secret key as well as your default region.

2. Fill in the global variables in `run.py`

  * `app.config['MONGO_DBNAME']`: enter the name of your mongoDB name.
  * `app.config['MONGO_URI']`: populate the empty string with the uri address to your mongoDB table
  * `sqs_queue_name`: log into your AWS Console and fill in this string with the name of the queue you would like to send queued export jobs
  * `db_collection_name`: inside your mongoDB instance is a collection name.


3. Fill in the global variables in `task_runner.py`

  * `MongoClient`: populate the empty string with the uri address to your mongoDB table
  * `database`: replace 'job_table' with the your mongoDB instance. example: job_table
  * `admin_domain`: Fill in your admin domain url, without "http://"
  * `api_key`: Provide an api key from your CAKE Instance
  * `sqs_queue_name`: The queue url of your SQS queue.
    * Example: https://sqs.us-east-1.amazonaws.com/012319234/QueueName
  * `s3_bucket_name`: #within the quotes, fill in with the name of the s3 bucket to store completed csv.


4. Run the web interface
  * In the main cake conversion tools directory, run the command 'python run.py', this starts the Flask web server for the web interface.
  * Navigate to "http://localhost:5000"
  * Log into the interface using your admin domain address (without 'http://'), and an email address and password for a user that has admin access in your CAKE instance.

Once your credentials are validated, you will land on the welcome page, which will allow you to queue report exports.

5. Scheduling a Conversion Report Export:
  * Fill in the start and end date, being mindful of the date format 'dd-mm-yyyy' (the dashes must be included)
  * Click "Schedule Job"
  * When the job is successfully added to the queue, the message 'job has been scheduled' will appear underneath the grey well.
  * If you want to schedule multiple jobs, just add another start and end date combo.

Great! You've scheduled an export. Now in a separate terminal window, navigate to the project and run the command 'python task_runner.py'. The script repeats every minute to check if there are new jobs in the queue and subsequently processes those. Keep 'task_runner.py' open for the duration of the export job.

The Task Runner connects to the SQS queue and processes each job in the queue. Keep in mind that it may take a few hours to complete your export. Once the job is complete it drops the completed CSV in your S3 bucket as well as providing a link to download the report in the web interface.

The csv download link, "Download Report" on the Conversion Export Tool interface is only valid for 24 hours from the time the queue job completes. This is a limitation of S3. Your csv file will always be accessible from S3.
