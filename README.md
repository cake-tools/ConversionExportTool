# CAKE Conversion Export Tool


In order to run the CAKE Conversion Export Tool, you will need to follow the directions below:

## Requirements
* `Python 3.4`
* `MongoDB`
* `AWS SQS`
* `AWS S3`

## Guidelines & Caveats:
  * This tool is not meant for LARGE date ranges or LARGE report sizes. Whenever possible, please keep your report queries within one or two days maximum. Remember, you have the ability to queue multiple jobs!
  * Please consider that it may take hours to complete one queued job.
  * Use the format 'dd-mm-yyyy' for both the start and end date (the dashes must be included)

## Instructions

1. First execute the following command in your terminal `pip install -r requirements.txt`. This will install all the necessary Python packages to run the tool.

2. Configure your AWS credentials
  * In your terminal run `aws config`. It will ask you a few questions requiring you to provide an AWS access key and secret key as well as your default region.

3. Fill in each empty quote in `settings.py`

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

## Additional Information

### Reading Your Queue Report (Web Interface)
#### Date Formatting
The `date format` for the web interface is defaulted to `dd-mm-yyyy`.

#### Job ID
A `job_id` looks like this `report_10012016_04022016`. It consists of 3 parts: title, start, and end date. The title `report` begins the job_id. The `start date` is in `DDMMYYYY` format; same with the `end date`. For the example `job id`, the csv would be contain the results of January 10th 2016 to February 4th 2016. 

#### Job Statuses
Your queued export job can have one of the following statuses:
* `Queued`: Your job was successfully added to the SQS queue. When the task runner is executed, it will read from the SQS queue and process each queue item in the order that it was entered. 
* `In Progress`: Your queued job is now being actively exported. In this state, it is best to leave your task runner alive until you receive an updated status.
* `Success`: Your queue job has completed. The `Download Link` is now visible. Clicking on the link will automatically start a download of your file.
* `Failed`: Your queue job encountered an error while being exported. Try reading the task runner output, or queue the job once again.
