from flask import Flask, render_template, request, redirect, url_for
import os
import requests
import json
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'static/uploads/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Databricks API Config
DATABRICKS_TOKEN = 'YOUR_DATABRICKS_TOKEN'
DATABRICKS_URL = 'https://<databricks-instance>.azuredatabricks.net'
DATABRICKS_JOB_ID = 'YOUR_JOB_ID'  # The job ID that runs your notebook

# Route for homepage
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle image upload
@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        # Check if the post request has the file part
        if 'file' not in request.files:
            return "No file part", 400
        file = request.files['file']
        
        # If user does not select file, browser also submits an empty part
        if file.filename == '':
            return "No selected file", 400
        
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            # Upload the image to cloud storage (Azure Blob, AWS S3, etc.)
            # Here you need to write code to upload the image to cloud storage
            # Assume `image_url` is the public URL of the image stored in the cloud

            image_url = upload_to_cloud_storage(file_path)

            # Trigger Databricks job
            job_response = run_databricks_notebook(image_url)
            job_id = job_response.get("run_id")

            # Redirect to results page with job ID
            return redirect(url_for('results', job_id=job_id))

# Function to run Databricks notebook via API
def run_databricks_notebook(image_url):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/run-now"
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    
    data = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": {
            "image_url": image_url
        }
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

# Route to check and display results
@app.route('/results/<job_id>')
def results(job_id):
    # Poll Databricks to check if the job has completed
    run_status = get_databricks_job_status(job_id)
    
    if run_status.get('state', {}).get('life_cycle_state') == 'TERMINATED':
        # Job is complete, fetch the results (could be a URL or some output)
        result = run_status.get('state', {}).get('result_state')
        if result == 'SUCCESS':
            # Fetch the output (e.g., processed image or JSON results)
            # Assume the processed image URL or results are saved in the notebook run output
            processed_image_url = fetch_databricks_output(job_id)
            return render_template('results.html', image_url=processed_image_url)
        else:
            return "Job failed", 500
    else:
        # Job is still running, or queued
        return "Job is still running. Please refresh the page.", 202

# Function to check job status in Databricks
def get_databricks_job_status(job_id):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/runs/get?run_id={job_id}"
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    response = requests.get(url, headers=headers)
    return response.json()

# Function to fetch output from Databricks (e.g., processed image URL)
def fetch_databricks_output(job_id):
    # Fetch output URL or JSON result from Databricks
    # Implement logic to retrieve output
    return "https://example.com/processed_image.jpg"  # Placeholder for actual image URL

# Function to upload the image to cloud storage (placeholder for Azure Blob or S3)
def upload_to_cloud_storage(file_path):
    # Implement cloud storage upload logic
    return "https://example.com/uploaded_image.jpg"  # Placeholder URL

if __name__ == '__main__':
    app.run(debug=True)
