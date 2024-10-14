from flask import Flask, render_template, request, redirect, url_for
import requests
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'static/uploads/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Databricks API Config
DATABRICKS_TOKEN = 'dapi8eaa98dd1ad13f41a326c867a91f1959'
DATABRICKS_URL = 'https://dbc-9ed4de26-ed93.cloud.databricks.com/'
DATABRICKS_JOB_ID = '212305500244037'  # The job ID that runs your notebook

# Azure Blob Storage Config
AZURE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=nfbstorageaccount;AccountKey=k9NyIYnSzIisNymw+l0nQ/OC08riRYuT5akPHBj/fY5LLiwy8o7AnBKolw2qJotcaMBILVKJlTSJ+AStRU/gXA==;EndpointSuffix=core.windows.net'
AZURE_CONTAINER_NAME = 'food-images-container'

BLOB_TOKEN = "sp=r&st=2024-10-14T17:59:52Z&se=2024-10-15T01:59:52Z&spr=https&sv=2022-11-02&sr=c&sig=5UVRnubtXNof7mI6by1saEGRpTWHb5182VUiwNDk36Q%3D"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

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

            # Upload the file to Azure Blob Storage
            blob_url = upload_to_azure_blob(file, filename)

            # Trigger Databricks job with the url of the image in the blob
            # storage
            job_response = run_databricks_notebook(blob_url)
            job_id = job_response.get("run_id")

            # Redirect to results page with job ID
            return redirect(url_for('results', job_id=job_id))

# Function to upload image to Azure Blob Storage
def upload_to_azure_blob(file, filename):
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=filename)

    # Upload the file to Azure Blob Storage
    blob_client.upload_blob(file, overwrite=True)

    # Construct the Blob URL
    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{filename}"
    blob_url = blob_url + "?" + BLOB_TOKEN
    return blob_url


# Function to run Databricks notebook via API
def run_databricks_notebook(blob_url):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/run-now"
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    
    data = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": {
            "blob_url": blob_url
        }
    }
    
    response = requests.post(url, headers=headers, json=data)
    return response.json()

# Route to check and display results
@app.route('/results/<job_id>')
def results(job_id):
    # Poll Databricks to check if the job has completed
    run_status = get_databricks_job_status(job_id)
    
    if run_status.get('state', {}).get('life_cycle_state') == 'TERMINATED':
        # Job is complete, fetch the results
        result = run_status.get('state', {}).get('result_state')
        if result == 'SUCCESS':
            # Fetch the output (e.g., processed image or JSON results)
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
    # Implement logic to retrieve output from the job run
    # Assuming the output is stored in a specific way in the notebook
    return f"https://<databricks-instance>.azuredatabricks.net/files/output/{job_id}/processed_image.jpg"  # Adjust according to your needs


if __name__ == '__main__':
    app.run(debug=True)
