import requests
import json
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient
from flask import Flask, render_template, request, redirect, url_for

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
    if response.status_code != 200:
        return {"status": "error", "message": response.text}

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
            # Assuming your job returns the recipes as JSON output
            output = fetch_databricks_output(job_id)
            if "recipes" in output:
                return render_template('results.html', recipes=output.get("recipes"))
            if "error" in output:
                return render_template('results.html', error=output.get("error"))
        else:
            error_message = run_status.get('state', {}).get('message', 'Job failed with no specific error.')
            return render_template('results.html', error=error_message)
    else:
        # Job is still running, or queued
        return "Job is still running. Please refresh the page.", 202


# Function to check job status in Databricks
def get_databricks_job_status(job_id):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/runs/get?run_id={job_id}"
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    response = requests.get(url, headers=headers)
    return response.json()

# Function to fetch output from Databricks (e.g., processed image URL and recipes)
def fetch_databricks_output(job_id):
    # Replace with your Databricks API token and workspace URL
    databricks_api_token = DATABRICKS_TOKEN
    databricks_instance = DATABRICKS_URL

    # Set the headers for the API request
    headers = {
        'Authorization': f'Bearer {databricks_api_token}',
        'Content-Type': 'application/json',
    }

    # Define the URL to retrieve the job run output (adjust as necessary)
    run_output_url = f"{databricks_instance}/api/2.0/jobs/runs/get-output?run_id={job_id}"

    # Make the request to fetch the job output
    response = requests.get(run_output_url, headers=headers)
    if response.status_code == 200:
        try:
            # Attempt to parse the response as JSON
            output_data = response.json()

            # Check if the job run has a notebook output
            notebook_output = output_data.get('notebook_output', {})
            if notebook_output:
                # Extract the result and parse it as JSON
                result_str = notebook_output.get('result', '')
                result_data = json.loads(result_str) if result_str else {}

                # Check the status of the output
                if result_data.get('status') == 'success':
                    # Assuming you have a key for image_url or similar in a successful output
                    image_url = result_data.get('data', {}).get('image_url', '')
                    recipes = result_data.get('data', {}).get('recipes', [])
                    
                    return {
                        'image_url': image_url,
                        'recipes': recipes,
                    }
                else:
                    # If the status is error, get the error message
                    error_message = result_data.get('message', 'Unknown error')
                    print(f"Error from Databricks: {error_message}")
                    return {
                        'error': error_message
                    }
            else:
                print("No notebook output found.")
                return None

        except ValueError as e:
            print("Error parsing JSON:", e)
            print("Response text:", response.text)  # Print the raw response text
            return None
    else:
        # Handle errors, return None or raise an exception
        print(f"Error fetching output: {response.status_code} - {response.text}")
        return None

if __name__ == '__main__':
    app.run(debug=True)
