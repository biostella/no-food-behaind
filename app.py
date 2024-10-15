import requests
import json
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = "static/uploads/"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Databricks API Config
DATABRICKS_TOKEN = "dapi8eaa98dd1ad13f41a326c867a91f1959"
DATABRICKS_URL = "https://dbc-9ed4de26-ed93.cloud.databricks.com/"
DATABRICKS_JOB_ID = "212305500244037"  # The job ID that runs your notebook

# Azure Blob Storage Config
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=nfbstorageaccount;AccountKey=k9NyIYnSzIisNymw+l0nQ/OC08riRYuT5akPHBj/fY5LLiwy8o7AnBKolw2qJotcaMBILVKJlTSJ+AStRU/gXA==;EndpointSuffix=core.windows.net"
AZURE_CONTAINER_NAME = "food-images-container"

BLOB_TOKEN = "sp=r&st=2024-10-15T14:33:25Z&se=2024-12-25T23:33:25Z&spr=https&sv=2022-11-02&sr=c&sig=guo%2BSXVtEm7hHE2SQBnAMsdMp%2Bggy5p1KKgxdAARlAQ%3D"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


# Route for homepage
@app.route("/")
def index():
    return render_template("index.html")


# Route to handle image upload
@app.route("/upload", methods=["POST"])
def upload_file():
    if request.method == "POST":
        # Check if the post request has the file part
        if "file" not in request.files:
            return "No file part", 400
        file = request.files["file"]

        # If user does not select file, browser also submits an empty part
        if file.filename == "":
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
            return redirect(url_for("results", job_id=job_id))


# Function to upload image to Azure Blob Storage
def upload_to_azure_blob(file, filename):
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container=AZURE_CONTAINER_NAME, blob=filename
    )

    # Upload the file to Azure Blob Storage
    blob_client.upload_blob(file, overwrite=True)

    # Construct the Blob URL
    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{filename}"
    blob_url = blob_url + "?" + BLOB_TOKEN
    return blob_url


# Function to run Databricks notebook via API
def run_databricks_notebook(blob_url):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    data = {"job_id": DATABRICKS_JOB_ID, "notebook_params": {"blob_url": blob_url}}

    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        return {"status": "error", "message": response.text}

    return response.json()


# Route to check and display results
@app.route("/results/<job_id>")
def results(job_id):
    # Poll Databricks to check if the job has completed
    run_status = get_databricks_job_status(job_id)

    if run_status.get("state", {}).get("life_cycle_state") == "TERMINATED":
        # Job is complete, fetch the results
        result = run_status.get("state", {}).get("result_state")
        if result == "SUCCESS":
            # Assuming your job returns the recipes as JSON output
            output = fetch_databricks_output(job_id)
            if "recipes" in output:
                return render_template("results.html", recipes=output.get("recipes"))
            if "error" in output:
                return render_template("results.html", error=output.get("error"))
        else:
            error_message = run_status.get("state", {}).get(
                "message", "Job failed with no specific error."
            )
            return render_template("results.html", error=error_message)
    else:
        # Job is still running, or queued
        return "Job is still running. Please refresh the page.", 202


# Function to check job status in Databricks
def get_databricks_job_status(job_id):
    url = f"{DATABRICKS_URL}/api/2.0/jobs/runs/get?run_id={job_id}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers)
    return response.json()


def fetch_databricks_output(job_id):
    """
    Fetch the output from a Databricks job and extract relevant information.

    Args:
        job_id (int): The ID of the Databricks job to fetch.

    Returns:
        dict: A dictionary containing the assistant's message if successful,
              or an error message if there was a problem.
    """
    # Configuration: replace these with your actual Databricks token and URL
    databricks_token = DATABRICKS_TOKEN
    databricks_url = DATABRICKS_URL

    # Set up headers for the request
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    # Build the URL for the API request
    url = f"{databricks_url}/api/2.0/jobs/runs/get-output?run_id={job_id}"
    # Configuration: replace these with your actual Databricks token and URL
    databricks_token = DATABRICKS_TOKEN
    databricks_url = DATABRICKS_URL

    # Set up headers for the request
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    # Build the URL for the API request
    url = f"{databricks_url}/api/2.0/jobs/runs/get-output?run_id={job_id}"
    # Make the API request to Databricks
    response = requests.get(url, headers=headers)

    # Check for a successful response
    if response.status_code != 200:
        return {
            "error": f"Failed to fetch job output. Status code: {response.status_code}"
        }

    # Parse the response JSON
    try:
        output_data = response.json()
    except json.JSONDecodeError:
        return {"error": "Failed to parse the response as JSON."}

    # Extract notebook output if available
    notebook_output = output_data.get("notebook_output", {})
    if not notebook_output:
        return {"error": "No notebook output found in the response."}

    # Parse the result field in the notebook output
    notebook_result = notebook_output.get("result", "")
    if not notebook_result:
        return {"error": "No notebook result found in the notebook output."}

    # Attempt to parse the result string as JSON
    try:
        notebook_result = json.loads(notebook_result)
    except json.JSONDecodeError:
        return {"error": "Failed to parse the notebook result as JSON."}
    print(notebook_result)
    # Check if the result indicates success
    if notebook_result.get("status") != "success":
        error_message = notebook_result.get("message", "Unknown error occurred.")
        return {"error": f"Job failed with error: {error_message}"}

    # Extract the assistant's message from the OpenAI completion data
    try:
        completion_data = notebook_result.get("data", "{}")
        choices = completion_data.get("choices", [])
        if not choices:
            return {"error": "No choices found in the completion data."}
        assistant_message = choices[0].get("message", {}).get("content", "")
        return {"recipes": assistant_message}
    except (KeyError, TypeError, json.JSONDecodeError):
        return {
            "error": "Failed to extract the assistant's message from the completion data."
        }


if __name__ == "__main__":
    app.run(debug=True)
