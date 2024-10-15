import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from helper import (
    fetch_databricks_output,
    get_databricks_job_status,
    run_databricks_notebook,
    upload_to_azure_blob,
)

load_dotenv()

# Configurations loaded from environment variables
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
BLOB_TOKEN = os.getenv("BLOB_TOKEN")

# Flask app initialization
app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = "static/uploads/"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


# Route for homepage
@app.route("/")
def index():
    return render_template("index.html")


# Route to handle image upload
@app.route("/upload", methods=["POST"])
def upload_file():
    # Check if the post request has the file part
    if "file" not in request.files:
        return "No file part", 400
    file = request.files["file"]

    # If no file is selected
    if file.filename == "":
        return "No selected file", 400

    # Secure and validate the uploaded file
    if file:
        filename = secure_filename(file.filename)

        # Upload the file to Azure Blob Storage
        blob_url = upload_to_azure_blob(
            blob_service_client, AZURE_CONTAINER_NAME, file, filename, BLOB_TOKEN
        )

        # Trigger Databricks job with the image URL
        job_response = run_databricks_notebook(
            DATABRICKS_URL, DATABRICKS_TOKEN, DATABRICKS_JOB_ID, blob_url
        )
        job_id = job_response.get("run_id")

        # Redirect to results page with job ID
        return redirect(url_for("results", job_id=job_id))


# Route to check and display results
@app.route("/results/<job_id>")
def results(job_id):
    # Poll Databricks for job status
    run_status = get_databricks_job_status(DATABRICKS_URL, DATABRICKS_TOKEN, job_id)

    if run_status.get("state", {}).get("life_cycle_state") == "TERMINATED":
        result = run_status.get("state", {}).get("result_state")

        if result == "SUCCESS":
            # Fetch job results
            output = fetch_databricks_output(DATABRICKS_URL, DATABRICKS_TOKEN, job_id)
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
        # Job still running
        return "Job is still running. Please refresh the page.", 202


if __name__ == "__main__":
    app.run(debug=True)
