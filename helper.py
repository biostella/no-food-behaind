import json

import requests
from azure.storage.blob import BlobServiceClient


def upload_to_azure_blob(
    blob_service_client, container_name, file, filename, blob_token
):
    """
    Uploads a file to Azure Blob Storage and returns the file's URL.
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=filename
    )
    blob_client.upload_blob(file, overwrite=True)

    # Construct Blob URL with the token
    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{filename}?{blob_token}"
    return blob_url


def run_databricks_notebook(databricks_url, databricks_token, job_id, blob_url):
    """
    Triggers a Databricks notebook run with the given parameters.
    """
    url = f"{databricks_url}/api/2.0/jobs/run-now"
    headers = {"Authorization": f"Bearer {databricks_token}"}
    data = {"job_id": job_id, "notebook_params": {"blob_url": blob_url}}

    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        return {"status": "error", "message": response.text}

    return response.json()


def get_databricks_job_status(databricks_url, databricks_token, job_id):
    """
    Polls Databricks for the status of a specific job run.
    """
    url = f"{databricks_url}/api/2.0/jobs/runs/get?run_id={job_id}"
    headers = {"Authorization": f"Bearer {databricks_token}"}
    response = requests.get(url, headers=headers)
    return response.json()


def fetch_databricks_output(databricks_url, databricks_token, job_id):
    """
    Fetches the output of a completed Databricks notebook job.
    """
    url = f"{databricks_url}/api/2.0/jobs/runs/get-output?run_id={job_id}"
    headers = {"Authorization": f"Bearer {databricks_token}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return {
            "error": f"Failed to fetch job output. Status code: {response.status_code}"
        }

    try:
        output_data = response.json()
    except json.JSONDecodeError:
        return {"error": "Failed to parse the response as JSON."}

    notebook_output = output_data.get("notebook_output", {})
    if not notebook_output:
        return {"error": "No notebook output found in the response."}

    notebook_result = notebook_output.get("result", "")
    if not notebook_result:
        return {"error": "No notebook result found in the notebook output."}

    try:
        notebook_result = json.loads(notebook_result)
    except json.JSONDecodeError:
        return {"error": "Failed to parse the notebook result as JSON."}

    if notebook_result.get("status") != "success":
        error_message = notebook_result.get("message", "Unknown error occurred.")
        return {"error": f"Job failed with error: {error_message}"}

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
