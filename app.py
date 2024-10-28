import base64
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

# Import helper functions
from .helper import process_image, upload_to_dbfs

# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()

# Flask app initialization
app = Flask(__name__)
executor = ThreadPoolExecutor()
tasks = {}


# Flask Routes
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_file():
    # Check if the post request has the file part
    if "file" not in request.files:
        return "No file part", 400
    file = request.files["file"]

    # If no file is selected
    if file.filename == "":
        return "No selected file", 400

    if file:
        filename = secure_filename(file.filename)
        logging.info(filename)

        # Read the file data once
        file_data = file.read()

        # Encode the image to base64 using the same data
        try:
            encoded_image = base64.b64encode(file_data).decode("utf-8")
            logging.info("IMAGE - Encoded Image Data Success.")
        except Exception as e:
            logging.error("IMAGE - Unable to encode image.")
            return "Failed to encode image", 500

        # Store the future task, encoded_image, and upload status
        tasks[filename] = {
            "future": executor.submit(process_image, encoded_image),
            "encoded_image": encoded_image,
            "upload_submitted": False,  # Initially, the upload hasn't been submitted
        }

        return redirect(url_for("results", filename=filename))


@app.route("/results/<filename>")
def results(filename):
    try:
        task_info = tasks.get(filename)
        if not task_info:
            return (
                render_template(
                    "results.html", error=f"Task for {filename} not found."
                ),
                404,
            )

        future = task_info["future"]
        encoded_image = task_info["encoded_image"]
        upload_submitted = task_info["upload_submitted"]

        if future.done():
            result = future.result()
            if result.get("status") == "success":
                completion_data = result.get("data", "{}")

                # Ensure completion_data is a dictionary
                if isinstance(completion_data, str):
                    completion_data = json.loads(completion_data)

                # Submit the upload task if not already submitted
                if not upload_submitted:
                    upload_future = executor.submit(
                        upload_to_dbfs, filename, encoded_image
                    )
                    task_info["upload_submitted"] = True  # Prevent resubmission
                    logging.info(f"SQL - File {filename} upload started in background.")

                # Render the results page immediately
                return render_template("results.html", recipes=completion_data)
            else:
                return render_template("results.html", error=result.get("data", "{}"))
        else:
            return render_template("loading.html"), 202

    except Exception as e:
        logging.exception("An error occurred in the results route.")
        return render_template("results.html", error=str(e)), 500


if __name__ == "__main__":
    app.run(debug=True)
