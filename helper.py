import base64
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from dotenv import load_dotenv
from openai import OpenAI

ROOT = Path(os.path.abspath(os.getcwd()))

# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()

# Configurations loaded from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")
DBFS_PATH = os.getenv("DBFS_PATH")


def process_image(image_base64):
    """Process the image by encoding it and sending it to OpenAI API."""

    try:
        logging.info(f"PROMT - ready for GPT...")
        with open(ROOT / "prompt.txt", "r") as file:
            prompt = file.read()
            logging.info(f"PROMT - Personalised prompt used.")
    except Exception as e:
        logging.error(f"PROMT - Error reading prompt file: {e}")
        prompt = ""

    # Prepare the message content
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": prompt,
                },
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"},
                },
            ],
        }
    ]

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=1,
            max_tokens=2048,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        logging.info(f"API - Response: {response}")

        # Corrected access to the response object
        if hasattr(response, "choices") and len(response.choices) > 0:
            output = response.choices[0].message.content
        else:
            logging.error("No choices in the response")
            return {"status": "error", "data": "No choices in the response"}

        # Remove code block markers if present
        output = output.strip()
        output = re.sub(r"^```(?:json)?\n?", "", output)
        output = re.sub(r"\n?```$", "", output)

        # Parse JSON from output
        try:
            final_json = json.loads(output)
            return {"status": "success", "data": final_json}
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding error: {e}")
            return {"status": "error", "data": "Invalid JSON format"}

    except Exception as e:
        logging.error(f"Error processing image: {e}")
        return {"status": "error", "data": str(e)}


def upload_to_dbfs(image_name, encoded_image_data):
    """Upload an image as binary data to the Unity Catalog table, and return the base64-encoded data."""
    url = f"{DATABRICKS_URL}/api/2.0/sql/statements"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    # Create an SQL INSERT statement
    sql_query = f"""
    INSERT INTO workspace.default.uploaded_images_table (image_name, image_data)
    VALUES ('{image_name}', '{encoded_image_data}')
    """

    # Prepare the payload for the API request
    data = {
        "statement": sql_query,
        "warehouse_id": "fabaa7a0ba5a3fd2",
        "catalog": "workspace",
        "schema": "default",
    }

    try:
        logging.info(f"Inserting image {image_name} into the Unity Catalog table")

        # Make the API call to execute the SQL statement (slow process for large images)
        response = requests.post(url, headers=headers, json=data)

        # Log the response for debugging
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Response text: {response.text}")

        if response.status_code == 200:
            logging.info(f"Image {image_name} successfully uploaded to the table.")
            return True
        else:
            logging.error(
                f"Failed to upload image. Status Code: {response.status_code}"
            )
            logging.error(f"Response Text: {response.text}")
            return False

    except Exception as e:
        logging.error(f"Exception occurred during upload: {str(e)}")
        return False


def get_image_from_db(image_name):
    """Fetch the base64 encoded image data from the Unity Catalog table."""
    url = f"{DATABRICKS_URL}/api/2.0/sql/statements"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    logging.info(image_name)

    # SQL query to fetch image data for the given image name
    sql_query = f"""
    SELECT image_data
    FROM workspace.default.uploaded_images_table
    WHERE image_name = '{image_name}'
    """

    # Prepare the payload for the API request
    data = {
        "statement": sql_query,
        "warehouse_id": "fabaa7a0ba5a3fd2",
        "catalog": "workspace",
        "schema": "default",
    }

    logging.info(f"Fetching image data for {image_name} from the Unity Catalog table")

    try:
        # Make the API call to execute the SQL query
        response = requests.post(url, headers=headers, json=data)

        # Log the full response for debugging
        logging.info(f"Full response from Databricks: {response.json()}")
        result = response.json()

        # Check if the response contains the expected result
        if result.get("status", {}).get("state") == "SUCCEEDED" and result.get(
            "result", {}
        ).get("data_array"):
            # Extract the base64 encoded image data
            image_data = result.get("result", {}).get("data_array", [])[0][0]
            logging.info(f"Successfully fetched image data for {image_name}")
            return image_data
        else:
            logging.error(f"No data found for image: {image_name}")
            return None

    except Exception as e:
        logging.error(f"Exception occurred while fetching image data: {str(e)}")
        return None
