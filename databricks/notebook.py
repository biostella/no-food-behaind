from  openai import OpenAI
import requests
import json

blob_url = dbutils.widgets.get("blob_url")
openai_api_key = dbutils.widgets.get("openai_api_key")
PROMPT_FILE_PATH = "prompt.txt"

def read_txt_file(txt_path):
    # Open the file in read mode
    with open(txt_path, 'r') as file:
        # Read the contents of the file
        content = file.read()
    return content

prompt = read_txt_file(PROMPT_FILE_PATH)

client = OpenAI(
    # This is the default and can be omitted
    api_key=openai_api_key,
)

try:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"{blob_url}"},
                    },
                ],
            }
        ],
    )
    # Return the successful 
    output = completion.to_json()
    exit_output = {
        "status": "success",
        "data": json.loads(output)
    }
except Exception as e:
    # Capture the error and return it
    error_message = str(e)
    exit_output = {
        "status": "error",
        "data": error_message
    }

dbutils.notebook.exit(json.dumps(exit_output))