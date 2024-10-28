
# No-Food-BehAInd

Repository containing the project submitted for the Generative AI World Cup 2024.

This Flask-based web application allows users to upload food images, which are then processed using Databricks jobs triggered via API. The app stores uploaded images in Azure Blob Storage and returns recipe suggestions based on the food in the image using a generative AI model.

## Table of Contents
- [Features](#features)
- [Requirements](#requirements)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Environment Variables](#environment-variables)
- [File Structure](#file-structure)
- [License](#license)

## Features
- Upload images of food.
- Images are stored in Azure Blob Storage.
- Databricks job is triggered via API to process the image.
- Generates recipe suggestions based on the uploaded food image.
  
## Requirements
This project requires the following to be installed:
- Python 3.12
- Flask
- Azure SDK for Python
- Databricks API access

## Setup Instructions

Follow these steps to set up the project:

1. Clone the repository:
   ```bash
   git clone https://github.com/biostella/no-food-behaind.git
   cd no-food-behaind
   ```

2. Create a Python virtual environment:
   ```bash
   python3 -m venv .venv
   ```

3. Activate the virtual environment:

   On macOS/Linux:
   ```bash
   source .venv/bin/activate
   ```

   On Windows:
   ```bash
   .venv\Scripts\activate
   ```

4. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Set up the environment variables by creating a `.env` file in the root of the project. See the [Environment Variables](#environment-variables) section for more details.

6. Run the Flask application:
   ```bash
   flask run
   ```

The application will be running on `http://127.0.0.1:5000/`.

## Usage

1. Visit the homepage at `http://127.0.0.1:5000/`.
2. Upload a food image through the provided form.
3. The image is stored in Azure Blob Storage, and a Databricks job is triggered.
4. Wait for the job to complete and view the generated recipe suggestions on the results page.

## Environment Variables

You will need to create a `.env` file in the root of the project with the following variables:

```ini
# Databricks settings
DATABRICKS_TOKEN = your-databricks-token
DATABRICKS_URL = https://your-databricks-instance-url
DBFS_PATH = your-database-path
OPENAI_API_KEY = your-api-key-for-model

```

Make sure to replace the placeholder values with your actual Databricks and Azure credentials.

## File Structure

Here is an overview of the project's file structure:

```
.
├── app.py                  # The main Flask app
├── helper.py               # Helper functions for Azure and Databricks interactions
├── prompt.py               # AI prompt
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not committed to version control)
├── static/
│   └── uploads/            # Directory for storing uploaded files temporarily
├── templates/
│   ├── index.html          # Homepage template
│   ├── loading.html        # Loading template
│   └── results.html        # Results page template
└── README.md               # Project documentation (this file)
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
