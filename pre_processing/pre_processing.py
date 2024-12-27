# Import libraries
import time
import pandas as pd
from pathlib import Path
import google.generativeai as genai
import json
import re
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Connect to Gemini API
"""
IMPORTANT: Replace `GEMINI_API_KEY` with your valid Gemini API key.
This is required to authenticate requests to the generative AI model.
"""
GEMINI_API_KEY = ''
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# MongoDB setup
"""
IMPORTANT: Replace the MongoDB URI, database name (`ClinicalNotesReviewer`), and collection name (`ProcessedReports`) 
with your actual setup. Ensure the collection name matches where you want the data to be stored.
"""
uri = ""
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['ClinicalNotesReviewer']
collection = db['processed_reports']

# Function to generate summary
def generate_summary(extracted_text):
    """
    Generates a structured summary of a radiology report based on extracted text.

    The summary includes three sections: 
    1. **Diseases Mentioned** - Identifies explicitly stated diseases and their elaborations.
    2. **Organs Mentioned** - Lists organs mentioned along with their conditions.
    3. **Symptoms/Phenomena of Concern** - Highlights key symptoms or phenomena of concern.

    The function uses a prompt to instruct the generative AI model on how to extract, classify, 
    and structure the content. It also ensures there is no duplication or inference in the output.

    Parameters:
        extracted_text (str): The raw extracted text from a radiology report.

    Returns:
        dict: A dictionary with structured sections containing the summarized information.
              Keys include "Diseases Mentioned", "Organs Mentioned", and 
              "Symptoms/Phenomena of Concern".
              If an error occurs, returns a dictionary with empty sections.
    """

    prompt = (
        "You are a world-class medical system knowledgeable in ICD-10-AM medical coding and specialized in analyzing and summarizing medical documents. \n"
        "The following text is extracted from a radiology report. \n"
        f"Text: {extracted_text}"
        "Firstly, determine and remember what type of image the text was extracted from. \n\n"
        "Secondly, I would like you to summarize the text extracted based on the following guiding questions:\n"
        "1. Any disease(s) mentioned in the radiology report? If yes, include all elaboration related to the disease(s). Name this section strictly as: **Diseases Mentioned:**, do not include numbering!\n"
        "    - Identify only medical diseases that are explicitly mentioned in the text extracted. Only focus on relevant diseases that are explicitly present in the extracted text. Do **NOT** infer any diseases that are not explicitly stated.\n"
        "    - According to National Institute of Health, a disease is an abnormal condition that affects the structure or function of part or all of the body and is usually associated with specific signs and symptoms.\n"
        "    - **Only consider diseases that have been diagnosed in the past or are part of the patient's medical history or previous reports.** The radiology report will generally focus on findings and observations, not definitive diagnoses.\n"
        "    - **Do not include symptoms** such as pleural effusion, atelectasis, consolidation, etc., as these are **symptoms** and not diseases.\n"
        "    - Hence, do not classify any symptoms under this section.\n"
        "    - If no disease name is mentioned, leave the section as * **NIL:** No diseases mentioned.\n"
        "    - If not, for each disease mentioned in the report, output the name of the disease in **bold** followed by a concise description of the disease as mentioned in the report.\n"
        "    - Example format: * **Disease Name:** Description of the disease.\n\n"
        "    - If there are more than 1 disease, output subsequent ones in a new line."

        "2. Any organ(s) mentioned in the radiology report? If yes, include all information regarding the organ(s). Name this section strictly as: **Organs Mentioned:**, do not include numbering!\n"
        "    - Identify only organs that are explicitly mentioned in the extracted text. Only focus on organs that are explicitly present in the extracted text. Do **NOT** infer any organs that are not explicitly stated.\n"
        "    - If no organ is mentioned, leave the section as * **NIL:** No organ mentioned.\n"
        "    - If not, for each organ mentioned, output the name of the organ in **bold** followed by the details related to the organ.\n"
        "    - Example format: * **Organ Name:** Description of the organ's condition.\n\n"
        "    - If there are more than 1 organ, output subsequent ones in a new line."

        "3. Any symptoms or phenomena that would cause attention? If yes, please elaborate on the concerns. Name this section strictly as **Symptoms/Phenomena of Concern:**, do not include numbering!\n"
        "    - Identify only medical symptoms that are explicitly mentioned in the extracted text. Only focus on relevant symptoms that are explicitly present in the extracted text. Do **NOT** infer any symptoms that are not explicitly stated.\n"
        "    - If no symptoms or phenomena are mentioned, leave the section as * **NIL:** No symptoms or phenomena mentioned.\n"
        "    - If not, for each symptom or phenomena of concern, output the main symptom or phenomenonin **bold** followed by any relevant details.\n"
        "    - Example format: * **Name of Symptom/Phenomenon:** Details related to the symptom or phenomenon.\n\n"
        "    - If there are more than 1 sypmtom/pehnomenon, output subsequent ones in a new line."

        "Thirdly, check for duplication or overlap between the sections:\n"
        "    - Ensure there are **no duplicated** entries between the 'Diseases Mentioned' and 'Symptoms/Phenomena of Concern' sections. If a condition or finding appears in both sections, classify it appropriately and move it to the correct section.\n"
        "    - **Symptoms and phenomena should not appear in the Diseases section**, and vice versa. For example, 'pleural effusion' is a finding, not a disease, and should appear in the Symptoms/Phenomena section.\n"
        "    - DO NOT output anything for this step as part of the language model response. Only provide response for the 3 sections defined above.\n\n"

        "Lastly, check that there are no entries that are suggested or implied by the language model:\n"
        "    - Use only words and information from the provided text. Do **NOT** suggest or imply anything. \n"
        "    - Remove any entries or elaborations containing the word forms suggest and implied.\n"
    )

    try:
        response = model.generate_content(prompt)

        summary_text = response.text.strip() if hasattr(response, 'text') and response.text else "Summary could not be generated."
        print(f"summary_text {summary_text}")

        headers = [
            "**Diseases Mentioned:**",
            "**Organs Mentioned:**",
            "**Symptoms/Phenomena of Concern:**"
        ]

        # Parse the summary into structured dictionaries
        summary_sections = {
            "Diseases Mentioned": {},
            "Organs Mentioned": {},
            "Symptoms/Phenomena of Concern": {}
        }

    
        def parse_section_to_dict(content):
            # Check if the content explicitly starts with "NIL" or is empty
            if "NIL" in content or not content.strip():
                return {}

            # Find all entries that follow the format * **Key:** Value
            matches = re.findall(r"\* \*\*(.+?):\s*\*?\*?\s*(.+?)(?=\n\* \*\*|\Z)", content, re.DOTALL)

            # Debug: print the matches found by the regex
            print("Debug: Matches found by regex:")
            for match in matches:
                print(f"Key: {match[0].strip()}, Value: {match[1].strip()}")

            # Return the dictionary with valid matches
            return {match[0].strip(): match[1].strip() for match in matches}


        # Extract and parse each section
        for i, header in enumerate(headers):
            start_idx = summary_text.find(header)
            if start_idx != -1:
                next_header_idx = (
                    summary_text.find(headers[i + 1], start_idx) if i + 1 < len(headers) else len(summary_text)
                )
                content = summary_text[start_idx + len(header):next_header_idx].strip()
                print(f"Content: {content}")
                section_name = header.strip("*").strip(":")
                print(f"section_name {section_name}")
                summary_sections[section_name] = parse_section_to_dict(content)

        return summary_sections

    except Exception as e:
        print(f"Error generating summary: {e}")
        return {
            "Diseases Mentioned": {},
            "Organs Mentioned": {},
            "Symptoms/Phenomena of Concern": {}
        }


# Function to generate layman explanation of the report
def generate_layman_explanation(extracted_text):
    """
    Generates a layman explanation of the content of a radiology report.

    The function translates the medical report content into simpler terms for non-medical audiences, 
    avoiding jargon and unnecessary technical details.

    Parameters:
        extracted_text (str): The raw extracted text from a radiology report.

    Returns:
        str: A concise layman explanation of the report. 
             If an error occurs, returns an error message or a default string indicating failure.
    """
    prompt = (
        "The following text is extracted from a radiology report."
        "You are an interpreter tasked to translate the radiology report 'Text' section into layman terms.\n"
        "Remember that your audience does not have any prior medical knowledge.\n"
        "Refrain from using medically intensive jargon.\n"
        "Do not include any extraneous information or explanations. Provide a complete, clear, and concise layman summary of the extracted content.\n"
        f"Text: {extracted_text}"
    )

    try:
        response = model.generate_content(prompt)
        
        # Safely access response content
        return response.text.strip() if hasattr(response, 'text') and response.text else "Layman explanation could not be generated."
    except Exception as e:
        print(f"Error generating layman explanation: {e}")
        return "Error generating layman explanation."

# Load the raw CSV file containing rows of radiology reports
"""
IMPORTANT: Replace 'Chest Scans_deidentified_test.csv' with the correct file path to your dataset.
Ensure the CSV file contains columns such as 'Masked_PatientID', 'Performed Date Time', and 'Text',
as these are used in the script for processing.
"""
df = pd.read_csv('Chest Scans_deidentified_test.csv')  # Replace with your test file name
print("read csv")

# Initialize a list to store the JSON output
json_output = []

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    # Extract PatientID and Updated Date
    patient_id = f"Patient{int(row['Masked_PatientID'])}"
    updated_date = row['Performed Date Time']

    # Store the raw report content
    """
    IMPORTANT: Ensure the column name for the text content in your CSV matches 'Text'.
    Modify `row['Text']` if your column name is different (e.g., 'Report Content').
    """
    report_text = row['Text']

    # Generate the layman explanation and summary
    layman_explanation = generate_layman_explanation(report_text)
    summary = generate_summary(report_text)

    # Build the JSON structure for the report
    report_data = {
        "PatientID": patient_id,
        "Performed Date Time": updated_date,
        "Raw Report": {col: str(row[col]) for col in df.columns},
        "Processed Data": {
            "Layman Explanation": layman_explanation,
            "Summary": summary
        }
    }

    # Append the report data to the JSON output list
    json_output.append(report_data)

    # Add 30-second wait between API calls
    print("Waiting for 30 seconds before next API call...")
    time.sleep(30)

try:
    result = collection.insert_many(json_output)
    print(f"Data successfully uploaded to MongoDB! Inserted IDs: {result.inserted_ids}")
except Exception as e:
    print(f"Error uploading data to MongoDB: {e}")
