# Import libraries
import google.generativeai as genai
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dateutil import parser
import time

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
comparison_collection = db['comparison_gemini_sectioned_test']

# Adjust the date format in get_reports_by_patient function
def get_reports_by_patient():
    """
    Retrieve and group radiology reports by patient ID, parsing their performed date-time.

    Returns:
        dict: A dictionary where keys are patient IDs and values are lists of tuples containing
              the performed date-time (datetime object) and the respective report (dict).

    Notes:
        - Reports with unparsable dates are skipped with a logged error.
        - Date format assumed: "%d/%m/%Y %H:%M".
    """

    reports_by_patient = {}
    reports = collection.find({})

    for report in reports:
        try:
            # Parse using explicit date format to prevent issues
            performed_date_time = datetime.strptime(report['Performed Date Time'], "%d/%m/%Y %H:%M")
            patient_id = report['PatientID']

            if patient_id not in reports_by_patient:
                reports_by_patient[patient_id] = []
            reports_by_patient[patient_id].append((performed_date_time, report))
        except ValueError as e:
            print(f"Error parsing date for report {report['_id']}: {e}")
            continue

    return reports_by_patient

# Function to format radiology report
def format_radiology_report(report):
    """
    Format a radiology report for display and extract summarized processed data.

    Args:
        report (dict): A single radiology report containing raw and processed data.

    Returns:
        tuple: 
            - str: Formatted report string.
            - dict: Processed data summary with keys "Diseases Mentioned",
                    "Organs Mentioned", and "Symptoms/Phenomena of Concern".
    """
    formatted_report = (
        f"Patient ID: {report['Raw Report']['Masked_PatientID']}, Performed Date: {report['Performed Date Time']}\n\n"
        f"Raw Radiology Report Extracted\n"
        f"Text: {report['Raw Report']['Text'].strip()}\n\n"
    )

    processed_data = {
        "Diseases Mentioned": report['Processed Data']['Summary'].get('Diseases Mentioned', ""),
        "Organs Mentioned": report['Processed Data']['Summary'].get('Organs Mentioned', ""),
        "Symptoms/Phenomena of Concern": report['Processed Data']['Summary'].get('Symptoms/Phenomena of Concern', "")
    }

    return formatted_report, processed_data


def generate_comparison_prompt(section_name, section_content_1, section_content_2, date1_str, date2_str):
    """
    Generate a comparison prompt for analyzing differences between two sections
    of radiology reports.

    Args:
        section_name (str): Name of the section being compared.
        section_content_1 (dict): Content of the section in the newer report.
        section_content_2 (dict): Content of the section in the older report.
        date1_str (str): Date of the newer report as a string.
        date2_str (str): Date of the older report as a string.

    Returns:
        str: A detailed prompt guiding the comparison logic.
    """

    prompt = (
        f"You are comparing two radiology reports in the section '{section_name}', where the content is provided as key-value pairs.\n\n"
        
        f"### Structure of Input:\n"
        f"The input consists of observations represented as key-value pairs:\n"
        f"Example:\n"
        f"**Newer Report ({date1_str}):**\n"
        f"{{'Heart': 'Appears mildly enlarged.', 'Lungs': 'Minor atelectasis in the right lower zone and left paracardiac region. No consolidation or sizeable effusion.'}}\n\n"
        f"**Older Report ({date2_str}):**\n"
        f"{{'Heart': 'Normal size.', 'Lungs': 'Normal lungs with no effusion or consolidation.'}}\n\n"
        f"In this comparison, the input for the Newer Report is: {section_content_1}\n"
        f"and the input for the Older Report is: {section_content_2}\n\n"

        f"### Special Cases:\n"
        f"1. **Both Reports Are Empty:**\n"
        f"   - If there are no key-value pairs in both the Newer Report and Older Report, output *Both report sections are empty*.\n\n"
        f"2. **Newer Report is Empty, Older Report Has Key-Value Pairs:**\n"
        f"   - If the Newer Report contains no key-value pairs, but the Older Report does:\n"
        f"     - Categorize each key-value pair in the Older Report as **No Longer Mentioned**.\n"
        f"     - Use 'NIL' as the content for '{date1_str} Content'.\n"
        f"     - Use the values from the Older Report for '{date2_str} Content'.\n\n"

        f"### Comparison Instructions:\n"
        f"Follow the step-by-step logic to compare the key-value pairs from the two reports:\n\n"
        f"1. **Comparison Flow:**\n"
        f"   - Compare each key in the Newer Report ({date1_str}) against all keys in the Older Report ({date2_str}).\n"
        f"   - Treat keys that are phrased **similarly** or refer to the same concept as being the **same key**.\n\n"

        f"2. **Categorization Logic:**\n"
        f"   - **CASE 1: Difference**\n"
        f"     - If a key in the Newer Report matches a key in the Older Report (or is phrased similarly):\n"
        f"       - Use the value from the Newer Report as '{date1_str} Content'.\n"
        f"       - Use the value from the Older Report as '{date2_str} Content'.\n"
        f"       - If the value from the Newer Report is the same or similar to the value of the Older Report (e.g., both values are 'Enlarged'): Categorized as **Difference**.\n\n"
        f"       - Else, DO NOT categorise as **Difference** and move on to the next key-value pair in Newer Report.\n\n"
        f"   - **CASE 2: New Development**\n"
        f"     - If the key in the Newer Report is **not present** in the Older Report (and no similar key exists):\n"
        f"       - Use the value from the Newer Report as '{date1_str} Content'.\n"
        f"       - Use 'NIL' for '{date2_str} Content'.\n"
        f"       - Categorize as **New Development**.\n\n"
        f"   - **CASE 3: No Longer Mentioned**\n"
        f"     - After processing all keys from the Newer Report, check the Older Report for any unused key-value pairs.\n"
        f"       - For each unused key-value pair:\n"
        f"         - Use 'NIL' for '{date1_str} Content'.\n"
        f"         - Use the value from the Older Report as '{date2_str} Content'.\n"
        f"         - Categorize as **No Longer Mentioned**.\n\n"

        f"3. **Key Similarity Clarification:**\n"
        f"   - Keys that are similar in meaning or phrasing (e.g., 'Minor atelectasis' and 'Atelectasis (lung collapse)') must be treated as the **same key**.\n"
        f"   - These comparisons must always be categorized as **Difference** ONLY IF their values differ (e.g., both values are 'Enlarged').\n\n"

        f"4. **Output Format:**\n"
        f"Do not output irrelevant text like: Here's the comparison of the two radiology reports following the provided logic, output only the table."
        f"Present the comparison results in the following table format:\n"
        f"| Category            | {date1_str} Content                      | {date2_str} Content                      | Explanation                         |\n"
        f"|---------------------|------------------------------------------|------------------------------------------|-------------------------------------|\n"
        f"| Difference          | {{Newer Report Value}}                   | {{Older Report Value}}                   | Explanation of the difference.      |\n"
        f"| New Development     | {{Newer Report Value}}                   | NIL                                      | Explanation of new observation.     |\n"
        f"| No Longer Mentioned | NIL                                      | {{Older Report Value}}                   | Explanation of removal or absence.  |\n\n"
        
        f"### Important Rules:\n"
        f"1. Do **not** interpret or infer any information that is not explicitly stated in the provided key-value pairs.\n"
        f"2. Treat similar keys as the **same key** and strictly follow the comparison flow and categorization rules.\n"
        f"3. Use the exact wording from the reports for {date1_str} Content and {date2_str} Content.\n"
        f"4. Make sure there is no duplication of entries across categories.\n\n"
        f"Please proceed with the comparison following the above logic strictly."
    )
    return prompt

def compare_section(section_name, content1, content2, date1, date2):
    """
    Compare a specific section between two radiology reports.

    Args:
        section_name (str): Name of the section to compare.
        content1 (dict): Content of the section in the newer report.
        content2 (dict): Content of the section in the older report.
        date1 (datetime): Date of the newer report.
        date2 (datetime): Date of the older report.

    Returns:
        list: Structured comparison results as a list of dictionaries, each representing
              a comparison entry categorized by difference, new development, or no longer mentioned.
    """

    date1_str = date1.strftime("%d/%m/%Y %H:%M:%S")
    date2_str = date2.strftime("%d/%m/%Y %H:%M:%S")
    prompt = generate_comparison_prompt(section_name, content1, content2, date1_str, date2_str)

    retries = 3
    for attempt in range(retries):
        try:
            response = model.generate_content(prompt)
            print(f"AI Response for '{section_name}': {response.text}")
            if hasattr(response, 'text') and response.text:
                comparison_output = response.text.strip()
                
                # Parse the response into structured format
                def parse_comparison_headers(comparison_string):
                    lines = comparison_string.strip().split("\n")
                    headers = [header.strip() for header in lines[0].split("|")[1:-1]]
                    comparison_list = []
                    
                    for line in lines[2:]:
                        parts = line.split("|")
                        comparison_entry = {headers[i]: parts[i+1].strip() for i in range(len(headers))}
                        comparison_list.append(comparison_entry)
                    
                    print(f"comparison_list: {comparison_list}")
                    return comparison_list

                structured_comparison = parse_comparison_headers(comparison_output)
                return structured_comparison
            else:
                return []
        except Exception as e:
            if "429" in str(e):
                print(f"API quota exceeded. Retrying in 30 seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(30)
            else:
                print(f"Error generating comparison for section '{section_name}': {e}")
                return []
    return []

# Function to compare multiple reports
def compare_multiple_reports(reports):
    """
    Compare multiple radiology reports for a patient, generating structured comparisons
    across sections.

    Args:
        reports (list): A list of tuples, each containing a datetime object and a report dictionary.

    Returns:
        list: A list of structured comparisons across all sections for all report pairs.
    """

    reports.sort(key=lambda x: x[0], reverse=True)
    base_report = reports[0]
    all_comparisons = []

    for i in range(1, len(reports)):
        report = reports[i]
        base_text, base_sections = format_radiology_report(base_report[1])
        report_text, report_sections = format_radiology_report(report[1])

        # loop through comparison prompt for each section
        for section_name in base_sections.keys():
            # Debug prints
            print(f"Base section ({section_name}): {base_sections.get(section_name)}")
            print(f"Report section ({section_name}): {report_sections.get(section_name)}")

            comparison_result = compare_section(
                section_name,
                base_sections[section_name],
                report_sections[section_name],
                base_report[0],
                report[0]
            )
            # Add the Section attribute to each comparison result and ensure it appears first
            for comparison in comparison_result:
                comparison = {
                    "Section": section_name, 
                    **comparison,
                    'New Report Date': base_report[0],
                    'Old Report Date': report[0],
                    'New Report Order ID': base_report[1]['Raw Report']['Order ID'],  
                    'Old Report Order ID': report[1]['Raw Report']['Order ID'],       
                    'New Report Order Name': base_report[1]['Raw Report']['Order Name'],
                    'Old Report Order Name': report[1]['Raw Report']['Order Name'], }
                
                all_comparisons.append(comparison)

    return all_comparisons

def parse_comparison_result(comparison_results):
    """
    Parse and aggregate comparison results by date pairs and sections.

    Args:
        comparison_results (list): A list of comparison results with associated dates and sections.

    Returns:
        dict: Aggregated comparison results organized by date pairs and section names.
    """

    # Define the sections you're interested in
    sections = {
        'Diseases Mentioned': 'Diseases Mentioned',
        'Organs Mentioned': 'Organs Mentioned',
        'Symptoms/Phenomena of Concern': 'Symptoms/Phenomena of Concern'
    }

    # Initialize an aggregated data structure to hold results by date pairs and section
    aggregated_data = {}

    for comparison_result in comparison_results:
        # comparison_text = comparison_result['comparison_result']
        date1_str = comparison_result['New Report Date'].strftime("%d/%m/%Y %H:%M:%S")
        date2_str = comparison_result['Old Report Date'].strftime("%d/%m/%Y %H:%M:%S")

        # Initialize the date pair structure if it doesn't exist
        if (date1_str, date2_str) not in aggregated_data:
            aggregated_data[(date1_str, date2_str)] = {section: [] for section in sections.keys()}

        aggregated_data[(date1_str, date2_str)][comparison_result['Section']].append({
            "Category": comparison_result['Category'],
            "NewContent": comparison_result[f"{date1_str} Content"],
            "OldContent": comparison_result[f"{date2_str} Content"],
            "Explanation": comparison_result['Explanation']
        })
   
    # Return the aggregated data in the desired structure
    print(f"aggregated_data: {aggregated_data}")
    return aggregated_data

# Save comparisons to MongoDB
def save_comparisons(patient_id, report_dates, comparison_result):
    """
    Save comparison results to a MongoDB collection.

    Args:
        patient_id (str): ID of the patient.
        report_dates (list): List of report dates in datetime format.
        comparisons (list): Structured comparison results.

    Returns:
        None
    """
    
    parsed_data = parse_comparison_result(comparison_result)

    json_output = {
        "PatientID": patient_id,
        "ReportDates": [date.strftime("%d/%m/%Y %H:%M:%S") for date in report_dates],
        "Comparisons": []
    }

    # Convert parsed_data to the required JSON structure
    for comparison in comparison_result:
        date1_str = comparison['New Report Date'].strftime("%d/%m/%Y %H:%M:%S")
        date2_str = comparison['Old Report Date'].strftime("%d/%m/%Y %H:%M:%S")
        order_id1 = comparison['New Report Order ID'] 
        order_id2 = comparison['Old Report Order ID'] 
        order_name1 = comparison['New Report Order Name']
        order_name2 = comparison['Old Report Order Name']

        # Skip if the old report date (date2_str) already exists in the Comparisons
        if any(entry['Old Report Date'] == date2_str for entry in json_output["Comparisons"]):
            print(f"Skipping comparison for Old Report Date: {date2_str} (already exists).")
            continue

        date_pair_entry = {
            "New Report Date": date1_str,
            "New Report Order ID": order_id1,   # Add Order ID for date1
            "New Report Order Name": order_name1,
            "Old Report Date": date2_str,
            "Old Report Order ID": order_id2,   # Add Order ID for date2
            "Old Report Order Name": order_name2,
            "Sections": {}
        }

        # Add section comparisons
        for section, comparisons in parsed_data[(date1_str, date2_str)].items():
            date_pair_entry["Sections"][section] = comparisons

        json_output["Comparisons"].append(date_pair_entry)

    # Use upsert to replace or insert document in MongoDB
    comparison_collection.update_one(
        {"PatientID": patient_id},
        {"$set": json_output},
        upsert=True
    )
    print(f"Comparison for PatientID {patient_id} saved to MongoDB (replaced if existing).")

def main():
    """
    Main function to retrieve, process, and save comparisons of radiology reports by patient.

    Workflow:
        - Retrieve grouped reports by patient.
        - Generate comparisons for multiple reports.
        - Save comparison results to MongoDB.
    """
    
    reports_by_patient = get_reports_by_patient()

    for patient_id, reports in reports_by_patient.items():
        # Sort reports by performed date time in ascending order
        reports.sort(key=lambda x: x[0])  # Sort by datetime

        # Consider cases whereby there is only 1 report for the patient
        if len(reports) == 1:
            print(f"Only one report available for PatientID {patient_id}. No comparison will be generated.")

            # Create a simple JSON output for patients with only one report
            single_report_output = {
                "PatientID": patient_id,
                "Comparison": "No comparison available as there is only one report for this patient."
            }

            # Save to MongoDB
            comparison_collection.update_one(
                {"PatientID": patient_id}, 
                {"$set": single_report_output},
                upsert=True
            )
            continue

        # If there are more than 5 reports, only keep the latest 5
        if len(reports) > 5:
            reports = reports[-5:]

        # Get the latest report date in the current reports list
        latest_report_date = reports[-1][0]

        # Check if there's already a comparison output for this patient
        existing_comparison = comparison_collection.find_one({"PatientID": patient_id})

        if existing_comparison:
            # Convert dates from existing comparison to datetime objects for comparison
            existing_dates = [parser.parse(date_str) for date_str in existing_comparison.get("ReportDates", [])]

            # Find the latest date in the existing comparison output
            latest_existing_date = max(existing_dates) if existing_dates else None

            # Check if there's a new report by comparing dates
            if latest_existing_date and latest_existing_date >= latest_report_date:
                print(f"No new reports for PatientID {patient_id}. Skipping comparison.")
                continue

        # Perform comparison since either there’s no existing comparison or new reports are present
        comparison_results = compare_multiple_reports(reports)

        # Collect report dates for saving in the JSON output
        report_dates = [report[0] for report in reports]

        # Save comparison output to MongoDB
        save_comparisons(patient_id, report_dates, comparison_results)

if __name__ == "__main__":
    main()
