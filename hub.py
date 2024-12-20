import os
import requests
import json
import pandas as pd
from datetime import datetime
import re
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)

# Fetch HubSpot Access Token from environment variable
SERVICE_UPDATE_INTEGRATION = os.getenv('SERVICE_UPDATE_INTEGRATION')

if not SERVICE_UPDATE_INTEGRATION:
    raise Exception("SERVICE_UPDATE_INTEGRATION environment variable is not set")

# Headers for HubSpot API requests with Bearer token
HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {SERVICE_UPDATE_INTEGRATION}",
    "Content-Type": "application/json"
}

# Load enriched data from JSON file
def load_enriched_data(filename=None):
    filename = filename or os.getenv('ENRICHED_DATA_FILE', 'enriched_premises_data.json')
    try:
        with open(filename, 'r') as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        logging.error(f"Enriched data file '{filename}' not found.")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from '{filename}': {e}")
        return []

# Load sales rep data from CSV file
def load_sales_rep_data(filename=None):
    filename = filename or os.getenv('SALES_REP_DATA_FILE', 'id.csv')
    try:
        return pd.read_csv(filename)
    except FileNotFoundError:
        logging.error(f"Sales rep data file '{filename}' not found.")
        return pd.DataFrame()
    except pd.errors.EmptyDataError as e:
        logging.error(f"Error reading CSV from '{filename}': {e}")
        return pd.DataFrame()

# Load ticket types data from JSON file
def load_ticket_types(filename=None):
    filename = filename or os.getenv('TICKET_TYPES_FILE', 'ticket_types.json')
    try:
        with open(filename, 'r') as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        logging.error(f"Ticket types file '{filename}' not found.")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from '{filename}': {e}")
        return {}

# Helper function to format dates to YYYY-MM-DD
def format_date(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and format to YYYY-MM-DD
            return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Helper function to convert date to Unix timestamp (milliseconds)
def format_date_to_timestamp(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and get Unix timestamp in milliseconds
            return int(datetime.fromisoformat(date_str).timestamp() * 1000)
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Define installation and service pipeline stages
# Define installation and service pipeline stages
installation_pipeline_stages = {
    "Rejection": 2,
    "closed - rejection - duplication": 2,
    "Closed - rejection - duplication": 2,
    "closed - rejected": 2,
    "Fiber Ready": 3,
    "Active Refusal": 4,
    "SALES - Active Refusal": 4,
    "Passive Refusal": 258799956,
    "Pre Order": 258799957,
    "New Order": 258799958,
    "NID Relocate": 258799960,
    "Civil Drop": 258799961,
    "Optical Drop": 258799962,
    "Soft Blockage": 258799963,
    "Hard Blockage": 258799964,
    "NCCH": 258799965,
    "Full Handover": 258799966,
    "NID Installation Complete": 258799967,
    "ISP Scheduled": 258799968,
    "ISP Complete": 258799969,
    "Pending Auto Configuration": 258799970,
    "pending configuration": 258799970,
    "Auto Configuration Failed": 258799971,
    "Activation Complete": 258799972,
    "Not Actionable": 258799973,
    "Installation": 258799974,
    "Provisioning": 267644843,
    "provisioning failed": 267644843,
    "Provisioned": 267644843,
    "Other": 267644850,
    "NID Installation": 267644851,
    "closed - nid - installation complete": 267644851,
    "Service Activation (without installation)": 267644856,
    "L3 Configuration": 267644930,
    "configured": 267644930,
    "Relocation": 267644931,
    "Abandoned": 954945896
}

service_pipeline_stages = {
    "Cancellation": 267644932,
    "Cancellation in Progress": 267644932,
    "Cancellation pending": 267644932,
    "Cancellation Pending": 267644932,
    "cancelled": 267644932,
    "Cancelled": 267644932,
    "Change Service": 267644933,
    "Service change": 267644933,
    "service change approved": 954945906,
    "Service Change Approved": 954945906,
    "Change Service": 267644933,
    "Fiber Break": 267644934,
    "Service Down": 267644935,
    "Light Levels": 267647763,
    "Power Down": 267647764,
    "Maintenance": 267647765,
    "Swapout Device": 267647766,
    "Recover Device": 267647767,
    "Deprovisioning": 267647768,
    "Speed Test": 267647769,
    "Change Service Provider": 267647770,
    "Fault": 267647771,
    "service change approved": 954945906,
    "Change Service": 954945906,
    "rejected": 955026021,
    "deprovisioned": 954733986
}

# Create or update a contact in HubSpot and return the contact ID
def create_or_update_contact_in_hubspot(premise, customer, sales_rep_data):
    if not premise or not customer:
        logging.warning("Premise or customer data is None, skipping this premise.")
        return

    # Extract updated_at from the nested structure
    services = premise.get('services', [])
    service_status_date = None  # Default to None if no date found

    full_service_premise = {}
    for service in services:
        # Ensure service_details is not None
        service_details = service.get('service_details', None)
        if service_details is None:
            logging.warning(f"Missing service_details for premise {premise.get('id', 'Unknown ID')}. Skipping.")
            continue

        full_service = service_details.get('full_service', {})
        full_service_premise = full_service.get('premise', {})
        service_metadata = full_service.get('service', {})

        # Get the updated_at field if it exists
        updated_at = service_metadata.get('updated_at')
        if updated_at:
            # Convert to Unix timestamp in milliseconds
            service_status_date = format_date_to_unix(updated_at)
            break  # Use the first valid updated_at found

    # Extract sales_rep_id from premise
    sales_rep_id = premise.get('sales_channel_id')

    if pd.notna(sales_rep_id):
        matching_rows = sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text']
        sales_rep = matching_rows.iloc[0] if not matching_rows.empty else 'No Sales Agent Selected'
    else:
        sales_rep = 'No Sales Agent Selected'

    # Prepare contact data
    contact_data = {
        "properties": {
            "firstname": customer.get('first_name', ''),
            "lastname": customer.get('last_name', ''),
            "email": customer.get('email', ''),
            "phone": customer.get('mobile_number', ''),
            "address": f"{full_service_premise.get('street_number', '')} {full_service_premise.get('street_name', '')}",
            "city": full_service_premise.get('city', ''),
            "state": full_service_premise.get('province', ''),
            "zip": full_service_premise.get('postal_code', ''),
            "aex_id": premise.get('premise_id', ''),
            "latitude": full_service_premise.get('lat', ''),
            "longitude": full_service_premise.get('lon', ''),
            "service_status_date": service_status_date,  # Add the Unix timestamp
            "sales_rep": sales_rep,
            "sales_rep_id": sales_rep_id,
            "service_status": premise.get('status', '')
        }
    }

    email = customer.get('email', '')
    aex_id = premise.get('premise_id', '')
    existing_contact_id = find_existing_contact_by_email_or_aex_id(email, aex_id)

    if existing_contact_id:
        # Update the existing contact
        update_contact(existing_contact_id, contact_data)
        return existing_contact_id
    else:
        # Create a new contact
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        response = requests.post(url, headers=HUBSPOT_HEADERS, json=contact_data)

        if response.status_code in (200, 201):
            logging.info(f"Contact created successfully for AEX ID: {aex_id}")
            return response.json().get('id')
        else:
            logging.error(f"Error creating contact: {response.text}")
            return None

# Helper function to format dates to YYYY-MM-DD
def format_date(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and format to YYYY-MM-DD
            return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Helper function to convert date to Unix timestamp (milliseconds)
def format_date_to_unix(date_str, in_milliseconds=True):
    if date_str:
        try:
            # Parse the date string with timezone info
            dt = datetime.fromisoformat(date_str)
            # Convert to Unix timestamp
            unix_timestamp = dt.timestamp()
            # Convert to milliseconds if required
            return int(unix_timestamp * 1000) if in_milliseconds else int(unix_timestamp)
        except ValueError:
            logging.error(f"Invalid date format: {date_str}")
            return None  # Return None for invalid date formats
    return None

# Update an existing contact by ID
def update_contact(contact_id, contact_data):
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    response = requests.patch(url, headers=HUBSPOT_HEADERS, json=contact_data)

    if response.status_code == 200:
        logging.info(f"Contact {contact_id} updated successfully.")
    else:
        logging.error(f"Error updating contact {contact_id}: {response.text}")
        if response.status_code == 409:  # Conflict: Contact already exists
            existing_contact_id = extract_existing_contact_id(response.text)
            if existing_contact_id and existing_contact_id != contact_id:
                logging.info(f"Conflict detected. Retrying update with existing contact ID: {existing_contact_id}")
                update_contact(existing_contact_id, contact_data)

# Extract the existing contact ID from the conflict error message
def extract_existing_contact_id(error_message):
    match = re.search(r"Existing ID: (\d+)", error_message)
    if match:
        return match.group(1)
    return None

# Search for an existing contact by email or AEX ID
def find_existing_contact_by_email_or_aex_id(email, aex_id):
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "aex_id",
                        "operator": "EQ",
                        "value": aex_id
                    }
                ]
            }
        ]
    }
    
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=query)
    
    if response.status_code == 200:
        try:
            data = response.json()
            if data.get('results'):
                return data['results'][0].get('id')  # Return the existing contact ID
        except ValueError:
            logging.error(f"Invalid JSON response: {response.text}")
    else:
        logging.error(f"Error finding contact in HubSpot by email or AEX ID: {response.text}")
    return None

# Create or update tickets in HubSpot for a contact
def create_or_update_tickets_for_contact(contact_id, work_order, ticket_types, premise, customer, service, sales_rep_data):
    if not work_order:
        logging.warning("Work order data is None, skipping ticket creation.")
        return

    try:
        # Extract product and sales rep
        product = (
            premise.get('services', [{}])[0]
            .get('service_details', {})
            .get('full_service', {})
            .get('isp_product', {})
            .get('name', 'Unknown Product')
        )
        sales_rep_id = premise.get('sales_channel_id')
        sales_rep = (
            sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text']
            .iloc[0]
            if pd.notna(sales_rep_id) and not sales_rep_data.empty else 'No Sales Agent Selected'
        )

        # Extract key data
        work_order_id = work_order.get('id', '')
        service_id = premise.get('id', '')
        status = premise.get('status', 'Unknown Status')
        work_order_status = work_order.get('status', '').strip()
        full_service_premise = premise.get('services', [{}])[0].get('service_details', {}).get('full_service', {}).get('premise', {})
        street_number = full_service_premise.get('street_number', '')
        street_name = full_service_premise.get('street_name', '')
        subject = f"{street_number} {street_name} - {work_order_status}"

        # Define pipeline and stage mappings
        pipeline_id, pipeline_stage_id = None, None
        lower_case_pipeline_stages = {k.lower(): v for k, v in installation_pipeline_stages.items()}
        work_order_status_lower = work_order_status.lower()
        if work_order_status_lower in lower_case_pipeline_stages:
            pipeline_id = "0"  # Example pipeline ID for installation
            pipeline_stage_id = lower_case_pipeline_stages[work_order_status_lower]
        elif work_order_status_lower in ["service change", "cancellation", "service change approved", "cancellation pending"]:
            pipeline_id = "160077657"  # Service pipeline ID
            pipeline_stage_id = service_pipeline_stages.get(work_order_status_lower, None)
        else:
            logging.error(f"Unknown work order status: '{work_order_status}'. Skipping ticket creation.")
            return

        # Check for existing ticket
        existing_ticket_id = find_existing_ticket_by_work_order_id(work_order_id)

        # Prepare ticket data
        ticket_data = {
            "properties": {
                "subject": subject,
                "content": work_order.get('description', 'No Description Provided'),
                "hs_pipeline": pipeline_id,
                "hs_pipeline_stage": pipeline_stage_id,
                "aex_work_order_id": work_order_id,
                "work_order_id1": work_order_id,
                "hubspot_owner_id": None,
                "premise_id": premise.get('premise_id', ''),
                "customer_id": customer.get('id', ''),
                "createdate": format_date_to_timestamp(work_order.get('created_at', '')),
                "sales_rep": sales_rep,
                "sales_rep_id": sales_rep_id,
                "service_status": status,
                "schedule_date": format_date_to_timestamp(work_order.get('schedule_date', '')),
                "closed_date": format_date_to_timestamp(work_order.get('completed_date', '')),
                "service_id": service_id,
                "product": product
            },
            "associations": [
                {
                    "to": {
                        "id": contact_id
                    },
                    "types": [
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": 81  # Ticket-to-contact association type ID
                        }
                    ]
                }
            ]
        }

        # Create or update ticket
        if existing_ticket_id:
            logging.info(f"Ticket already exists for work order {work_order_id}. Updating existing ticket.")
            update_ticket(existing_ticket_id, work_order, premise, customer, service, sales_rep_data)
        else:
            url = "https://api.hubapi.com/crm/v3/objects/tickets"
            response = requests.post(url, headers=HUBSPOT_HEADERS, json=ticket_data)
            if response.status_code in (200, 201):
                logging.info(f"Ticket created successfully for work order {work_order_id} and contact {contact_id}")
            else:
                logging.error(f"Error creating ticket for work order {work_order_id}: {response.text}")

    except Exception as e:
        logging.error(f"An error occurred during ticket creation: {e}")

def find_existing_ticket_by_work_order_id(work_order_id):
    """Checks if a ticket with the given `aex_work_order_id` already exists."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/search"
    search_data = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "work_order_id1",
                        "operator": "EQ",
                        "value": work_order_id
                    }
                ]
            }
        ],
        "properties": ["hs_object_id"]
    }
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=search_data)

    if response.status_code == 200:
        data = response.json()
        if data.get("total", 0) > 0:
            return data["results"][0]["id"]
    return None

# Update an existing ticket by ID
def update_ticket(ticket_id, work_order, premise, customer, service, sales_rep_data):
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    work_order_id = work_order.get('id', '') if work_order else ''

    # Extract address from full_service_premise
    full_service_premise = {}
    services = premise.get('services', [])
    if services:
        service_details = services[0].get('service_details', {})
        full_service = service_details.get('full_service', {})
        full_service_premise = full_service.get('premise', {})

    street_number = full_service_premise.get('street_number', '')
    street_name = full_service_premise.get('street_name', '')

    street_address = f"{street_number} {street_name}"

    # Handle services as a list
    services = premise.get('services', [])
    product = ''
    for service_entry in services:
        service_details = service_entry.get('service_details', {})
        full_service = service_details.get('full_service', {})
        isp_product = full_service.get('isp_product', {})
        product = isp_product.get('name', '')
        if product:  # Break if a product name is found
            break

    # Extract sales_rep_id from premise
    sales_rep_id = premise.get('sales_channel_id')
    status = premise.get('status', '')
    if pd.notna(sales_rep_id):
        matching_rows = sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text']
        sales_rep = matching_rows.iloc[0] if not matching_rows.empty else ''
    else:
        sales_rep = ''
    # Define pipeline and stage mappings
    pipeline_id, pipeline_stage_id = None, None
    lower_case_pipeline_stages = {k.lower(): v for k, v in installation_pipeline_stages.items()}
    work_order_status_lower = work_order_status.lower()
    if work_order_status_lower in lower_case_pipeline_stages:
        pipeline_id = "0"  # Example pipeline ID for installation
        pipeline_stage_id = lower_case_pipeline_stages[work_order_status_lower]
    elif work_order_status_lower in ["service change", "cancellation", "service change approved", "cancellation pending"]:
        pipeline_id = "160077657"  # Service pipeline ID
        pipeline_stage_id = service_pipeline_stages.get(work_order_status_lower, None)
    else:
        logging.error(f"Unknown work order status: '{work_order_status}'. Skipping ticket creation.")
        return
            
    ticket_data = {
        "properties": {
            "subject": f"{street_address} - {work_order.get('status', '').strip()}",
            "content": work_order.get('description', ''),
            "hs_pipeline_stage": pipeline_stage_id,
            "work_order_id1": work_order_id,
            "hubspot_owner_id": None,
            "premise_id": premise.get('premise_id', ''),
            "customer_id": customer.get('id', ''),
            "createdate": format_date_to_timestamp(work_order.get('created_at', '')),
            "aex_create_date": format_date_to_timestamp(work_order.get('created_at', '')),
            "sales_rep": sales_rep,
            "sales_rep_id": sales_rep_id,
            "service_status": status,
            "schedule_date": format_date_to_timestamp(work_order.get('schedule_date', '')) if work_order else '',
            "closed_date": format_date_to_timestamp(work_order.get('completed_date', '')) if work_order else '',
            "service_id": premise.get('id', ''),
            "product": product 
        }
    }

    # Log the ticket data being sent
    logging.info(f"Updating Ticket Data: {json.dumps(ticket_data, indent=2)}")

    response = requests.patch(url, headers=HUBSPOT_HEADERS, json=ticket_data)

    if response.status_code == 200:
        logging.info(f"Ticket {ticket_id} updated successfully.")
    else:
        logging.error(f"Error updating ticket {ticket_id}: {response.text}")

# Search for an existing ticket by work_order_id, premise_id, and contact_id
def find_existing_ticket_by_work_order_and_contact(work_order_id, premise_id, contact_id):
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "work_order_id1",  # Ensure this matches the exact custom property name in HubSpot
                        "operator": "EQ",
                        "value": work_order_id
                    }
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "premise_id",
                        "operator": "EQ",
                        "value": premise_id
                    },
                    {
                        "propertyName": "associations.contact",
                        "operator": "EQ",
                        "value": contact_id
                    }
                ]
            }
        ]
    }

    logging.info(f"Searching for existing ticket with work_order_id: {work_order_id}, premise_id: {premise_id}, contact_id: {contact_id}")
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=query)

    if response.status_code == 200:
        try:
            data = response.json()
            logging.info(f"Search response data: {json.dumps(data, indent=2)}")  # Log the response data for debugging
            if data.get('results'):
                ticket_id = data['results'][0].get('id')
                logging.info(f"Found existing ticket with ID: {ticket_id} for work order ID: {work_order_id}")
                return ticket_id  # Return the existing ticket ID
        except ValueError:
            logging.error(f"Invalid JSON response: {response.text}")
    else:
        logging.error(f"Error finding ticket in HubSpot by work_order_id, premise ID, and contact ID: {response.text}")

    logging.info("No existing ticket found. Proceeding with ticket creation.")
    return None

# Process premises data and create or update contacts and tickets in HubSpot for multiple work orders
def process_premises_for_hubspot():
    premises_data = load_enriched_data()
    sales_rep_data = load_sales_rep_data()
    ticket_types = load_ticket_types()

    for premise in premises_data:
        if not premise:
            logging.warning("Premise data is None, skipping this premise.")
            continue

        logging.debug(f"Processing premise: {json.dumps(premise, indent=2)}")

        customer = premise.get('customer')
        if customer is None:
            logging.warning("Customer data is missing, skipping this premise.")
            continue

        # Retrieve service_id directly from the premise
        service_id = premise.get('id')
        if not service_id:
            logging.warning("Service ID is missing, skipping this premise.")
            continue

        contact_id = create_or_update_contact_in_hubspot(premise, customer, sales_rep_data)

        if contact_id:
            services = premise.get('services', [])
            if not isinstance(services, list):
                logging.error(f"Expected 'services' to be a list, but got {type(services)}. Skipping premise.")
                continue

            logging.debug(f"Services for premise: {json.dumps(services, indent=2)}")

            for service in services:
                if not isinstance(service, dict):
                    logging.warning(f"Invalid service object: {service}. Skipping.")
                    continue

                logging.debug(f"Processing service: {json.dumps(service, indent=2)}")

                service_details = service.get('service_details')
                if not service_details or not isinstance(service_details, dict):
                    logging.warning("Service details are missing or invalid, skipping service.")
                    continue

                full_service = service_details.get('full_service', {})
                if not isinstance(full_service, dict):
                    logging.warning("Full service details are missing or invalid, skipping service.")
                    continue

                logging.debug(f"Processing full_service: {json.dumps(full_service, indent=2)}")

                work_orders_data = service.get('work_orders')
                if not work_orders_data or not isinstance(work_orders_data, dict):
                    logging.warning("Work orders data is missing or invalid, skipping service.")
                    continue

                work_orders = work_orders_data.get('items', [])
                if not isinstance(work_orders, list):
                    logging.warning(f"Expected 'work_orders' to be a list, but got {type(work_orders)}. Skipping service.")
                    continue

                logging.debug(f"Work orders for service: {json.dumps(work_orders, indent=2)}")

                for work_order in work_orders:
                    if not isinstance(work_order, dict):
                        logging.warning(f"Invalid work order object: {work_order}. Skipping.")
                        continue

                    logging.debug(f"Processing work order: {json.dumps(work_order, indent=2)}")

                    # Validate ticket creation inputs before proceeding
                    if not contact_id or not ticket_types:
                        logging.error("Required data for ticket creation is missing, skipping work order.")
                        continue

                    try:
                        create_or_update_tickets_for_contact(
                            contact_id,
                            work_order,
                            ticket_types,
                            premise,
                            customer,
                            {"id": service_id},
                            sales_rep_data
                        )
                    except Exception as e:
                        logging.error(f"Error creating or updating tickets: {e}")


# Run the main function
if __name__ == "__main__":
    process_premises_for_hubspot()
