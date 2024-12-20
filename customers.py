import os
import requests
import json
from datetime import datetime, timedelta
import logging


# Base URL for API
BASE_URL = "https://fno.national-us.aex.systems"

# Fetch API_TOKEN from environment or .env file
API_TOKEN = os.getenv('API_TOKEN')   # Fetching API token from environment variable

if not API_TOKEN:
    raise Exception("API_TOKEN environment variable is not set")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Set the number of hours for 'updated_after'. If None, defaults to 24 hours.
HOURS = 24

# Function to get 'updated_after' date (24 hours prior or custom interval)
def get_updated_after(hours=None):
    if hours is None:
        hours = 24
    pull_time = datetime.now() - timedelta(hours=hours)
    formatted_time = pull_time.isoformat().replace('T', ' ').split('.')[0]
    return formatted_time

# Fetch premises with updated_after filter and handle pagination
def fetch_premises(updated_after, page=1):
    url = f"{BASE_URL}/services"
    params = {
        "updated_after": updated_after,
        "page": page
    }

    try:
        logging.info(f"Fetching premises data for page {page}")
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data for page {page}")
            return response.json()
        else:
            raise Exception(f"Error fetching premises (page {page}): {response.status_code}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

# Fetch details for a specific service by ID
def fetch_service_details(service_id):
    url = f"{BASE_URL}/services/{service_id}"
    try:
        logging.info(f"Fetching details for service ID {service_id}")
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            logging.info(f"Successfully fetched details for service ID {service_id}")
            return response.json()
        else:
            raise Exception(f"Error fetching details for service ID {service_id}: {response.status_code}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

# Create customers.json file using services fetched with updated_after filter
def create_customers_json():
    updated_after = get_updated_after(HOURS)
    logging.info(f"Fetching premises updated after {updated_after}")
    customer_data = []
    page = 1

    while True:
        services_data = fetch_premises(updated_after, page)
        if services_data and 'items' in services_data:
            services = services_data['items']
            if not services:
                logging.info(f"No more data available at page {page}")
                break

            logging.info(f"Processing {len(services)} services from page {page}")
            for service in services:
                service_id = service['id']
                service_details = fetch_service_details(service_id)  # Fetch additional details
                if service_details:
                    # Merge service details with base data
                    customer_entry = {
                        "id": service_id,
                        "preorder": service.get('preorder'),
                        "customer_id": service.get('customer_id'),
                        "product_id": service.get('product_id'),
                        "premise_id": service.get('premise_id'),
                        "provisioned": service.get('provisioned'),
                        "on_network": service.get('on_network'),
                        "created_at": service.get('created_at'),
                        "updated_at": service.get('updated_at'),
                        "promo_code": service.get('promo_code'),
                        "sales_agent": service.get('sales_agent'),
                        "sales_channel_id": service.get('sales_channel_id'),
                        "cancelled": service.get('cancelled'),
                        "cancelled_date": service.get('cancelled_date'),
                        "status": service_details.get('status')  # Add status from secondary API call
                    }
                    customer_data.append(customer_entry)

            # If the number of items is less than 10, assume it's the last page
            if len(services) < 10:
                logging.info(f"Reached the last page of data at page {page}")
                break
            page += 1
        else:
            logging.info(f"No more data available at page {page}")
            break

    with open("customers.json", 'w') as json_file:
        json.dump(customer_data, json_file, indent=4)
        logging.info(f"Data saved to customers.json")

# Main function to demonstrate creating customers.json
def main():
    logging.info("Starting the process to create customers.json")
    create_customers_json()
    logging.info("Process completed")

# Run the main function
if __name__ == "__main__":
    main()
