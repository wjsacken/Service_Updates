import os
import requests
import json

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

# Load premises data from a JSON file
def load_premises_data(filename="customers.json"):
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
        return data

# Fetch premises by customer_id
def fetch_premises_by_customer(customer_id):
    url = f"{BASE_URL}/premises?customer={customer_id}"

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response.json().get("items", [])
        else:
            raise Exception(f"Error fetching premises for customer {customer_id}: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# Fetch services by service_id
def fetch_services(service_id):
    url = f"{BASE_URL}/services/{service_id}"

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            services_data = response.json()
            print(f"Services Data for Service {service_id}: {services_data}")
            return services_data
        else:
            raise Exception(f"Error fetching services for service {service_id}: {response.status_code}")
    except Exception as e:
        print(f"An error occurred while fetching services: {e}")
        return {}

# Fetch full service details by service_id
def fetch_service_details(service_id):
    full_service_url = f"{BASE_URL}/services/{service_id}/full"

    try:
        full_service_response = requests.get(full_service_url, headers=HEADERS)

        if full_service_response.status_code == 200:
            return full_service_response.json()
        else:
            raise Exception(f"Error fetching details for service {service_id}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}

# Fetch work orders by service_id
def fetch_work_orders(service_id):
    url = f"{BASE_URL}/work-orders"
    params = {"service": service_id}

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching work orders for service {service_id}: {response.status_code}")
    except Exception as e:
        print(f"An error occurred while fetching work orders: {e}")
        return []

def fetch_customer_details(customer_id):
    customer_url = f"{BASE_URL}/customers/{customer_id}"
    customer_services_url = f"{BASE_URL}/customers/{customer_id}/services"

    try:
        customer_response = requests.get(customer_url, headers=HEADERS)
        customer_services_response = requests.get(customer_services_url, headers=HEADERS)

        if customer_response.status_code == 200 and customer_services_response.status_code == 200:
            return {
                "customer_details": customer_response.json(),
                "customer_services": customer_services_response.json()
            }
        else:
            raise Exception(f"Error fetching details for customer {customer_id}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}

# Enrich each premise with its services, work orders, and customer details
def enrich_premises_with_services_and_customers(premises_data):
    enriched_data = []
    for premise in premises_data:
        premise_id = premise['premise_id']
        customer_id = premise['customer_id']
        service_id = premise['id']  # Using 'id' from JSON as the service_id

        # Fetch related services for this premise using service_id
        services = fetch_services(service_id)

        # Fetch detailed service info and work orders
        service_details = []
        if isinstance(services, dict) and 'id' in services:
            # Fetch detailed service info
            details = fetch_service_details(service_id)

            # Fetch related work orders for the service
            work_orders = fetch_work_orders(service_id)

            # Attach work orders to the service details
            service_info = {
                "service_details": details,
                "work_orders": work_orders
            }
            service_details.append(service_info)
        else:
            print(f"Invalid service data for service {service_id}: {services}")

        # Fetch customer details for this premise
        customer_details = fetch_customer_details(customer_id)
        customer = customer_details.get('customer_details', {})

        # Attach services and customer info to the premise data
        premise_copy = premise.copy()  # Create a shallow copy to avoid circular reference
        premise_copy['services'] = service_details
        premise_copy['customer'] = customer
        enriched_data.append(premise_copy)

    return enriched_data

# Save the enriched data to a JSON file (overwrites the file each time)
def save_data_to_file(data, filename="enriched_premises_data.json"):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
        print(f"Data saved to {filename}")

# Main function to demonstrate the API call with pagination and save enriched data to file
def main():
    # Load premises data from the JSON file
    premises_data = load_premises_data()

    if not premises_data:
        print("No premises data available or an error occurred")
        return

    enriched_data = enrich_premises_with_services_and_customers(premises_data)
    save_data_to_file(enriched_data)
    print(f"Fetched and enriched {len(enriched_data)} premises in total.")

# Run the main function
if __name__ == "__main__":
    main()
