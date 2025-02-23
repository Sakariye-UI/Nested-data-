import datetime
import os
from napkin import response, request
from klaviyo_api import KlaviyoAPI  # Import the Klaviyo SDK

# Initialize the Klaviyo client with your API key
klaviyo = KlaviyoAPI(api_key=os.environ['klaviyo_api'], max_delay=60, max_retries=3)

# Function to bubble up nested data
def bubble_up_nested_data(data):
    event_properties = data.get("event", {})

    properties = {
        "Order_ID": event_properties.get("OrderId", ""),
        "Email": data.get("email", ""),
        "Discount_Value": event_properties.get("DiscountValue", 0),
        "Total_Value": event_properties.get("$value", 0),
        "Currency": event_properties.get("$value_currency", ""),
        "Billing_Address": event_properties.get("BillingAddress", {}).get("Address1", ""),
        "City": event_properties.get("BillingAddress", {}).get("City", ""),
        "Country_Code": event_properties.get("BillingAddress", {}).get("CountryCode", ""),
        "First_Name": event_properties.get("BillingAddress", {}).get("FirstName", ""),
        "Last_Name": event_properties.get("BillingAddress", {}).get("LastName", ""),
        "Phone_Number": event_properties.get("BillingAddress", {}).get("Phone", ""),
        "Region_Code": event_properties.get("BillingAddress", {}).get("RegionCode", "")
    }

    # Bubble up categories
    categories = event_properties.get("Categories", [])
    for i, category in enumerate(categories):
        properties[f"Category_{i+1}"] = category

    # Bubble up items
    items = event_properties.get("Items", [])
    item_list = []
    for i, item in enumerate(items):
        item_properties = {
            "Product_ID": item.get("ProductID", ""),
            "SKU": item.get("SKU", ""),
            "Product_Name": item.get("ProductName", ""),
            "Quantity": item.get("Quantity", 0),
            "Item_Price": item.get("ItemPrice", 0),
            "Row_Total": item.get("RowTotal", 0),
            "Product_URL": item.get("ProductURL", ""),
            "Image_URL": item.get("ImageURL", ""),
            "Categories": item.get("Categories", []),
            "Brand": item.get("Brand", "")
        }
        item_list.append(item_properties)
    properties["Items"] = item_list

    return properties

# Function to stamp the current time
def stamp_current_time(event_properties):
    date_time = datetime.datetime.now()
    event_properties["Event_Time"] = date_time.strftime("%Y-%m-%dT%H:%M:%S")
    return event_properties

# Function to send data to Klaviyo
def send_data_to_klaviyo(event_properties):
    email = event_properties.get("Email")  # Ensure email is extracted for the event

    if not email:
        print("Error: Missing email identifier.")
        return

    # Create the event in Klaviyo using the Klaviyo API client
    try:
        klaviyo.Events.create_event({
            "data": {
                "type": "event",
                "attributes": {
                    "time": event_properties.get("Event_Time"),
                    "properties": event_properties,
                    "metric": {
                        "data": {
                            "type": "metric",
                            "attributes": {
                                "name": "Placed Order Nested Data"  # Event name
                            }
                        }
                    },
                    "profile": {
                        "data": {
                            "type": "profile",
                            "attributes": {
                                "email": email
                            }
                        }
                    }
                }
            }
        })
        print("Event sent successfully.")
    except Exception as e:
        print(f"Failed to send event: {e}")

# Main logic: Use incoming data from the request
try:
    # Access the incoming JSON payload
    incoming_data = request.body  # Use this if the content type is application/json
    if incoming_data:
        event_properties = bubble_up_nested_data(incoming_data)
        event_properties = stamp_current_time(event_properties)
        send_data_to_klaviyo(event_properties)
        response.status_code = 200
        response.body = "Event processed successfully"
    else:
        response.status_code = 400
        response.body = "No data received"
except Exception as e:
    response.status_code = 500
    response.body = f"Error processing event: {str(e)}"
