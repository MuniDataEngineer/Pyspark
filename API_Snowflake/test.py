import requests

# Your FastAPI URL
url = "http://127.0.0.1:8000/get-product-name/123"  # Change 123 to your actual product_id

# 🛡Headers with API key
headers = {
    "x-api-key": "your_api_key_here",  # Replace with the API key from your .env
    "Content-Type": "application/json"
}

# Make the GET request
response = requests.get(url, headers=headers)

# Print results
print("Status Code:", response.status_code)
print("Response JSON:", response.json())
