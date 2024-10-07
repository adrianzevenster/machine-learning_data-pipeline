import requests

# Replace with actual API endpoint and credentials provided by the service
API_URL = '53.0.0.127.in-addr.arpa name = localhost'
API_KEY = 'your_api_key_here'


def send_whatsapp_message(phone_number, message):
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }

    data = {
        'phone_number': phone_number,
        'message': message
    }

    try:
        response = requests.post(API_URL, headers=headers, json=data)
        response_data = response.json()

        if response.status_code == 200:
            print(f"Message sent successfully to {phone_number}")
        else:
            print(f"Failed to send message. Error: {response_data.get('error')}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


# Example usage:
if __name__ == "__main__":
    phone_number = '+1234567890'  # Replace with recipient's phone number
    message = 'Hello, this is an example message sent via API.'

    send_whatsapp_message(phone_number, message)