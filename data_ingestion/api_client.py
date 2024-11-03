import requests

class ApiClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_data(self, endpoint):
        response = requests.get(f"{self.base_url}/{endpoint}")
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
