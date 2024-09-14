import requests
from os import environ

class BunnyAPI:
    def __init__(self):
        self.API_Endpoint_URL = environ["BUNNY_API_ENDPOINT_ADDRESS"]
    def file_QueueUpload(self, local_file_path: str, target_file_path: str):
        headers = {
            "local-file-path": local_file_path,
            "target-file-path": target_file_path
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/files/upload", headers=headers)

        return request.status_code
    
    def file_List(self, path: str):
        headers = {
            "path": path
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/files/list", headers=headers)

        return request.json()
    def file_Delete(self, target_file_path: str):
        headers = {
            "target-file-path": target_file_path
        }
        request = requests.delete(f"http://{self.API_Endpoint_URL}/files/delete", headers=headers)

        return request.status_code
    def file_Retrieve(self, target_file_path: str):
        headers = {
            "target-file-path": target_file_path
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/files/retrieve_metadata", headers=headers)

        return request.status_code

    def cache_Purge(self, target_url: str):
        request = requests.post(f"http://{self.API_Endpoint_URL}/cache/purge?url={target_url}")

        return request.status_code