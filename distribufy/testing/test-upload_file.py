import requests
import os

def upload_file(server_url, file_path, key):
    """
    Sends a file to the server.
    
    Args:
        server_url (str): The URL of the server endpoint for sending files.
        file_path (str): The path to the file to send.
        key (str): The key associated with the file.
    """
    with open(file_path, 'rb') as file:
        file_data = file.read()
    
    headers = {
        'key': key,
        'file_name': os.path.basename(file_path),
        'Content-Length': str(len(file_data))
    }
    
    response = requests.post(server_url, headers=headers, data=file_data)
    print(f'Status Code: {response.status_code}')
    print(f'Response: {response.json()}')

if __name__ == "__main__":
    # Update these variables with your server URL, file path, and key
    server_url = 'http://localhost:8001/upload-file'
    file_path = './test_files/test_file.txt'
    key = 'your_file_key'

    upload_file(server_url, file_path, key)
