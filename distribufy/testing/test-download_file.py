import requests

def receive_file(server_url, key, output_path):
    """
    Receives a file from the server.
    
    Args:
        server_url (str): The URL of the server endpoint for sending files.
        key (str): The key associated with the file.
        output_path (str): The path where the file should be saved.
    """
    response = requests.get(f'{server_url}?key={key}', stream=True)
    
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f'File saved to {output_path}')
    else:
        print(f'Failed to receive file. Status Code: {response.status_code}')
        print(f'Response: {response.json()}')

if __name__ == "__main__":
    # Update these variables with your server URL, key, and output path
    server_url = 'http://localhost:8001/download-file'
    key = 'your_file_key'
    output_path = './test_files/received_file.txt'

    receive_file(server_url, key, output_path)