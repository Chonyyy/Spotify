import json
import requests
from http.server import BaseHTTPRequestHandler

def sync_data_with_node(node_ip, data):
    url = f'http://{node_ip}:8001/sync'
    response = requests.post(url, json=data)
    return response.status_code

class SyncHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        # Handle synchronization logic here
        self.send_response(200)
        self.end_headers()
