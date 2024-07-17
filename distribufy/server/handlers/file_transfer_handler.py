import os
import json
import threading
from urllib.parse import urlparse, parse_qs

from http.server import BaseHTTPRequestHandler

class FileTransferHandler(BaseHTTPRequestHandler):
    
    def do_POST(self):
        """Handle POST requests for receiving files."""
        if self.path == '/upload-file':
            self.handle_upload_file()
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)
        
    def do_GET(self):
        """Handle GET requests for sending files."""
        if self.path.startswith('/send-file'):
            self.handle_download_file()
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)

    def handle_upload_file(self):
        """Receives a file sent as a binary stream."""
        file_size = int(self.headers['Content-Length'])
        file_data = self.rfile.read(file_size)
        
        if 'key' not in self.headers or 'file_name' not in self.headers:
            self.send_json_response(None, error_message='Missing key or file_name in headers', status=400)
            return

        key = self.headers['key']
        file_name = self.headers['file_name']
        file_path = os.path.join(self.server.node.file_storage_path, file_name)

        try:
            with open(file_path, 'wb') as file:
                file.write(file_data)
            
            self.server.node.store_file(key, file_path)
            self.send_json_response({"status": "success"})
        except Exception as e:
            self.send_json_response(None, error_message=str(e), status=500)

    def handle_download_file(self):
        """Sends a file as a binary stream."""
        query_params = urlparse(self.path).query
        params = parse_qs(query_params)
        
        if 'key' not in params:
            self.send_json_response(None, error_message='Query must contain a key', status=400)
            return

        key = params['key'][0]
        file_path = self.server.node.get_file_path(key)

        if not file_path or not os.path.exists(file_path):
            self.send_json_response(None, error_message='File not found', status=404)
            return

        try:
            with open(file_path, 'rb') as file:
                file_data = file.read()

            self.send_response(200)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("Content-Length", str(len(file_data)))
            self.send_header("Content-Disposition", f'attachment; filename="{os.path.basename(file_path)}"')
            self.end_headers()
            self.wfile.write(file_data)
        except Exception as e:
            self.send_json_response(None, error_message=str(e), status=500)

    def send_json_response(self, response, error_message=None, status=200):
        """Sends a JSON response."""
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        
        if response:
            self.wfile.write(json.dumps(response).encode())
        else:
            if error_message:
                self.wfile.write(json.dumps({'status': 'error', 'message': error_message}).encode())
            else:
                self.wfile.write(json.dumps({'status': 'error', 'message': 'Unknown error'}).encode())