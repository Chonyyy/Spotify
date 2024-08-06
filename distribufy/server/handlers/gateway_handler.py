import hashlib
import threading
import json
import logging
import os
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from server.gateway_reference import GatewayReference

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

class GatewayRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        logger_rh.debug(f'Request path {self.path}')
        """Handle POST requests."""
        
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None
        
        if self.path == '/notify':
            self.handle_notify(post_data)
        elif self.path == '/new_leader':
            self.handle_new_leader(post_data)
        elif self.path == '/share_knowledge':
            self.handle_share_knowledge(post_data['nodes'])
        else:
            self.send_json_response(None, error_message='Page not found', status=404)


        # if self.path == '/get-leader':
        #     response = self.server.node.leader_info()
        #     self.send_json_response(response)
        # elif self.path.startswith('/iterate-songs'):
        #     response = self.server.node._get_songs(post_data['origin'])
        #     self.send_json_response(response, status=200)
        # elif self.path.startswith('/songs/upload_chunks/{song_id}'):#TODO
        #     pass
        # else:
        #     self.send_json_response({}, 'Invalid Endpoint', status=404)
        
    def do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path == '/ping':
            self.send_json_response({'status':'up'})
        else:
            self.send_json_response(None, error_message='Page not found', status=404)
        
        # if self.path.startswith('/get_user'):
        #     response = self.handle_get_user(self.path)
        #     self.send_json_response(response, status=200)
        # elif self.path == '/list_users':
        #     response = self.server.node.list_users()
        #     self.send_json_response(response, status=200)
        # elif self.path.startswith('/songs/stream/{song_id}'):#TODO sends the song with id
        #     pass
        # else:
        #     self.send_json_response(response, error_message='Page not found', status=404)
           
    def handle_notify(self, node_data):
        node = GatewayReference(node_data['id'], node_data['ip'])
        result = self.server.node.notify(node)
        if result[0]:
            self.send_json_response(result[1])
        else:
            self.send_json_response(None, result[1], 400)
            
    def handle_new_leader(self, leader_data):
        node = GatewayReference(leader_data['id'], leader_data['ip'])
        self.server.node.set_new_leader(node) #TODO return if leader its alive
        self.send_json_response({'status':'ok'})
    
    def handle_share_knowledge(self, nodes):
        live_nodes = []
        for node in nodes:
            live_nodes.append(GatewayReference(**node))
        self.server.node.share_knowledge(live_nodes)
        
    def send_json_response(self, response, error_message=None, status=200, origin = None):
        if origin:
            print(f'Responging to {self.client_address}, from {origin}')
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        
        if response:
            if isinstance(response, dict):
                if origin:
                    print('response as dict')
                self.wfile.write(json.dumps(response).encode())
            elif isinstance(response, GatewayReference):
                self.wfile.write(json.dumps({'id': response.id, 'ip': response.ip}).encode())
            else:
                self.wfile.write(json.dumps(response).encode())
        else:
            if error_message:
                self.wfile.write(json.dumps({'status': 'error', 'message': error_message}).encode())
            else:
                self.wfile.write(json.dumps({'id': None, 'ip': None}).encode())
                
        