import hashlib
import threading
import json
import logging
from http.server import BaseHTTPRequestHandler
from server.node_reference import ChordNodeReference

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

#region RequestHandler
class ChordNodeRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Request path {self.path}')
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None

        if self.path == '/store-data':
            response = self.handle_store_data(post_data)
            self.send_json_response(response)
        elif self.path == '/get-data':
            response = self.handle_get_data(post_data)
            self.send_json_response(response)
        elif self.path == '/store-replic':
            print('store replication')
            # threading.Thread(target=self.handle_store_replic, args=[post_data], daemon=True)
            self.handle_store_replic(post_data)
            self.send_json_response({'status':'recieved'})
        elif self.path == '/election':
            response = self.handle_election(post_data)
        elif self.path == '/coordinator':
            response = self.handle_coordinator(post_data)
        elif self.path == '/find_successor':
            response = self.server.node.find_succ(post_data['id'])
            self.send_json_response(response)
        elif self.path == '/find_predecessor':
            response = self.server.node.find_pred(post_data['id'])
            self.send_json_response(response)
        elif self.path == '/get_successor':
            response = self.server.node.succ
            self.send_json_response(response)
        elif self.path == '/get_predecessor':
            response = self.server.node.pred
            logger_rh.debug(f'Response for get_predecessor request:\n{response}')
            self.send_json_response(response)
        elif self.path == '/notify':
            response = self.handle_notify(post_data)
            self.send_json_response(response)
        elif self.path == '/check_predecessor':#FIXME: I broke this
            response = {'status': 'success'}
            self.send_json_response(response, status=200)#TODO: change this to get request
        elif self.path == '/ping':
            response = {'status': 'success'}
            self.send_json_response(response, status=200)#TODO: change this to get request
        elif self.path == '/closest_preceding_finger':
            response = self.server.node.closest_preceding_finger(post_data['id'])
            self.send_json_response(response)
        elif self.path == '/debug-node-data':
            self.server.node._debug_log_data()
            self.send_json_response({"status": "success"})
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)
        
    def do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path.startswith('/get_user'):
            response = self.handle_get_user(self.path)
            self.send_json_response(response, status=200)
            
        elif self.path == '/list_users':
            response = self.server.node.list_users()
            self.send_json_response(response, status=200)
            
        elif self.path == '/election_failed':
            self.send_json_response({'status':'Accepted'}, status=202)
            self.handle_start_election()
        elif self.path == '/debug/discover':
            logger.info(f'Discovery Debug Not Implemented')#TODO: Fix discovery
        elif self.path == '/debug/finger_table':
            self.server.node.print_finger_table()
        elif self.path == '/debug/start-election':
            self.handle_start_election()
            
        else:
            self.send_json_response(response, error_message='Page not found', status=404)
            
    def handle_store_replic(self, post_data):
        #TODO: Add validations
        print('hola')
        if 'source' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a source id', status=400)
        if post_data['source'] != self.server.node.succ.id or post_data['source'] != self.server.node.pred.id:
            self.send_json_response(None, error_message='Data must belong to predecesor or succesor', status=400)
        source = post_data['source']
        key = post_data['key']
        del post_data['key']
        del post_data['source']
        print('adios')
        self.server.node.store_replic(source, post_data, key)
        return {"status": "success"} 

    def handle_store_data(self, post_data):
        if 'callback' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a callback addr', status=400)
        if 'key_fields' not in post_data or not isinstance(post_data['key_fields'], list):
            self.send_json_response(None, error_message='Provided data must contain a key_fields list')
        callback = post_data['callback']
        key_fields = post_data['key_fields']
        del post_data['callback'], post_data['key_fields']
        self.server.node.store_data(key_fields, post_data, callback)
        return {"status": "success"}
    
    def handle_get_data(self, post_data):
        if 'callback' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a callback addr', status=400)
        if 'key' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a key')
        callback = post_data['callback']
        key = post_data['key']
        self.server.node.get_data(key, callback)
        return {"status": "success"}

    def handle_election(self, post_data):
        # Request validation
        necessary_fields = all(['candidates' in post_data, 'initiator' in post_data])
        correct_types = necessary_fields and all([isinstance(post_data['candidates'], list), isinstance(post_data['initiator'], list)])
        all_info_init = correct_types and len(post_data['initiator']) == 2
        all_info_candidates = all_info_init and all([len(x) == 2 for x in post_data['candidates']])
        
        if not necessary_fields:
            error_message = "Missing necessary fields. Required: 'candidates' (list) and 'initiator' (list)."
        elif not correct_types:
            error_message = "Incorrect field types. 'candidates' and 'initiator' should be lists."
        elif not all_info_init:
            error_message = "Initiator must be a list of length 2."
        elif not all_info_candidates:
            error_message = "Each candidate must be a list of length 2."
        else:
            threading.Thread(target=self.server.node.process_election_message, args=[post_data], daemon=True).start()
            self.send_json_response({"status": "success"})
            return
        self.send_json_response(None, error_message=error_message, status=400)

    def handle_coordinator(self, post_data):
        
        # Request validation
        necessary_fields = all(['leader' in post_data, 'initiator' in post_data])
        correct_types = necessary_fields and all([isinstance(post_data['leader'], list), isinstance(post_data['initiator'], list)])
        correct_lengths = correct_types and all([len(post_data['leader']) == 2, len(post_data['initiator']) == 2])

        if not necessary_fields:
            error_message = "Missing necessary fields. Required: 'leader' (list) and 'initiator' (list)."
        elif not correct_types:
            error_message = "Incorrect field types. 'leader' and 'initiator' should be lists."
        elif not correct_lengths:
            error_message = "'leader' and 'initiator' must be lists of length 2."
        else:
            threading.Thread(target=self.server.node.process_coordinator_message, args=[post_data], daemon=True).start()
            self.send_json_response({"status": "success"})
            return

        self.send_json_response({}, error_message=error_message, status=400)

    def handle_notify(self, post_data):
        node = ChordNodeReference(post_data['id'], post_data['ip'])
        self.server.node.notify(node)

    def handle_start_election(self):
        threading.Thread(target=self.server.node.start_election, daemon=True).start()
    
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
            elif isinstance(response, ChordNodeReference):
                self.wfile.write(json.dumps({'id': response.id, 'ip': response.ip}).encode())
            else:
                self.wfile.write(json.dumps(response).encode())
        else:
            if error_message:
                self.wfile.write(json.dumps({'status': 'error', 'message': error_message}).encode())
            else:
                self.wfile.write(json.dumps({'id': None, 'ip': None}).encode())