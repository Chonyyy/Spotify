import hashlib
import threading
import json
import logging
import os
from urllib.parse import urlparse, parse_qs
from server.chord_node import ChordNode
from server.utils.helper_funcs import get_sha_repr
from server.handlers.base_handler import BaseHandler
from server.node_reference import ChordNodeReference

# Set up logging
logger = logging.getLogger("__main__")
logger_rh = logging.getLogger("__main__.rh")

class ChordNodeHandler(BaseHandler):

    def _do_POST(self):
        """Handle POST requests."""
        logger_rh.debug(f'Request path {self.path}')
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Handling the following request \n{post_data}')
        response = None

        if self.path == '/store-data':
            '''Stores data in the corresponding node'''
            response = self.handle_store_data(post_data)
            self.send_json_response(response)
        elif self.path == '/get-data':
            '''Gets a specific data from the corresponding node'''
            response = self.handle_get_data(post_data)
            self.send_json_response(response)
        elif self.path == '/store-replic':
            '''Stores the data of another node as a replic'''
            self.handle_store_replic(post_data)
            self.send_json_response({'status':'recieved'})
        elif self.path == '/election':
            '''Presents itself as candidate and expands election to the successor node'''
            response = self.handle_election(post_data)
        elif self.path == '/coordinator':
            '''Starts the process of Aknowloging the leader. Recieves the information on who is the leader'''
            response = self.handle_coordinator(post_data)
        elif self.path == '/find_successor':
            '''Finds the successor node for a given key'''
            response = self.server.node.find_succ(post_data['id'])
            self.send_json_response(response)
        elif self.path == '/find_predecessor':
            '''Finds the predecessor node for a given key'''
            response = self.server.node.find_pred(post_data['id'])
            self.send_json_response(response)
        elif self.path == '/notify':
            #TODO: What does this do ?
            response = self.handle_notify(post_data)
            self.send_json_response(response)
        elif self.path == '/closest_preceding_finger':
            #TODO: What does this do ?
            response = self.server.node.closest_preceding_finger(post_data['id'])
            self.send_json_response(response)
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)

    def _do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path == '/election_failed':
            '''Notifies the initiator of an election that the election failed'''
            self.send_json_response({'status':'Accepted'}, status=202)
            self.handle_start_election()
        elif self.path == '/ping':
            '''Ping request'''
            response = {'status': 'success'}
            self.send_json_response(response, status=200)
        elif self.path.startswith('/drop-successor'):
            '''Someone is asking this node to drop the replicated database of its successor'''
            response = self.server.node.drop_successor()
            self.send_json_response(response, status=200)
        elif self.path.startswith('/drop-predecesor'):
            '''Someone is asking this node to drop the replicated database of its predecesor'''
            response = self.server.node.drop_predecessor()
            self.send_json_response(response, status=200)
        elif self.path == '/get-leader':#TODO: Once discovery changes, is this necesary ?
            '''Returns the ip and id of the leader of this role'''
            response = self.server.node.leader
            self.send_json_response(response)
        elif self.path == '/check_predecessor':
            '''Ping request to a predecesor node'''
            response = {'status': 'success'}
            self.send_json_response(response, status=200)
        elif self.path == '/get_successor':
            '''Returns the successor of this node'''
            response = self.server.node.succ
            self.send_json_response(response)
        elif self.path == '/get_predecessor':
            '''Returns the predecessor of this node'''
            response = self.server.node.pred
            self.send_json_response(response)
            
        elif self.path == '/debug/finger_table':
            self.server.node.print_finger_table()
        elif self.path == '/debug/start-election':
            self.handle_start_election()
        elif self.path == '/debug-node-data':
            self.server.node._debug_log_data()
            self.send_json_response({"status": "success"})
        else:
            self.send_json_response(response, error_message='Page not found', status=404)
            
    def handle_store_data(self, post_data):
        if 'callback' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a callback addr', status=400)
        if 'key_fields' not in post_data or not isinstance(post_data['key_fields'], list):
            self.send_json_response(None, error_message='Provided data must contain a key_fields list', status=400)
        for key_part in post_data['key_fields']:
            if not key_part in key_fields:
                logger_rh.error(f'Key part {key_part} not provided in request')
                self.send_json_response(None, error_message=f'Key part {key_part} not provided in request', status=400)
                
        callback = post_data['callback']
        key_fields = post_data['key_fields']
        del post_data['callback'], post_data['key_fields']
        self.server.node.store_data(key_fields, post_data, callback)
        return {"status": "success"}###TODO: Add better error managing????
    
    def handle_get_data(self, post_data):
        if 'callback' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a callback addr', status=400)
        if 'key' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a key')
        callback = post_data['callback']
        key = post_data['key']
        self.server.node.get_data(key, callback)
        return {"status": "success"}###TODO: Add better error managing????
    
    def handle_store_replic(self, post_data):
        if 'source' not in post_data:
            self.send_json_response(None, error_message='Provided data must contain a source id', status=400)
        if post_data['source'] != self.server.node.succ.id and post_data['source'] != self.server.node.pred.id:
            self.send_json_response(None, error_message='Data must belong to predecesor or succesor', status=400)

        source = post_data['source']
        key = post_data['key']
        del post_data['key']
        del post_data['source']
        
        self.server.node.store_replic(source, post_data, key)
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