import hashlib
import threading
import json
import requests
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from server.handlers.user import User
from server.utils.my_orm import JSONDatabase

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

#region NodeReference

class ChordNodeReference:
    def __init__(self, id: str, ip: str, port: int = 8001):
        self.id = get_sha_repr(ip) if not id else id
        self.ip = ip
        self.port = port
        self.replication_queue = []
        
    #region Coordination
    
    def send_election_message(self, election_message):
        self._send_request('/election', election_message)

    def send_coordinator_message(self, coordinator_message):
        self._send_request('/coordinator', coordinator_message)
    
    def send_store_data(self, data, callback, key_fields):
        """Send request to store a user."""
        data['callback'] = callback
        data['key_fields'] = key_fields
        self._send_request('/store-data', data)
        
    def send_get_data(self, key, callback):#TODO: Implement this
        """Send request to get an user"""
        data = {
            'callback':callback,
            'key':key
        }
        self._send_request('/get-data', data)
        
    def enqueue_rep_operation(self, source_id, data, operation='insertion', key=None):
        self.replication_queue.append((source_id, data, operation, key))
    
    def apply_rep_operations(self):
            while self.replication_queue:
                source, data, operation, key = self.replication_queue.pop()
                if operation == 'insertion':
                    data['source'] = source
                    data['key'] = key
                    self._send_request('/store-replic', data)

    #region Chord logic
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        """Find successor of a given id."""
        response = self._send_request('/find_successor', {'id': str(id)})
        logger.debug(f'Find Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        response = self._send_request('/find_predecessor', {'id': str(id)})
        logger.debug(f'Find Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    @property
    def succ(self) -> 'ChordNodeReference':
        """Get successor node."""
        response = self._send_request('/get_successor', {})
        logger.debug(f'Get Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        """Get predecessor node."""
        response = self._send_request('/get_predecessor', {})
        logger.debug(f'Get Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def ping(self):
        self._send_request('/ping', {})

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})

    def check_predecessor(self):
        """Ping the predecessor to check if it is alive."""
        self._send_request('/check_predecessor', {})

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        response = self._send_request('/closest_preceding_finger', {'id': id})
        return ChordNodeReference(response['id'], response['ip'], self.port)

    #region Utils
    
    def _send_request(self, path: str, data: dict) -> dict:
        """Send a request and handle retries."""
        max_retries = 4
        for i in range(max_retries):
            response = None
            try:
                url = f'http://{self.ip}:{self.port}{path}'
                logger.info(f'Sending request to {url}\nPayload: {data}')

                response_raw = requests.post(url, json=data)
                response = response_raw.json()#TODO: Remove this after you are done
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError as e:
                logger.error(f'Connection Error in IP {self.ip}')
                logger.error(f'{e}')
                if i == max_retries - 1:
                    raise e
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f'JSON Decode Error: {e}')
                logger.error(f'Response text: {response_raw.text}')  # Log the response body
            except Exception as e:
                logger.error(f"Error sending data to {path}: {data}\n{e}")
                raise e
            
    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)

#region ChordNode
class ChordNode:
    def __init__(self, ip: str, db: JSONDatabase, pred_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = role
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = self.ref
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}#TODO: implementar una clase database, que funcione como una db no relacional. Pandas para facilitar filtrado a lo mejor ?
        self.replicated_data_pred = {}
        self.replicated_data_succ = {}
        self.replicating = False#TODO: Is this necesary with thread lock ?
        self.leader = self.ref
        self.election_started = False#TODO: What happens if the election takes too long
        #TODO: If pred or succ changes, replicate the whole database
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self
        self.replication_lock = threading.Lock()

        logger.info(f'node_addr: {ip}:{port}')

        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        threading.Thread(target=self.replication_loop, daemon=True).start()  # Start replication thread
          
    #region Data
    
    def replication_loop(self):
        while True:
            time.sleep(30)
            if self.pred.replication_queue:
                with self.replication_lock:
                    self.succ.apply_rep_operations()
                    self.pred.apply_rep_operations()
        
    #TODO: Respond to the callback once the data is actually stored
    def store_data(self, key_fields, data, callback = None):
        key_information = ''
        for element in [str(data[k]) for k in key_fields]:
            key_information += element
        key = get_sha_repr(key_information)
        logger_dt.info(f'Storing information: {key_information}; key: {key}')
        target_node = self.find_succ(key)
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id}')
        if target_node.id == self.id:
            self.data[key] = data
            logger.info(f'Data {key_information} stored at node {self.ip}')
            self.enqueue_replication_operation(data, 'insertion', key)
        else:
            data['key_fields'] = key_fields
            threading.Thread(target=target_node.send_store_data, args= [data, callback, key_fields], daemon=True).start()
            
    def get_data(self, key, callback):#TODO: While testing sending to an invalid url, the url seemed fine but got an error
        logger_dt.info(f'Getting item by key {key}')
        target_node = self.find_succ(key)
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id}')
        if target_node.id == self.id:
            ##TODO: respond to callback
            data = self.data[key]
            self._send_request(callback, data)
        else:
            threading.Thread(target=target_node.send_get_data, args= [key, callback], daemon=True).start()
            
    def store_replic(self, source, data, key):
        logger.info(f'Replic storage comenced')
        if source == self.pred.id:
            logger.info(f'Storing replic information: {data} in from pred node {source}')
            self.replicated_data_pred[key] = data
            logger.info(f'Replicated data stored')
        if source == self.succ.id:
            logger.info(f'Storing replic information: {data} in from succ node {source}')
            self.replicated_data_succ[key] = data
            logger.info(f'Replicated data stored')
            
    def enqueue_replication_operation(self, data, operation, key):#TODO: Debug
        max_retries = 5
        retry_interval = 2  # seconds
        
        for _ in range(max_retries):
            if self.succ.id == self.id or self.pred.id == self.id:
                logger.info('Stabilization in progress. Retrying...')
                time.sleep(retry_interval)
            else:
                with self.replication_lock:#TODO: Synch should be implemented so i cant delete something that havent been created ?
                    self.pred.enqueue_rep_operation(self.id, data, operation, key)
                    self.succ.enqueue_rep_operation(self.id, data, operation, key)
                break
        else:
            logger.error('Failed to enqueue replication operation after multiple retries.')
        
    def _debug_log_data(self):
        logger.debug(f'Data in node {self.ip}\n{self.data.values()}\nReplic succ\n{self.replicated_data_succ}\nReplic pred:\n{self.replicated_data_pred}')

    #region Coordination

    def check_leader(self):
        """Regularly check the leader availability."""
        while True and not self.election_started:
            logger_le.info('===CHECKING LEADER===')
            try:
                self.leader.ping
            except requests.ConnectionError:
                logger_le.info('Connection with leader ended')
                self.start_election()
            except Exception as e:
                logger_le.error(f"in checking leader: {e}")
                raise e
            logger_le.info('===CHECKING LEADER===')
            time.sleep(10)
    
    def start_election(self):
        self.election_started = True
        logger_le.info(f'Node {self.id} starting an election.')
        self.leader = None  # Clear current leader
        election_message = {'candidates': [(self.id, self.ip)], 'initiator': (self.id, self.ip)}
        self.succ.send_election_message(election_message)
        
    def process_coordinator_message(self, coordinator_message):
        # self.leader = ChordNodeReference(coordinator_message['id'], coordinator_message['ip'])
        self.election_started = False
        if self.id == coordinator_message['initiator'][0]:
            logger_le.info(f'=== Election Done ===')
            return
        self.leader = ChordNodeReference(coordinator_message['leader'][0], coordinator_message['leader'][1])
        logger_le.info(f'Node {self.id} acknowledges new leader: {(self.leader.id, self.leader.ip)}')
        self.succ.send_coordinator_message(coordinator_message)
        
    def process_election_message(self, election_message):
        if not self.election_started:
            self.election_started = True
        if self.id == election_message['initiator'][0]:
            # Election has completed the ring
            self.determine_leader(election_message['candidates'])
        else:
            # Add self to the list of participating nodes and forward the message
            election_message['candidates'].append((self.id, self.ip))
            self.succ.send_election_message(election_message)

    def determine_leader(self, candidates):
        # Find the candidate with the highest id
        new_leader = max(candidates, key=lambda candidate: candidate[0])
        
        # Update the leader information
        self.leader = ChordNodeReference(new_leader[0], new_leader[1])
        logger_le.info(f'Node {self.id} acknowledged the new leader: {(self.leader.id, self.leader.ip)}')
        
        self.notify_all_nodes(new_leader)
        
    def notify_all_nodes(self, leader):
        """Notify all nodes about the new leader."""
        notification_message = {'leader': (leader[0], leader[1]), 'initiator': (self.id, self.ip)}
        self.succ.send_coordinator_message(notification_message)
    
    #region Chord logic

    def _inbetween(self, k: int, start: int, end: int) -> bool:
        """Check if k is in the interval (start, end]."""
        logger.debug(f'Inbetween (k = {k}, start = {start}, end = {end})')
        if int(start) < int(end):
            return int(start) < int(k) <= int(end)
        else:  # The interval wraps around 0
            return int(start) < int(k) or int(k) <= int(end)

    def find_succ(self, id: int) -> 'ChordNodeReference':
        """Find successor of a given id."""
        node = self.find_pred(id)  # Find predecessor of id
        return node.succ  # Return successor of that node

    def find_pred(self, id: int) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.closest_preceding_finger(id)
            logger.debug(f'closest preceding finger found {node.id}')
        logger.debug(f'Pred found {node.id}')
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        for i in range(self.m - 1, -1, -1):
            print(i)
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node.ip == self.ip:
            return
        logger.info(f'Joining to node {node.id}')
        self.pred = self.ref
        self.succ = node.find_successor(self.id)
        logger.info(f'New-Succ-join | {node.id} | node {self.id}')
        self.succ.notify(self.ref)
        time.sleep(10) #To wait a bit for the ring to stabilize
        self.start_election()

    def stabilize(self):
        """Regularly check and stabilize the Chord structure."""
        while True:
            logger_stab.info('===STABILIZING===')
            try:
                logger_stab.info(f'Current successor is {self.succ.ip}')
                if self.pred:
                    logger_stab.info(f'Current predecessor is: {self.pred.ip}')
                else:
                    logger_stab.info('Current predecessor is None')

                x = self.succ.pred
                if x.id != self.id and x and self._inbetween(x.id, self.id, self.succ.id):
                    self.succ = x
                    logger.info(f'New-Succ-Stabilize | {x.id} | node {self.id}')
                self.succ.notify(self.ref)
            # except ConnectionRefusedError:
            except requests.ConnectionError:
                self.succ = self.ref
                # self.pred = self.ref#FIXME: THIS WILL CAUSE PROBLEMS IF THERES A PREDECESOR
                logger.info(f'New-Succ-Stabilize | self | node {self.id}')
            except Exception as e:
                logger_stab.error(f"in stabilize: {e}")
            logger_stab.info('===STABILIZING DONE===')
            time.sleep(10)

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        if node.id != self.id and (not self.pred or self._inbetween(node.id, self.pred.id, self.id)):
            self.pred = node

    def fix_fingers(self):
        """Periodically update finger table entries."""
        while True:
            logger_ff.info('Updating The Finger Table')
            try:
                self.next = (self.next + 1) % self.m
                if self.next == 0:
                    logger_ff.info('Finished Finger Table Iteration')
                    self.print_finger_table()
                self.finger[self.next] = self.find_succ((self.id + 2**self.next) % 2**self.m)
            except Exception as e:
                logger_ff.error(f"Error in fix_fingers: {e}")
            logger_ff.info('===Finger Table Updating Done===')
            time.sleep(10)

    def check_predecessor(self):
        """Periodically check if predecessor is alive."""
        logger_cp.info('Checking Predecessor')
        while True:
            try:
                if self.pred:
                    self.pred.check_predecessor()
            except requests.ConnectionError:#TODO: Debug this code
                self.pred = self.ref
            logger_cp.info('===Predecessor Checking Done===')
            time.sleep(10)
            
    def print_finger_table(self):
        """Print the finger table."""
        logger.debug(f"Finger table for node {self.ip}:{self.port}|{self.id}")
        intervals = []

        for i in range(self.m):
            start = (self.id + 2**i) % 2**self.m
            end = (self.id + 2**(i + 1)) % 2**self.m
            manager_node = self.finger[i]
            intervals.append((start, end, manager_node.ip, manager_node.port))

        # Merge intervals
        merged_intervals = []
        current_start, current_end, current_ip, current_port = intervals[0]

        for start, end, ip, port in intervals[1:]:
            if ip == current_ip and port == current_port:
                current_end = end
            else:
                merged_intervals.append((current_start, current_end, current_ip, current_port))
                current_start, current_end, current_ip, current_port = start, end, ip, port

        merged_intervals.append((current_start, current_end, current_ip, current_port))

        for start, end, ip, port in merged_intervals:
            logger.debug(f"{start}-{end}-{ip}:{port}")
            
    def _send_request(self, url: str, data: dict) -> dict:
        """Send a request and handle retries."""
        max_retries = 4
        for i in range(max_retries):
            response = None
            try:
                logger.info(f'Sending request to {url}\nPayload: {data}')
                response_raw = requests.post(url, json=data).json()
                # response = response_raw.json()#TODO: Remove this after you are done
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError as e:
                logger.error(f'Connection Error in IP {self.ip}')
                logger.error(f'{e}')
                if i == max_retries - 1:
                    raise e
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f'JSON Decode Error: {e}')
                logger.error(f'Response text: {response_raw.text}')  # Log the response body
            except Exception as e:
                logger.error(f"Error sending data to {url}: {data}\n{e}")
                raise e