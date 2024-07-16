import hashlib
import threading
import json
import requests
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from server.handlers.user import User

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.stab")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

#region RequestHandler
class ChordNodeRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        #TODO: Some requests need to be handled in 
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Request path {self.path}')
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None

        if self.path == '/register':
            response = self.handle_register(post_data)
            self.send_json_response(response)
        elif self.path == '/store_user':
            response = self.handle_store_user(post_data)
            self.send_json_response(response)
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
        elif self.path == '/check_predecessor':#TODO: This one and the one bellow should be get requests
            response = {'status': 'success'}
            self.send_json_response(response, status=200)#TODO: change this to get request
        elif self.path == '/ping':
            response = {'status': 'success'}
            self.send_json_response(response, status=200)#TODO: change this to get request
        elif self.path == '/closest_preceding_finger':
            response = self.server.node.closest_preceding_finger(post_data['id'])
            self.send_json_response(response)
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
            self.server.node._print_finger_table()
        elif self.path == '/debug/start-election':
            self.handle_start_election()
            
        else:
            self.send_json_response(response, error_message='Page not found', status=404)#TODO: This response will always be sent, change this

    def handle_register(self, post_data):
        username = post_data['username']
        password = post_data['password']
        user = User(username, password)
        self.server.node.store_user(user)
        return {"status": "success"}

    def handle_store_user(self, post_data):
        username = post_data['username']
        password = post_data['password']
        user = User(username, password)
        self.server.node.data[get_sha_repr(username)] = user.to_dict()
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

    def handle_get_user(self, path):
        query = path.split('?')
        if len(query) > 1:
            username = query[1].split('=')[1]
            return self.server.node.get_user(username)
        else:
            self.send_response(400)
            self.end_headers()
            return

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
        
    #region Coordination
    def send_election_message(self, election_message):
        self._send_request('/election', election_message)

    def send_coordinator_message(self, coordinator_message):
        self._send_request('/coordinator', coordinator_message)

    #region Buisness logic
    def send_store_user(self, user: User):
        """Send request to store a user."""
        data = {'username': user.username, 'password': user.password}
        self._send_request('/store_user', data)

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
            try:
                url = f'http://{self.ip}:{self.port}{path}'
                logger.info(f'Sending request to {url}\nPayload: {data}')

                response = requests.post(url, json=data).json()
                
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError as e:
                logger.error(f'Connection Error in IP {self.ip}')
                logger.error(f'{e}')
                if i == max_retries - 1:
                    raise e
            except Exception as e:
                logger.error(f"Error sending data to {path}: {data}\n{e}")
                raise e
            
    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)

#region ChordNode
class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = self.ref
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}
        self.leader = self.ref
        self.election_started = False#TODO: What happens if the election takes too long

        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self

        logger.info(f'node_addr: {ip}:{port}')

        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.check_leader, daemon=True).start()
          
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
        # notification_message = {'id': leader_id, 'ip': self.get_ip(leader_id)}#TODO: Maybe this makes sense
        notification_message = {'leader': (leader[0], leader[1]), 'initiator': (self.id, self.ip)}
        self.succ.send_coordinator_message(notification_message)
    
    #region buisness logic

    def store_user(self, user: User):
        """Store user in the appropriate node."""
        user_key = get_sha_repr(user.username)
        logger.debug(f'Storing user: {user_key}')
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            self.data[user_key] = user.to_dict()
            logger.info(f'User {user.username} stored at node {self.ip}')
        else:
            target_node.send_store_user(user)

    def get_user(self, username: str) -> dict:
        """Retrieve user from the appropriate node."""
        user_key = get_sha_repr(username)
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            return self.data.get(user_key, None)
        else:
            return target_node.send_get_user(username)

    def list_users(self) -> dict:
        """List all users stored in this node."""
        return {"users": list(self.data.values())}
    
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
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        for i in range(self.m - 1, -1, -1):
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
                self.pred = self.ref#FIXME: THIS WILL CAUSE PROBLEMS IF THERES A PREDECESOR
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
                    self._print_finger_table()
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
            except Exception:
                self.pred = self.ref #TODO: Debug this code
            logger_cp.info('===Predecessor Checking Done===')
            time.sleep(10)
            
    #region Utils
    def _print_finger_table(self):
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