import hashlib
import requests
import logging

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

#region NodeReference

class ChordNodeReference:
    def __init__(self, id: str, ip: str, port: int = 8001):
        self.id = get_sha_repr(ip) if not id else id
        self.ip = ip
        self.port = port
        self.replication_queue = []
        
    #region Coordination
    def drop_suc_rep(self):
        self._send_request('/drop-suc-rep', method='get')
        
    def drop_pred_rep(self):
        self._send_request('/drop-pred-rep', method='get')
    
    def send_election_message(self, election_message):
        self._send_request('/election', election_message)

    def send_coordinator_message(self, coordinator_message):
        self._send_request('/coordinator', coordinator_message)
    
    def send_store_data(self, data, callback, key_fields):
        """Send request to store a user."""
        data['callback'] = callback
        data['key_fields'] = key_fields
        self._send_request('/store-data', data)
        
    def send_get_data(self, key, callback):
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
                else:
                    logger.error('Operation not suported')#TODO: implement other operations

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
        response = self._send_request('/get_successor', method='get')
        logger.debug(f'Get Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        """Get predecessor node."""
        response = self._send_request('/get_predecessor', method='get')
        logger.debug(f'Get Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def ping(self):
        self._send_request('/ping', method='get')
    
    @property
    def leader(self):
        return self._send_request('/get-leader', method='get')

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})
        
    def songs_iterations(self, origin_id):
        """Iterate trough all nodes getting all song informations"""
        return self._send_request('/iterate-songs', {'origin': origin_id})

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        response = self._send_request('/closest_preceding_finger', {'id': id})
        return ChordNodeReference(response['id'], response['ip'], self.port)

    #region Utils
        
    def _send_request(self, path: str, data: dict = None, method: str = 'POST', query_params = None) -> dict:
        """Send a request and handle retries."""
        max_retries = 4
        for i in range(max_retries):
            response = None
            try:
                url = f'http://{self.ip}:{self.port}{path}'

                if method.upper() == 'POST':
                    logger.info(f'Sending POST request to {url}\nPayload: {data}')
                    response_raw = requests.post(url, json=data)
                elif method.upper() == 'GET':
                    logger.info(f'Sending GET request to {url}')
                    response_raw = requests.get(url, params=query_params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response = response_raw.json()
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
                raise e
            except Exception as e:
                logger.error(f"Error sending data to {path}: {data}\n{e}")
                raise e
            
    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)
