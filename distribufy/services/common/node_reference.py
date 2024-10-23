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
        self.get_succ_count = 0
        self.get_pred_count = 0

    #region Coordination
    def drop_suc_rep(self):
        self._send_request('/drop-suc-rep', method='get')

    def drop_sec_suc_rep(self):
        self._send_request('/drop-sec-suc-rep', method='get')
    
    def replicate_sec_succ(self):
        self._send_request('/replicate-sec-succ', method='get')
    
    def replicate_succ(self):
        self._send_request('/replicate-succ', method='get')

    def update_sec_succ(self, id, ip):
        self._send_request('/update-sec-succ', {'id': id, 'ip': ip})

    def send_election_message(self, election_message):
        self._send_request('/election', election_message)

    def send_coordinator_message(self, coordinator_message):
        self._send_request('/coordinator', coordinator_message)
        
    def get_songs(self):
        self._send_request('/get-songs', method='get')
    
    def send_store_data(self, data, callback, key_fields):
        """Send request to store a user."""
        data['callback'] = callback
        data['key_fields'] = key_fields
        self._send_request('/store-data', data)

    def get_data(self, key):
        """Send request to get an user"""
        data = {
            'key':key
        }
        response = self._send_request('/get-data-target', data)
        logger.debug(f'Get data Response:\n{response}')
        return response
    
    def get_data_ext(self, key):
        """send request to get an db entry"""
        data = {
            'key':key
        }
        response = self._send_request('/get-data', data)
        logger.debug(f'Get data ext Response:\n{response}')
        return response
    
    def send_get_data(self, key):
        """Send request to get an user"""
        data = {
            'key':key
        }
        response = self._send_request('/get-data-target', data)
        logger.debug(f'send data Response:\n{response}')
        return response
        
    def enqueue_rep_operation(self, source_id, data, operation='insertion', key=None, second_pred = False):
        self.replication_queue.append((source_id, data, operation, key, second_pred))
        logger.debug(f'{self.ip}{self.id} {self.replication_queue}')
    
    def apply_rep_operations(self):
            logger_dt.debug("Apliying rep operations")
            while self.replication_queue:
                source, data, operation, key, second_pred = self.replication_queue.pop()
                if operation == 'insertion':
                    data['source'] = source
                    data['key'] = key
                    data['second_pred'] = second_pred
                    self._send_request('/store-replic', data)
                    # self.succ._send_request('/store-replic', data)
                else:
                    logger.error('Operation not suported')#TODO: implement other operations

    #region Chord logic
    
    def find_successor(self, id: int, origin = None) -> 'ChordNodeReference':
        """Find successor of a given id."""
        if origin:
            logger.debug(f'find successor origin: {origin}')
        response = self._send_request('/find_successor', {'id': str(id), 'origin': origin})
        logger.debug(f'Find Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    def find_predecessor(self, id: int, origin = None) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        if origin:
            logger.debug(f'find predecessor origin: {origin}')
        response = self._send_request('/find_predecessor', {'id': str(id), 'origin': origin})
        logger.debug(f'Find Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    def succ(self, origin = None) -> 'ChordNodeReference':
        """Get successor node."""
        self.get_succ_count += 1
        response = self._send_request(f'/get_successor-{self.get_succ_count}-{origin}', method='get')
        logger.debug(f'Get Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    def pred(self, origin = None) -> 'ChordNodeReference':
        """Get predecessor node."""
        self.get_pred_count += 1
        response = self._send_request(f'/get_predecessor-{self.get_pred_count}-{origin}', method='get')
        logger.debug(f'Get Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def ping(self):
        self._send_request('/ping', method='get')
    
    @property
    def leader(self):
        response = self._send_request('/get-leader', method='get')
        logger.debug(f'Get leader Response:\n{response}')
        return response
    
    @property
    def role(self):
        response = self._send_request('/get-role', method='get')
        logger.debug(f'Get role Response:\n{response}')
        return response

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})
        
    def songs_iterations(self, origin_id):
        """Iterate trough all nodes getting all song informations"""
        response = self._send_request('/iterate-songs', {'origin': origin_id})

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        response = self._send_request('/closest_preceding_finger', {'id': id})
        return ChordNodeReference(response['id'], response['ip'], self.port)

    #region Data Management
    def request_data(self, id: str):
        """Request data after a new join"""
        requested_data = self._send_request('/request_data', {'id': id})
        logger.debug(f'request data Response:\n{requested_data}')
        if 'id' in requested_data:
            return []
        return requested_data

    def absorb_rep_data(self):
        self._send_request('/absorb-rep-data', method='get')

    #region Music Node

    def get_db(self):
        response = self._send_request('/get-db', method='get')
        logger.debug(f'Get db:\n{response}')
        return response
    
    def save_song(self, data):#TODO ver esto
        response = self._send_request('/save-song',data)
        logger.debug(f'Save song Response:\n{response}')
        return data
    
    def get_songs(self):
        response = self._send_request('/get-songs', method='get')
        logger.debug(f'Get songs Response:\n{response}')
        return response

    def song_key_node(self, key):
        response = self._send_request('/get-song-key-node',data= key, method='post')
        logger.debug(f'Get song key node Response:\n{response}')
        return response

    def songs_title_node(self, title):
        response =  self._send_request('/get-songs-title-node',data= title, method='post')
        logger.debug(f'songs_title_node Response:\n{response}')
        return response
        
    def songs_artist_node(self, artist):
        response =  self._send_request('/get-songs-artist-node',data= artist, method='post')
        logger.debug(f'songs_artist_node Response:\n{response}')
        return response
        
    def songs_genre_node(self, genre):
        response =  self._send_request('/get-songs-genre-node',data= genre, method='post')
        logger.debug(f'songs_genre_node Response:\n{response}')
        return response
        
    def get_song_by_key(self, key):
        response =  self._send_request('/get-song-by-key',data= key, method='post')
        logger.debug(f'get_song_by_key Response:\n{response}')
        return response
        
    def get_songs_by_title(self, title):
        response =  self._send_request('/get-songs-by-title',data= title, method='post')
        logger.debug(f'get_songs_by_title Response:\n{response}')
        return response
        
    def get_songs_by_artist(self, artist):
        response =  self._send_request('/get-songs-by-artist',data= artist, method='post')
        logger.debug(f'get_songs_by_artist Response:\n{response}')
        return response
        
    def get_songs_by_genre(self, genre):
        response =  self._send_request('/get-songs-by-genre',data= genre, method='post')
        logger.debug(f'get_songs_by_genre Response:\n{response}')
        return response
        




    #region Utils
        
    def _send_request(self, path: str, data: dict = None, method: str = 'POST', query_params = None) -> dict:
        """Send a request and handle retries."""
        max_retries = 2
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
                logger.error(f'Request path: {path}')
                logger.error(f'Response text: {response_raw.text}')  # Log the response body
                raise e
            except Exception as e:
                logger.error(f"Error sending data to {path}: {data}\n{e}")
                raise e
            
    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)
