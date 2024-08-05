from server.handlers.file_transfer_handler import FileTransferHandler
from server.chord_node import ChordNode
from server.gateway_node import GatewayNode
from server.utils.my_orm import JSONDatabase
import logging

logger = logging.getLogger("__main__")

def initialize_database(role, filepath):
    columns = None
    replic_columns = None
    if role == 'music_info':
        columns = ['id','title', 'album', 'genre', 'artist']
        replic_columns = ['id','title', 'album', 'genre', 'artist', 'source']
    elif role == 'music_ftp':
        columns = ['id', 'addr']
        replic_columns = ['id', 'addr', 'columns']
    elif role == 'gateway':
        columns  = ['id', 'ip', 'role']
    return JSONDatabase(filepath, columns), JSONDatabase(filepath + 'pred', replic_columns), JSONDatabase(filepath + 'succ', replic_columns)

def start_server(ip, other_ip=None, role = 'music_info', db_name = 'db'):
    print(f'Launching App on {ip}')
    db, pred_db, succ_db = initialize_database(role, db_name)
    if role == 'gateway':
        node = GatewayNode(ip, db = db, pred_db = pred_db, succ_db = succ_db, role=role)
    else:
        node = ChordNode(ip, db = db, pred_db = pred_db, succ_db = succ_db, role=role)

    while True:
        pass