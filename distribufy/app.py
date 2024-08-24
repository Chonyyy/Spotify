import socket
import sys
import logging
from services.common.my_orm import JSONDatabase
from server import start_server
from services.common.multicast import send_multicast, receive_multicast

logger = logging.getLogger(__name__)


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
        columns  = ['id', 'ip', 'port', 'role']
    elif role == 'chord_testing':
        columns  = ['key', 'value']
        replic_columns = ['key', 'value']
    return JSONDatabase(filepath, columns), JSONDatabase(filepath + 'pred', replic_columns), JSONDatabase(filepath + 'succ', replic_columns)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception('database role must be provided')

    # Getting ip Address
    ip = socket.gethostbyname(socket.gethostname())

    # Configuring Logger
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = "%H:%M:%S"
    
    logging.basicConfig(
        filename= f'logs_for_{ip}.log',
        format= log_format,
        datefmt= date_format,
        filemode= 'w')
    
    logger.setLevel(logging.DEBUG)# TODO: Set this from input

    # Starting server
    database_name = f'db_{ip}'
    role = str(sys.argv[1])
    db, pred_db, succ_db = initialize_database(role, database_name)
    start_server(ip, role=role, db=db, pred_db=pred_db, succ_db=succ_db)
