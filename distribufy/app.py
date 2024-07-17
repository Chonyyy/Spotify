import socket
import sys
import logging
import threading
from server.server import start_server
from server.utils.multicast import send_multicast, receive_multicast

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    ip = socket.gethostbyname(socket.gethostname())

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = "%H:%M:%S"
    
    logging.basicConfig(
        filename= f'logs_for_{ip}.log',
        format= log_format,
        datefmt= date_format,
        filemode= 'w')
    
    logger.setLevel(logging.DEBUG)
    database_name = f'db_{ip}'
    
    if len(sys.argv) >= 2:
        role = str(sys.argv[1])
        start_server(ip, role=role, db_name=database_name)
    else:
        start_server(ip, db_name=database_name)
