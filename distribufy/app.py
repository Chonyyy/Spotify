import socket
import sys
import logging
from server.server import start_server

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
    
    logger.setLevel(logging.INFO)

    if len(sys.argv) >= 2:
        other_ip = sys.argv[1]
        start_server(ip, other_ip)
    else:
        start_server(ip)