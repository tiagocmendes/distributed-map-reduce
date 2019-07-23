import logging
import json
import socket
import time

from async_server import EchoProtocol

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Backup')

class Backup:
    """
    This class implements the fault tolerance mechanism in case that the Coordinator dies.

    It should connect to Coordinator address of a given (host, port).
    It also should receive the datastore list of the Coordinator.
    """
    def __init__(self, host: str, port: int, datastore: list):
        # connect to Coordinator address (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5) 
        self.sock.connect((host, port))
        self.host = host
        self.port = port

        # datastore properties
        self.datastore = datastore
        self.index_datastore = 0
        self.elems = []
        self.messages=[]

        # string buffer of a received message
        self.buffer = ''
    
    def decode_data(self, data: b'') -> str:
        """Decoding of a message using ISO-8859-1"""
        return data.decode('ISO-8859-1')
    
    def encode_data(self, data: list) -> b'':
        """Encoding of a given list of strings using ISO-8859-1""" 
        encoded = b''
        for i in data:
            encoded += i.encode('ISO-8859-1')
        return encoded

    def send_data(self, data):
        """
        Sends data to the connected socket
        
        First, receives data in json format.
        After, that data is encoded with an end of transmission character ('\x04').
        Finally, all the message is sent to the connected socket.        
        """
        e = self.encode_data([data, '\x00\x04'])
        self.sock.sendall(e)
    
    def register(self):
        """
        First task of this Backup class.
        
        After connecting to the Coordinator socket, the Backup sends
        a message to it in order to announce itself.
        """
        self.send_data(json.dumps({'task': 'register_backup'}))
    
    def handle_task(self, data):
        """
        Decides what to do with the received message.

        In most of the cases, it will only update the self.index_datastore 
        counter and also the self.elems list.
        """
        msg=''
        try:
            msg = json.loads(data)
        except Exception as e:
            logger.debug("Exception %s", e)
        
        logger.info('Received %s', msg)
        if msg != "":
            if msg['task'] == 'backup':
                self.messages = msg['values']

    def get_index_datastore(self) -> int:
        return self.index_datastore

    def get_elems(self) -> list:
        return self.elems

    def get_messages(self) -> list:
        return sorted(self.messages,key=lambda r:r[2])    
    
    def handle_recv(self, data: b''):
        """
        Called when any piece of a complete message is received.

        It checks if the received piece is or not the last of the message, 
        trying to find the EOT character.
        """

        # decode received data
        recv_buf = self.decode_data(data)
        logger.debug('Received buffer: %s', recv_buf)
        
        # check if the message is not complete 
        if '\x00\x04' not in recv_buf:
            self.buffer += recv_buf
        else:
            splitted_buf = recv_buf.split('\x00\x04')
            aux = self.buffer + splitted_buf[0]
            self.handle_task(aux)

            for i in splitted_buf[1:]:
                if (('{'in i)&('}'in i)):
                    self.handle_task(i)
                else:
                    self.buffer =i 
            
    def start_backup(self):
        """Backup starting method, like run() in threads."""
        logger.info("Backup started")
        
        # register itself in the Coordinator
        self.register()

        # receives messages until the Coordinator socket closes
        while True:
            data = self.sock.recv(4096)
            if not data:
                break
            self.handle_recv(data)

        self.sock.close()
        