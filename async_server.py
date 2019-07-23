import asyncio
import logging
import selectors
import socket
import sys

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('EchoProtocol')

class EchoProtocol(asyncio.BaseProtocol):
    """
    This class implements a callback based server using asynchronous programming.

    This class is only responsible for establishing the communication
    beetween the Coordinator and all the clients (Workers and Backup),
    receiving their messages.
    It receives in its constructor a Coordinator object, in order to
    handle the received message.
    """
    def __init__(self, coordinator):
        self.coordinator = coordinator
        
    def connection_made(self, transport):
        """Called when a new connection is made"""
        self.transport = transport 
        self.sock = transport.get_extra_info('socket')
        self.peername = transport.get_extra_info('peername')
        logger.info('Worker registered from %s', self.peername)
    
    def data_received(self, data):
        """Called when any data was received"""
        self.coordinator.handle_recv(self.sock, data)

    def eof_received(self):
        """Called when any connection is closed"""
        self.coordinator.handle_hangup(self.sock)
         