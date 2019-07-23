# coding: utf-8

import argparse
import json
import logging
import re
import socket
import string
import threading 
import unicodedata
import uuid
import locale
import time
from functools import cmp_to_key

from utils import binary_search,merge_sort
locale.setlocale(locale.LC_ALL,'pt_PT.UTF-8')

"""Assign an id to each worker usgin the uuid library"""
worker_id = str(uuid.uuid4())

"""Logger"""
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S',
                    handlers=[
                        logging.FileHandler("{0}/{1}_{2}.log".format('./logs', 'worker', worker_id), mode='w'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger('Worker')

class Worker:
    """
    This class implements the Worker entity, managed by the Coordinator.

    It connects to Coordinator address of a given (host, port).
    It receives tasks for mapping or reducing, 
    giving back the result to the Coordinator.
    """
    def __init__(self, host, port, worker_id):
        # connect to Coordinator address (host, port)
        self.worker_id = worker_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(100)
        self.buffer = ''
        self.host=host
        self.port=port
        while True:
            try:
                self.sock.connect((host, port)) # FIXME worker recebe connection refuse error quando coordenador morre
                #self.register()
                break
            except :
                continue
                #trying in 1 second
        
    def decode_data(self, data: b'') -> str:
        """Decoding of a message using UTF-8"""
        return data.decode('UTF-8')
    
    def encode_data(self, data: list) -> b'':
        """Encoding of a given list of strings using UTF-8""" 
        encoded = b''
        for i in data:
            encoded += i.encode('UTF-8')
        return encoded

    def send_data(self, data):
        """
        Sends data to the connected socket
        
        First, receives data in json format.
        After, that data is encoded with an end of transmission character ('\x04').
        Finally, all the message is sent to the connected socket.        
        """
        while True:
            try:
                e = self.encode_data([data, '\x00\x04'])
                self.sock.sendall(e)
                break
            except BrokenPipeError as e:
                break

            
    def register(self):
        """
        First task of this Worker class.
        
        After connecting to the Coordinator socket, the Worker sends
        a message to it in order to announce itself, with its respectively id.
        """
        json_message = json.dumps({'task': 'register', 'id': self.worker_id})
        self.send_data(json_message)
        logger.info("Register")
    
    def connect(self):
        while True:
            try:
                self.sock.connect((self.host, self.port))
                break
            except :
                continue

    def mapping(self, data):
        
        tokens = data.lower()
        tokens = tokens.translate(str.maketrans('', '', string.digits))
        tokens = tokens.translate(str.maketrans('', '', string.punctuation))
        tokens = tokens.translate(str.maketrans('', '', '«»'))
        tokens = tokens.rstrip()
        final = tokens.split()    

        reduced = []
        final=sorted(final,key=cmp_to_key(locale.strcoll))
        for elem in final:
            if(len(elem)!=0):
                reduced.append((elem.lower(),1))
        
        return reduced
    
    def fill_reduced(self, sorted_arr):
        reduced = []
        str_count = 0
        for i, elem in enumerate(sorted_arr):
            
            str_find = elem[0]
            str_count += elem[1]
            
            if i < (len(sorted_arr)-1):
                next_elem = sorted_arr[i+1]
                str_next = next_elem[0]

                if str_find != str_next:
                    reduced.append((str_find,str_count))
                    str_count = 0
            else:
                reduced.append((str_find,str_count))
                str_count = 0
        return reduced
    
    def reducing(self, data1, data2):

        # returns a list
        reduced1 = self.fill_reduced(data1)
        reduced2 = self.fill_reduced(data2)
        
        if len(reduced1) < len(reduced2):
            main = reduced1
            secondary = reduced2
        else:
            main = reduced2
            secondary = reduced1
            
        arr = []
        for i, elem in enumerate(main):
            str_find = elem[0]
            index = binary_search(secondary, 0, len(secondary)-1, str_find)
            
            if index != -1:
                second_item = secondary[index]
                second_value = second_item[1]
                secondary.remove(second_item)
                temp = main[i]
                temp_str = temp[0]
                final_value = temp[1] + second_value
                arr.append((temp_str, final_value))
            else:
                arr.append(elem)
            
        if len(secondary) != 0:
            arr.extend(secondary)
            merge_sort(arr) 

        return arr  

    def handle_task(self,data):
        """
        Decides what to do with the received message.

        The worker can receive 2 types of messages:
        - 'map_request': sent by the coordinator for map a list of words
        - 'reduce_request': sent by the coordinator for reducing two lists of words
        """
        msg = json.loads(data)
        self.buffer=''
        logger.info('Received %s', msg['task'])
        if msg['task'] == 'map_request':
            reduced = self.mapping(msg['blob'])
            reply = 'map_reply'
            
        elif msg['task'] == 'reduce_request':
            reduced = self.reducing(msg['value'][0], msg['value'][1])
            reply = 'reduce_reply'
            
        msg = json.dumps({'task': reply, 'value': reduced}, ensure_ascii=False)
        self.send_data(msg)


    
    def handle_recv(self, data):
        """
        Called when any piece of a complete message is received.

        It checks if the received piece is the last of the message or not, 
        trying to find the EOT character.
        """
        # decode received data
        recv_buf = self.decode_data(data)
        #logger.debug('Received %s', recv_buf)

        # check if the message is not complete 
        if '\x00\x04' not in recv_buf:
            self.buffer += recv_buf
        else:
            splitted_buf = recv_buf.split('\x00\x04')
            aux = self.buffer + splitted_buf[0]
            self.buffer=''
            self.buffer= splitted_buf[1]
            self.handle_task(aux)
            
    def start_worker(self):
        """Backup starting method, like run() in threads."""
        logger.info("Worker %s started", worker_id)
        
        # register itself in the Coordinator
        self.register()

        while True:
            try:
                data = self.sock.recv(4096)
            except ConnectionResetError:
                break
            
            if not data:
                break
            self.handle_recv(data)
        
            
def main(args):
    logger.debug('Connecting %d to %s:%d', args.id, args.hostname, args.port)

    # Start worker lifetime
    while True:
        worker = Worker(args.hostname, args.port, worker_id)
        worker.start_worker()
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--id', dest='id', type=int, help='worker id', default=0)
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)