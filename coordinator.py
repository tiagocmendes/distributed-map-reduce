# coding: utf-8

import argparse
import asyncio
import json
import logging
import sys
import uuid
import selectors
import socket
import time
import csv
import queue
from datetime import datetime
from async_server import EchoProtocol
import backup 

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S',
                    handlers=[
                        logging.FileHandler("{0}/{1}.log".format('./logs', 'coordinator'), mode='w'),
                        logging.StreamHandler()
                ])

logger = logging.getLogger('Coordinator')

class Coordinator:
    """
    This class implements the Coordinator entity, resposible for managing workers.

    It should bind itself to an address of a given (host, port)
    It will deliver tasks to workers (map or reduce) in the most efficient way.
    It is also responsible for sending backup messages to its Backup, in case
    of a failure.
    The Backup could also transform itself in a Coordinator.
    """
    def __init__(self, datastore, index_datastore = 0, elems = []):
        # text processing status
        self.datastore = datastore
        self.index_datastore = index_datastore
        self.elems = elems

        # sent and lost messages
        self.messages = {}
        self.message_lost = queue.Queue()
        self.messages_send_to_backup={}

        # workers and backup connections
        self.connections = {}
        self.backup_conn = None
        self.do_reduce = False

        # exit condition
        self.final_count = 0 
    
    def decode_data(self, data):
        """Decoding of a message using UTF-8"""
        try:
            return data.decode('UTF-8')
        except Exception as e:
            logger.debug('Exception %s', e)

    def encode_data(self, data):
        """Encoding of a given list of strings using UTF-8""" 
        encoded = b''
        for i in data:
            encoded += i.encode('UTF-8')
        return encoded

    def send_data(self, conn, data):
        """
        Sends data to the connected socket
        
        First, receives data in json format.
        After, that data is encoded with an end of transmission character ('\x04').
        Finally, all the message is sent to the connected socket.        
        """
        try:
            e = self.encode_data([data, '\x00\x04'])
            conn.sendall(e)
            self.messages[conn] = data
        except BrokenPipeError:
            logger.info("BrokenPipeERROR")

    def send_backup(self, data):
        """
        Sends data to the Backup.
        
        It checks if the coordinator has a Backup and sends the message to it.
        """
        try:
            if self.backup_conn is not None:
                self.send_data(self.backup_conn, data)
        except OSError as ose: 
            logger.debug('OSError: %s', ose)

    def handle_hangup(self, conn):
        """
        Handles the death of a worker.

        When a worker dies, the last message sent to him is placed in a queue of 
        lost messages and then forwarded to another worker.
        """
        logger.info("Hangup")
        if conn != self.backup_conn and conn in self.connections:
            del self.connections[conn]
            msg = self.messages[conn]
            self.message_lost.put(msg)
        
    
    def finish_processing(self):
        """
        Called when the text processing is finished.

        It prints out to terminal the final list, and saves it to a .csv file.
        Finally, the process is finished.
        """
        logger.info('List finished %s', self.elems)
        with args.out as f:
            csv_writer = csv.writer(f, delimiter=',',
            quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for w,c in self.elems[0]:
                csv_writer.writerow([w,c])
        sys.exit()

    def worker_registry(self, conn, worker_id):
        """
        Register of a Worker.

        When a Worker connects to the Coordinator, it first sends a register 
        message, in order to start communicating with it.
        The Coordinator receives that message and saves the worker connection in 
        self.connections dictionary, with the value beeing a list [buffer, worker_id].
        """

        self.connections[conn][1] = worker_id

        logger.info('Worker registered with id %s', worker_id)

        if self.index_datastore < len(self.datastore):
            # task response
            json_msg = json.dumps({'task': 'map_request', 'blob': self.datastore[self.index_datastore]})
            self.send_data(conn, json_msg)

            # send to backup
            self.messages_send_to_backup[conn] = (self.elems,self.index_datastore,datetime.now().timestamp())
            self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))
            
            self.index_datastore += 1

        else:
            if len(self.elems) < 2:
                self.messages_send_to_backup[conn]=(self.elems,self.index_datastore,datetime.now().timestamp())

                self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))

                self.final_count += 1

                if self.backup_conn != None :
                    if self.final_count == len(self.connections) - 1:
                        self.finish_processing()
                else:
                    if self.final_count == len(self.connections):
                        self.final_processing()
            else:
                # task response
                json_msg = json.dumps({'task': 'reduce_request', 'value': [self.elems[0], self.elems[1]]})
                self.send_data(conn, json_msg)

                # send to backup
                self.messages_send_to_backup[conn]=(self.elems,self.index_datastore,datetime.now().timestamp())
                self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values())}))
                del self.elems[1]
                del self.elems[0]
                
    def handle_task(self, conn, data):
        """
        Decides what to do with the received message.
        
        The coordinator can receive 4 types of messages:
        - 'register': sent by a worker to announce itself
        - 'register_backup': sent by the backup to announce itself
        - 'map_reply': sent by a worker in response of a 'map_request'
        - 'reduce_reply': sent by a worker in response of a 'reduce_request'
        """
        msg = json.loads(data)
        
        logger.info('Task: %s, current blob: %s', msg['task'], self.index_datastore)

        if msg['task'] == 'register':
            self.worker_registry(conn, msg['id'])

        elif msg['task'] == 'map_reply' or msg['task'] == 'reduce_reply':
            
            self.elems.append(msg['value'])

            if len(self.elems) < 2:


                if self.index_datastore < len(self.datastore):
                    if self.message_lost.qsize() == 0:
                        # task response
                        json_msg = json.dumps({'task': 'map_request', 'blob': self.datastore[self.index_datastore]})
                        self.send_data(conn, json_msg)

                        #send to backup
                        self.messages_send_to_backup[conn]=(self.elems,self.index_datastore,datetime.now().timestamp())
                        self.send_backup(json.dumps({'task':'backup', 'values':list(self.messages_send_to_backup.values()) }))
                        
                        self.index_datastore += 1

                    else:
                        # send lost message
                        msg = self.message_lost.get()
                        self.send_data(conn,msg)

                        # send to backup
                        self.messages_send_to_backup[conn]=(self.elems,self.index_datastore,datetime.now().timestamp())
                        self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))


                else:
                    if not self.do_reduce:
                        if len(self.datastore) == 1:
                            value = [self.elems[0], []]
                            self.messages_send_to_backup[conn ]= (self.elems,self.index_datastore, datetime.now().timestamp())

                            del self.elems[0]
                        else:
                            value=[self.elems[0], self.elems[1]]
                            self.messages_send_to_backup[conn] = (self.elems,self.index_datastore, datetime.now().timestamp())

                            del self.elems[1]
                            del self.elems[0]
                        
                        if self.message_lost.qsize() == 0:
                            self.do_reduce = True
                            # task response
                            json_msg = json.dumps({'task': 'reduce_request', 'value': value})
                            self.send_data(conn, json_msg)

                            # send to backup
                            self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values())}))
                    
                        else:
                            # send lost message
                            msg = self.message_lost.get()
                            self.send_data(conn,msg)

                            # send to backup
                            self.messages_send_to_backup[conn] = (self.elems,self.index_datastore,datetime.now().timestamp())
                            self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))
                    else:
                        self.final_count+=1
                        print(self.final_count)
                        print(len(self.connections))
                        if(self.backup_conn!=None):

                            if(self.final_count==len(self.connections)-1):#quando um worker termina o seu trabalho incrementamos o finalcount, mas so quando todos os workers termianrem e que imprimimos a lista final com as palavras
                                self.finish_processing()

                        else:
                            if self.final_count == len(self.connections):
                                self.finish_processing()
            else:
                if len(self.datastore) == 1:
                    value = [self.elems[0], []]

                    # send to backup
                    self.messages_send_to_backup[conn]=(self.elems, self.index_datastore, datetime.now().timestamp())
                    self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values())}))
                    del self.elems[0]
                else:
                    value = [self.elems[0], self.elems[1]]

                    # send to backup
                    self.messages_send_to_backup[conn] = (self.elems,self.index_datastore,datetime.now().timestamp())
                    self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))
                    del self.elems[1]
                    del self.elems[0]
         
                if self.message_lost.qsize() == 0:
                    self.do_reduce = True
                    json_msg = json.dumps({'task': 'reduce_request', 'value': value})
                    self.send_data(conn, json_msg)

                    # send to backup
                    self.messages_send_to_backup[conn] = (self.elems,self.index_datastore,datetime.now().timestamp())
                    
                else:
                    # send lost message
                    msg = self.message_lost.get()
                    self.send_data(conn,msg)
        
        elif msg['task'] == 'register_backup':
            self.backup_conn = conn
            self.send_backup(json.dumps({'task':'backup', 'values': list(self.messages_send_to_backup.values()) }))
        
    def handle_recv(self, conn, data):
        """
        Called when any piece of a complete message is received.
        
        It checks if the received piece is the last of the message or not, 
        trying to find the EOT character.
        """
        recv_buf = self.decode_data(data)
        logger.debug('Received %s', recv_buf)

        if conn not in self.connections:
            self.connections[conn] = ['', 0]

        # check if end of transmission
        if '\x00\x04' not in recv_buf:
            self.connections[conn][0] += recv_buf
        else:
            splitted_buf = recv_buf.split('\x00\x04')
            aux = self.connections[conn][0] + splitted_buf[0]
            self.connections[conn][0] = splitted_buf[1]
            self.handle_task(conn, aux)

async def main(args):
    datastore = []
    # Read the file passed by argument and break it into blobs
    with args.file as f:
        while True:
            blob = f.read(args.blob_size)
            if not blob:
                break
            # This loop is used to not break word in half
            while not str.isspace(blob[-1]):
                ch = f.read(1)
                if not ch:
                    break
                blob += ch
            logger.debug('Blob: %s', blob)
            datastore.append(blob)
    logger.debug('Size: '+str(len(datastore)))

    coordinator = Coordinator(datastore)
    
    try:
        logger.info('Starting coordinator')
        loop = asyncio.get_event_loop()
        server = await loop.create_server(lambda: EchoProtocol(coordinator), args.host, args.port)
        await server.serve_forever()

    except:
        logger.info('Starting backup')
        backup_coord = backup.Backup(args.host, args.port, datastore)
        backup_coord.start_backup()
        
        # if coordinator dies
        message_backup = backup_coord.get_messages()
        elems = message_backup[0][0]
        index = message_backup[0][1]

        # backup is the new coordinator
        coordinator = Coordinator(datastore,index,elems)

        loop = asyncio.get_event_loop()
        server = await loop.create_server(lambda: EchoProtocol(coordinator), args.host, args.port)
        await server.serve_forever()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-l', dest='host', type=str, help='coordinator host', default='127.0.0.1')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r', encoding='UTF-8'), help='input file path')
    parser.add_argument('-o', dest='out', type=argparse.FileType('w', encoding='UTF-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()

    # Call to asynchronous main function
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()