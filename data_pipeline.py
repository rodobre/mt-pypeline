from data_sync_queue import DataQueue
import time
import threading
import socket

# Data pipelines are bidirectional and are to be used according to the user's integration
class DataPipeline:
    def __init__(self, paired_pipeline_conn, bind_to=('0.0.0.0', 25461), packet_size=2048, sock_timeout=5):
        self.max_retries = 5
        self.sock_timeout = 5
        self.packet_size = packet_size
        self.recv_queue = DataQueue()
        self.send_queue = DataQueue()
        self.recv_bind_to = bind_to
        self.send_conn_to = paired_pipeline_conn
        self.send_socket = None
        self.recv_socket = None
        self.send_thread = threading.Thread(target=self.send_thread_pfn)
        self.recv_thread = threading.Thread(target=self.recv_thread_pfn)
        self.paired_socket = None
        self.send_queue_waiting = False
        self.recv_queue_waiting = False
    
    def get_recv_queue(self):
        return self.recv_queue
    
    def get_send_queue(self):
        return self.send_queue
    
    def get_send_queue_state(self):
        return self.send_queue_waiting

    def is_send_queue_full(self):
        return self.send_queue.is_full()

    def is_send_queue_empty(self):
        return self.send_queue.is_empty()

    def is_recv_queue_full(self):
        return self.recv_queue.is_full()

    def is_recv_queue_empty(self):
        return self.recv_queue.is_empty()

    def get_recv_queue_state(self):
        return self.recv_queue_waiting
    
    def start_service(self):
        self.init_recv_socket()
        self.init_send_socket()
        
    def wait_for(self):
        if self.send_thread == None or self.recv_thread == None:
            raise Exception('Attempted to join threads of value \'None\'')

        self.send_thread.join()
        self.recv_thread.join()

    def init_recv_socket(self, retries = 0):
        if retries > self.max_retries:
            raise Exception("Could not initialize the receiving socket of the data pipeline, over 5 retries")

        try:
            self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.recv_socket.bind(self.recv_bind_to)
            self.recv_socket.listen(self.sock_timeout)
            if self.recv_thread == None:
                self.recv_thread = threading.Thread(target=self.recv_thread_pfn)
            if not self.recv_thread.is_alive():
                self.recv_thread.start()
        except:
            time.sleep(1)
            self.init_recv_socket(retries + 1)
    
    def init_send_socket(self, retries = 0):
        if retries > self.max_retries:
            raise Exception("Could not initialize the sending socket of the data pipeline, over 5 retries")

        try:
            self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.send_socket.connect(self.send_conn_to)
            if self.send_thread == None:
                self.send_thread = threading.Thread(target=self.send_thread_pfn)
            if not self.send_thread.is_alive():
                self.send_thread.start()
        except:
            time.sleep(1)
            self.init_send_socket(retries + 1)

    def recv_thread_pfn(self):
        packet = None
        while True:
            packet = None
            if self.recv_socket == None:
                self.init_recv_socket()
                time.sleep(1)
            
            if self.paired_socket == None:
                try:
                    self.paired_socket = self.recv_socket.accept()
                except:
                    self.paired_socket = None
                    continue

            try:
                packet = self.paired_socket[0].recv(self.packet_size)
            except:
                self.recv_socket.close()
                self.recv_socket = None
                self.init_recv_socket()
                time.sleep(1)
                continue
            
            if packet == None:
                continue
            
            self.recv_queue.add_packet(packet)
    
    def send_thread_pfn(self):
        packet = None
        while True:
            packet = None

            try:
                self.send_queue_waiting = True
                packet = self.send_queue.get_packet()
                self.send_queue.signal_task_done()
                self.send_queue_waiting = False
            except:
                self.send_queue_waiting = False
                packet = None
                continue

            if packet == None:
                continue

            if self.send_socket == None:
                self.init_send_socket()
                time.sleep(1)

            try:
                self.send_socket.send(packet)
            except:
                self.send_socket.close()
                self.send_socket = None
                self.init_send_socket()
                time.sleep(1)
                continue
    
    def get_received_packet(self):
        packet = None

        try:
            self.recv_queue_waiting = True
            packet = self.recv_queue.get_packet()
            self.recv_queue.signal_task_done()
            self.recv_queue_waiting = False
        except:
            self.recv_queue_waiting = False
            packet = None
        
        return packet
    
    def queue_send_packet(self, packet):
        if len(packet) > self.packet_size:
            raise Exception(f'Cannot enqueue packet larger than queue element size! Specified size: {len(packet)}, expected: <= {self.packet_size}')

        success = True
        try:
            success = self.send_queue.add_packet(packet)
        except:
            pass

        return success
