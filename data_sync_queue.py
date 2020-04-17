import queue

class DataQueue:
    def __init__(self, max_size=2048, options={'max_item_size': 2048, 'timeout':3}):
        self.queue_max_size = max_size
        self.queue = queue.Queue(maxsize=max_size)
        self.options = options
    
    def add_packet(self, packet):
        if len(packet) > self.options['max_item_size']:
            print(f'Rejecting packet {packet}')
            return False
        try:
            self.queue.put(packet, True, self.options['timeout'])
        except:
            return False
        return True
    
    def get_packet(self):
        try:
            return self.queue.get(True, self.options['timeout'])
        except:
            return None

    def is_empty(self):
        return self.queue.empty()
    
    def is_full(self):
        return self.queue.full()
    
    def size(self):
        return self.queue.qsize()

    def max_size(self):
        return self.queue_max_size

    def signal_task_done(self):
        self.queue.task_done()