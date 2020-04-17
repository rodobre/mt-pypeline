from data_pipeline import DataPipeline

class DataPipelineManager:
    def __init__(self, bind_addresses, send_addresses, pipeline_count=8, packet_size=2048, sock_timeout=5):
        if len(bind_addresses) != len(send_addresses):
            raise Exception(f'Bind address array length is not equal to the send address array length. {len(bind_addresses)} vs {len(send_addresses)}')

        if len(bind_addresses) != pipeline_count:
            raise Exception(f'Pipeline count is not equal to the length of the supplied arrays. {len(bind_addresses)} vs {pipeline_count}')

        self.pipeline_count = pipeline_count
        self.data_pipelines = [None] * self.pipeline_count

        for i in range(self.pipeline_count):
            self.data_pipelines[i] = DataPipeline(send_addresses[i], bind_addresses[i], packet_size, sock_timeout)
    
    def start(self):
        for i in range(self.pipeline_count):
            self.data_pipelines[i].start_service()

    def wait_for(self):
        for i in range(self.pipeline_count):
            self.data_pipelines[i].wait_for()
    
    def schedule_packet(self, packet):
        non_full_pipelines = []
        pipeline = None

        for i in range(self.pipeline_count):
            pipeline = self.data_pipelines[i]
            if pipeline == None:
                raise Exception(f'Encountered uninitialized pipeline at index {i}')
            if pipeline.get_send_queue_state() == True:
                if pipeline.is_send_queue_full():
                    continue
                pipeline.queue_send_packet(packet)
                return

            if not pipeline.is_send_queue_full():
                non_full_pipelines.append(i)
        
        for i in non_full_pipelines:
            pipeline = self.data_pipelines[i]
            if pipeline == None:
                raise Exception(f'Encountered uninitialized pipeline at index {i}')
            if not pipeline.is_send_queue_full():
                pipeline.queue_send_packet(packet)
                return
        
        raise Exception(f'All {self.pipeline_count} pipelines are busy!')

    def get_packet(self):
        largest_queue = 0

        for i in range(self.pipeline_count):
            pipeline = self.data_pipelines[i]
            if pipeline == None:
                raise Exception(f'Encountered uninitialized pipeline at index {i}')
            recv_queue = pipeline.get_recv_queue()

            if pipeline.is_recv_queue_full():
                return pipeline.get_received_packet()
            
            if i == 0:     
                largest_queue = (recv_queue.max_size() - recv_queue.size(), 0)
            else:
                tmp_large_queue = recv_queue.max_size() - recv_queue.size()
                if tmp_large_queue > largest_queue[0]:
                    largest_queue = (tmp_large_queue, i)
        
        return self.data_pipelines[largest_queue[1]].get_received_packet()