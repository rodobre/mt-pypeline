from data_pipeline_manager import DataPipelineManager
from random import randint
import sys
import threading

PIPELINE_TEST_CTR = 8

def blue_pipeline():
    blue_pipeline_manager = DataPipelineManager([('127.0.0.1', 38632 + i) for i in range(PIPELINE_TEST_CTR)], 
                                                    [('127.0.0.1', 40312 + i) for i in range(PIPELINE_TEST_CTR)], PIPELINE_TEST_CTR)
    blue_pipeline_manager.start()
    for _ in range(50000):
        print(blue_pipeline_manager.get_packet())

    blue_pipeline_manager.wait_for()

def red_pipeline():
    red_pipeline_manager = DataPipelineManager([('127.0.0.1', 40312 + i) for i in range(PIPELINE_TEST_CTR)],
                                                    [('127.0.0.1', 38632 + i) for i in range(PIPELINE_TEST_CTR)], PIPELINE_TEST_CTR)
    red_pipeline_manager.start()

    for i in range(16384):
        try:
            red_pipeline_manager.schedule_packet(b'A'*randint(104,394))
        except:
            print(f'Crashed at {i}')
            sys.exit()

    red_pipeline_manager.wait_for()

if __name__ == '__main__':
    blue_thread = threading.Thread(target=blue_pipeline)
    red_thread  = threading.Thread(target=red_pipeline)

    blue_thread.start()
    red_thread.start()

    blue_thread.join()
    red_thread.join()