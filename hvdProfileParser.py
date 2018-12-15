import json
import argparse

class MPIOpInfo:
    def __init__(self):
        self.cnts = 0
        self.durs = 0.0
    def analysize(self, begin_event, end_event):
        self.cnts += 1
        self.durs += end_event['ts'] - begin_event['ts']

class DataLayer:
    def __init__(self, pid, name):
        self.pid = pid
        self.name = name
        self.stack = []
        self.mpi_op_infos = {}
    def push(self, event):
        self.stack.append(event)
    def pop(self):
        return self.stack.pop()

def add_args(parser):
    parser.add_argument("--file",
                        help="profile json file", default="profile.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="horovod profile file analysis",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_args(parser)
    args = parser.parse_args()
    f = open(args.file, 'r')
    # f = open("/home/xiaotaoc/timelines/40cores_timeline.json", 'r')
    events = json.load(f)    
    data_layers = {}
    
    assert isinstance(events, list)
    for event in events:
        pid = event['pid']
        if event['ph'] == 'M':
            if event['name'] == 'process_name' and pid not in data_layers.keys():
                data_layers[pid] = DataLayer(pid, event['args']['name'])
            continue
        data_layer = data_layers[pid]
        if event['ph'] == 'B':
            data_layer.push(event)
        elif event['ph'] == 'E':
            begin_event = data_layer.pop()
            event_name = begin_event['name']
            if event_name not in data_layer.mpi_op_infos.keys():
                data_layer.mpi_op_infos[event_name] = MPIOpInfo()
            data_layer.mpi_op_infos[event_name].analysize(begin_event, event)
    
    # print all
    for data_layer in data_layers.values():
        print("%s parameters summary:" % data_layer.name)
        for mpi_op_name, mpi_op_info in data_layer.mpi_op_infos.items():
            print("%s    time: %.3f ms/calls     %d calls" % (mpi_op_name, mpi_op_info.durs/mpi_op_info.cnts, mpi_op_info.cnts))
        

                
                
            
        

    
