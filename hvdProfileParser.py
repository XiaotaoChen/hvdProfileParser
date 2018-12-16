import json
import argparse

top_level_categories = ["NEGOTIATE_ALLREDUCE", "NEGOTIATE_ALLGATHER", "NEGOTIATE_BROADCAST",
                        "ALLREDUCE", "ALLGATHER", "BROADCAST"]
main_categories = ["ALLREDUCE", "ALLGATHER", "BROADCAST"]
# haven't include MPI_ALLREDUCE category
funsion_buffer_categories = ["MEMCPY_IN_FUSION_BUFFER", "MEMCPY_OUT_FUSION_BUFFER"]
MPI_categories = ["MPI_ALLREDUCE"]
ignored_categories = ["INIT_FUSION_BUFFER"]

class MPIOpInfo:
    def __init__(self):
        self.cnts = 0
        self.durs = 0.0

    def analysize(self, begin_event, end_event):
        self.cnts += 1
        self.durs += end_event['ts'] - begin_event['ts']

    def avg(self):
        if self.cnts == 0:
            return 0
        return self.durs/self.cnts/1000.0


class DataLayer:
    def __init__(self, pid, name):
        self.pid = pid
        self.name = name
        self.shape = None
        self.stack = []
        self.mpi_op_top_level_infos = {}
        self.mpi_op_fusion_infos = {}
        self.mpi_op_unfusion_infos = {}
        # for summary
        self.total_avg_time = 0.0
        self.negotiate_avg_time = 0.0
        self.main_avg_time = 0.0

    def push(self, event):
        self.stack.append(event)

    def pop(self):
        return self.stack.pop()

    def update(self):
        for op_name, op_value in self.mpi_op_top_level_infos.items():
            if "NEGOTIATE" in op_name:
                self.negotiate_avg_time = op_value.avg()
            else:
                self.main_avg_time = op_value.avg()
        self.total_avg_time = self.negotiate_avg_time + self.main_avg_time

def update_data_layers(data_layers):
    for data_layer in data_layers:
        data_layer.update()

def sort_data_layers_by_total_time(data_layers):
    return sorted(data_layers, key=lambda data_layer: data_layer.total_avg_time, reverse=True)


def sort_data_layers_by_negotiate_time(data_layers):
    return sorted(data_layers, key=lambda data_layer: data_layer.negotiate_avg_time, reverse=True)

def sort_data_layers_by_main_time(data_layers):
    return sorted(data_layers, key=lambda data_layer: data_layer.main_avg_time, reverse=True)


def ProfileParser(events, operator_name=None):
    if operator_name is not None:
        operator_names = [operator_name]
    else:
        operator_names = ["allreduce", "allgather", "broadcast"]
    data_layers = {}
    # fusion flag
    fusion_flag = False
    for event in events:
        pid = event['pid']
        if event['ph'] == 'M':
            if event['name'] == 'process_name' and pid not in data_layers.keys():
                process_name = event['args']['name']
                for operator_name in operator_names:
                    if operator_name in process_name:
                        data_layers[pid] = DataLayer(pid, process_name)
                        break
            continue
        if pid not in data_layers.keys():
            continue
        data_layer = data_layers[pid]
        if event['ph'] == 'B':
            data_layer.push(event)
        elif event['ph'] == 'E':
            begin_event = data_layer.pop()
            event_name = begin_event['name']
            if event_name in ignored_categories:
                continue
            # top level
            elif event_name in top_level_categories:
                if event_name not in data_layer.mpi_op_top_level_infos.keys():
                    data_layer.mpi_op_top_level_infos[event_name] = MPIOpInfo()
                if event_name in main_categories and data_layer.shape is None:
                    data_layer.shape = event['args']['shape']
                data_layer.mpi_op_top_level_infos[event_name].analysize(begin_event, event)
            # memory_in fusion
            elif event_name == funsion_buffer_categories[0]:
                fusion_flag = True
                if event_name not in data_layer.mpi_op_fusion_infos.keys():
                    data_layer.mpi_op_fusion_infos[event_name] = MPIOpInfo()
                data_layer.mpi_op_fusion_infos[event_name].analysize(begin_event, event)
            # memory_out fusion
            elif event_name == funsion_buffer_categories[1]:
                fusion_flag = False
                if event_name not in data_layer.mpi_op_fusion_infos.keys():
                    data_layer.mpi_op_fusion_infos[event_name] = MPIOpInfo()
                data_layer.mpi_op_fusion_infos[event_name].analysize(begin_event, event)
            # run mpi_allreduce during fusion
            elif event_name in MPI_categories and fusion_flag is True:
                if event_name not in data_layer.mpi_op_fusion_infos.keys():
                    data_layer.mpi_op_fusion_infos[event_name] = MPIOpInfo()
                data_layer.mpi_op_fusion_infos[event_name].analysize(begin_event, event)
            # others
            else:
                if event_name not in data_layer.mpi_op_unfusion_infos.keys():
                    data_layer.mpi_op_unfusion_infos[event_name] = MPIOpInfo()
                data_layer.mpi_op_unfusion_infos[event_name].analysize(begin_event, event)
    return list(data_layers.values())

def OPSummaryStr(data_layer_name, op_infos):
    total_time = sum([op_info.avg() for op_info in op_infos.values()])
    str1 = ""
    str2 = ("%.3f=") % total_time
    str3 = ""
    cnt = -1
    for op_name, op_info in op_infos.items():
        if cnt == -1:
            cnt = op_info.cnts
            str1 += ("%s") % op_name
            tmp_avg = op_info.avg()
            str2 += ("%s") % (round(tmp_avg, 3))
            str3 += ("%s") % (round(100.0 * tmp_avg / total_time, 2))
        else:
            assert cnt == op_info.cnts, "%s the calls of top level ops are not consistent." \
                                        " (%d VS %d)" % (data_layer_name, cnt, op_info.cnts)
            str1 += ("/%s") % op_name
            tmp_avg = op_info.avg()
            str2 += (" + %s") % (round(tmp_avg, 3))
            str3 += ("/%s") % (round(100.0 * tmp_avg / total_time, 2))

    str_summary = ("[%-70s] \ttotal time: %-20s ms/calls \tpercent: %-20s (%%) \t"
                   "cnt: %d calls") % (str1, str2, str3, cnt)

    return str_summary


def PrintAll(data_layers):
    print("horovod performation's summary according to timeline info.")
    for data_layer in data_layers:
        data_shape = [int(i) for i in data_layer.shape.strip("[]").split(",")]
        data_size = 1
        for i in data_shape:
            data_size *= i
        print("%s size: %d, shape:%s :" % (data_layer.name, data_size, data_shape))
        if len(data_layer.mpi_op_top_level_infos) > 0:
            print(OPSummaryStr(data_layer.name, data_layer.mpi_op_top_level_infos))
        if len(data_layer.mpi_op_unfusion_infos) > 0:
            print(OPSummaryStr(data_layer.name, data_layer.mpi_op_unfusion_infos))
        if len(data_layer.mpi_op_fusion_infos) > 0:
            print(OPSummaryStr(data_layer.name, data_layer.mpi_op_fusion_infos))


def add_args(parser):
    parser.add_argument("--file", help="profile json file", default="profile.json")
    parser.add_argument("--operator-name", help="analysize the specify operator", default=None)
    parser.add_argument("--topK", help="select number of k data layers which speed most time", default=5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="horovod profile file analysis",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_args(parser)
    args = parser.parse_args()
    f = open(args.file, 'r')
    events = json.load(f)

    data_layers = ProfileParser(events, args.operator_name)
    update_data_layers(data_layers)
    # sort by total time
    data_layers = sort_data_layers_by_total_time(data_layers)
    PrintAll(data_layers[:5])
    # sort by negotiate time
    data_layers = sort_data_layers_by_negotiate_time(data_layers)
    PrintAll(data_layers[:5])
    # sort by main time
    data_layers = sort_data_layers_by_main_time(data_layers)
    PrintAll(data_layers[:5])
