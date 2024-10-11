import argparse
from datetime import datetime, timedelta


def calc_time_diff(t1, t2):
    time_format = "%H:%M:%S.%f"
    ts1 = datetime.strptime(t1, time_format)
    ts2 = datetime.strptime(t2, time_format)
    if ts1 > ts2:
        ts2 += timedelta(days=1)
    return (ts2 - ts1).total_seconds() * 1000


def get_time(file):
    time_start, time_end = None, None
    date_start, date_end = None, None
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.endswith('Step "[step: 03] Creating saturation pods" started'):
                time_start = line.split(' ')[1]
                date_start = line.split(' ')[0][1:]
            if line.endswith('Step "[step: 04] Waiting for saturation pods to be running" ended'):
                time_end = line.split(' ')[1]
                date_end = line.split(' ')[0][1:]
                break
    return time_start, time_end, date_start, date_end


def get_sched_tp(file):
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        index = None
        p50, p99 = None, None
        for i, line in enumerate(lines):
            line = line.strip()
            if line.endswith('SchedulingThroughput: {'):
                index = i
                break
        if index is not None:
            if index + 1 < len(lines):
                p50 = lines[index + 1].split(':')[1][:-2]
                p50 = float(p50)
            if index + 3 < len(lines):
                p99 = lines[index + 3].split(':')[1][:-2]
                p99 = float(p99)
        return p50, p99


def get_latency_pod_startup(file):
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        index = None
        p99_latency = None
        for i, line in enumerate(lines):
            line = line.strip()
            if line.endswith('PodStartupLatency_PodStartupLatency: {'):
                index = i
                break
        if index is not None:
            metric_indices = [index + 11, index + 22, index + 33, index + 44, index + 55]
            if metric_indices[-1] < len(lines):
                for m_i in metric_indices:
                    if lines[m_i].strip().endswith('\"pod_startup\"'):
                        p99_index = m_i - 4
                        p99_latency = lines[p99_index].split(':')[1]
                        p99_latency = float(p99_latency)
        return p99_latency


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate cl2 metrics.')
    parser.add_argument('--file', '-f', type=str, required=True,
                        help='File path to calculate metrics of ClusterLoader2')
    parser.add_argument('--node', '-n', type=int, required=True, help='Number of nodes')
    args = parser.parse_args()
    # avg time to run a saturation pod
    time1, time2, date1, date2 = get_time(args.file)
    if time1 is None or time2 is None:
        print('Cannot find start or end time of saturation pod, calculation canceled.')
    else:
        print(f'Saturation pod scheduling starts from: {date1} {time1}')
        print(f'Ends at: {date2} {time2}')
        diff = calc_time_diff(time1, time2)
        print(f'Time spent for saturation pod: {diff / (1000 * 60):.2f} min')
        print(f'Average time spent for saturation pod: {diff / (args.node * 30):.2f} ms')
    # scheduling throughput
    p50, p99 = get_sched_tp(args.file)
    if p50 is None or p99 is None:
        print('Cannot find scheduling throughput, calculation canceled.')
    else:
        print(f'P50 scheduling throughput: {p50}\nP99 scheduling throughput: {p99}')
    # latency pod startup latency
    lpsl = get_latency_pod_startup(args.file)
    if lpsl is None:
        print('Cannot find latency pod startup latency, calculation canceled.')
    else:
        print(f'P99 latency pod startup latency: {lpsl:.2f} ms')
