import argparse
import os
from datetime import datetime
import time
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.utils import prom_query


def start_and_end_line(filepath):
    sl = ''
    el = ''
    with open(filepath, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line.endswith('get or create res list complete'):
                sl = line
            if line.endswith('start clear program'):
                el = line
    return sl, el


def first_and_last_nonempty_line(filepath):
    first_line = None
    last_nonempty_line = None
    with open(filepath, 'r', encoding='utf-8') as file:
        first_line = file.readline().strip()
        # 移动到文件末尾
        file.seek(0, os.SEEK_END)
        position = file.tell()
        buffer = ''
        # 逐渐向文件开头移动，寻找最后一个非空行
        while position > 0:
            file.seek(position - 1)
            char = file.read(1)
            if char == '\n':
                if buffer.strip():
                    last_nonempty_line = buffer.strip()
                    break
                buffer = ''
            else:
                buffer = char + buffer
            position -= 1
        # 如果文件是空的或者只有一行，处理这种特殊情况
        if not last_nonempty_line and first_line:
            last_nonempty_line = first_line

    return first_line, last_nonempty_line


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script to get K8s request latency.')
    parser.add_argument('--file', '-f', type=str, required=True, help='File path to calculate latency')
    args = parser.parse_args()
    start_line, end_line = start_and_end_line(args.file)
    if start_line == '' or end_line == '':
        print('Cannot find time when stress test starts or ends')
    else:
        # first_line, last_line = first_and_last_nonempty_line(args.file)
        start_str = start_line.split(' ')[0] + ' ' + start_line.split(' ')[1]
        end_str = end_line.split(' ')[0] + ' ' + end_line.split(' ')[1]
        print(f'Start from: {start_str}\nEnd at: {end_str}')
        time_format = '%Y/%m/%d %H:%M:%S'
        datetime_start = datetime.strptime(start_str, time_format)
        start_unix = int(time.mktime(datetime_start.timetuple()))
        datetime_end = datetime.strptime(end_str, time_format)
        end_unix = int(time.mktime(datetime_end.timetuple()))
        interval = end_unix - start_unix
        if interval > 600:
            interval = 600
            end_unix = start_unix + interval
            print(f'(Calibrate end at: {datetime.fromtimestamp(end_unix)})')
        promql_avg_all = ('sum(increase(apiserver_request_duration_seconds_sum{verb!=\'WATCH\'}[%ds]))/sum(increase('
                          'apiserver_request_duration_seconds_count{verb!=\'WATCH\'}[%ds]))'
                          % (interval, interval))
        promql_avg_get = ('sum(increase(apiserver_request_duration_seconds_sum{verb=\'GET\'}[%ds]))/sum(increase('
                          'apiserver_request_duration_seconds_count{verb=\'GET\'}[%ds]))'
                          % (interval, interval))
        promql_avg_list_namespace = ('sum(increase(apiserver_request_duration_seconds_sum{verb=\'LIST\', '
                                     'scope=\'namespace\'}[%ds]))/sum(increase(apiserver_request_duration_seconds_count'
                                     '{verb=\'LIST\',scope=\'namespace\'}[%ds]))'
                                     % (interval, interval))
        promql_avg_list_cluster = ('sum(increase(apiserver_request_duration_seconds_sum{verb=\'LIST\','
                                   'scope=\'cluster\'}[%ds]))/sum(increase('
                                   'apiserver_request_duration_seconds_count{verb=\'LIST\',scope=\'cluster\'}[%ds]))'
                                   % (interval, interval))
        promql_avg_mutating = ('sum(increase(apiserver_request_duration_seconds_sum{'
                               'verb=~\'DELETE|PATCH|POST|PUT\'}[%ds]))/sum('
                               'increase(apiserver_request_duration_seconds_count{'
                               'verb=~\'DELETE|PATCH|POST|PUT\'}[%ds]))'
                               % (interval, interval))
        promql_p99_get = ('histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{verb=\'GET\'}['
                          '%ds]))by(le))' % interval)
        promql_p99_list_namespace = ('histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{'
                                     'verb=\'LIST\',scope=\'namespace\'}[%ds]))by(le))' %
                                     interval)
        promql_p99_list_cluster = ('histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{'
                                   'verb=\'LIST\',scope=\'cluster\'}[%ds]))by(le))' %
                                   interval)
        promql_p99_mutating = ('histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{'
                               'verb=~\'DELETE|PATCH|POST|PUT\'}[%ds]))by(le))' %
                               interval)
        promql_num_all = ('sum(increase(apiserver_request_duration_seconds_count{verb!=\'WATCH\'}[%ds]))' % interval)
        promql_num_get = ('sum(increase(apiserver_request_duration_seconds_count{verb=\'GET\'}[%ds]))' % interval)
        promql_num_list_namespace = ('sum(increase(apiserver_request_duration_seconds_count{verb=\'LIST\','
                                     'scope=\'namespace\'}[%ds]))' % interval)
        promql_num_list_cluster = ('sum(increase(apiserver_request_duration_seconds_count{verb=\'LIST\','
                                   'scope=\'cluster\'}[%ds]))' % interval)
        promql_num_mutating = ('sum(increase(apiserver_request_duration_seconds_count{'
                               'verb=~\'DELETE|PATCH|POST|PUT\'}[%ds]))' % interval)
        option_time = f'&time={end_unix}'
        try:
            avg_all_data = prom_query.get_prometheus_data(query=promql_avg_all, range_query=False, options=option_time)
            avg_all_latency = 1000 * float(avg_all_data[0]['value'][1])
            print(f'Average latency of all requests: {avg_all_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying avg latency data from prometheus: {e}')
        try:
            num_all_data = prom_query.get_prometheus_data(query=promql_num_all, range_query=False, options=option_time)
            num_all = float(num_all_data[0]['value'][1])
            print(f'# requests: {int(num_all)}')
        except Exception as e:
            print(f'An error occurred while querying # req data from prometheus: {e}')
        try:
            avg_get_data = prom_query.get_prometheus_data(query=promql_avg_get, range_query=False, options=option_time)
            avg_get_latency = 1000 * float(avg_get_data[0]['value'][1])
            print(f'Average latency of GET: {avg_get_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying avg GET latency data from prometheus: {e}')
        try:
            p99_get_data = prom_query.get_prometheus_data(query=promql_p99_get, range_query=False, options=option_time)
            p99_get_latency = 1000 * float(p99_get_data[0]['value'][1])
            print(f'P99 latency of GET: {p99_get_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying p99 GET latency data from prometheus: {e}')
        try:
            num_get_data = prom_query.get_prometheus_data(query=promql_num_get, range_query=False, options=option_time)
            num_get = float(num_get_data[0]['value'][1])
            print(f'# GET requests: {int(num_get)}')
        except Exception as e:
            print(f'An error occurred while querying # GET req data from prometheus: {e}')
        try:
            avg_list_ns_data = prom_query.get_prometheus_data(query=promql_avg_list_namespace, range_query=False,
                                                              options=option_time)
            avg_list_ns_latency = 1000 * float(avg_list_ns_data[0]['value'][1])
            print(f'Average latency of LIST(namespace): {avg_list_ns_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying avg LIST(namespace) latency data from prometheus: {e}')
        try:
            p99_list_ns_data = prom_query.get_prometheus_data(query=promql_p99_list_namespace, range_query=False,
                                                              options=option_time)
            p99_list_ns_latency = 1000 * float(p99_list_ns_data[0]['value'][1])
            print(f'P99 latency of LIST(namespace): {p99_list_ns_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying p99 LIST(namespace) latency data from prometheus: {e}')
        try:
            num_list_namespace_data = prom_query.get_prometheus_data(
                query=promql_num_list_namespace, range_query=False, options=option_time)
            num_list_namespace = float(num_list_namespace_data[0]['value'][1])
            print(f'# LIST (namespace) requests: {int(num_list_namespace)}')
        except Exception as e:
            print(f'An error occurred while querying # LIST (namespace) req data from prometheus: {e}')
        try:
            avg_list_cluster_data = prom_query.get_prometheus_data(query=promql_avg_list_cluster, range_query=False,
                                                                   options=option_time)
            avg_list_cluster_latency = 1000 * float(avg_list_cluster_data[0]['value'][1])
            print(f'Average latency of LIST(cluster): {avg_list_cluster_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying avg LIST(cluster) latency data from prometheus: {e}')
        try:
            p99_list_cluster_data = prom_query.get_prometheus_data(query=promql_p99_list_cluster, range_query=False,
                                                                   options=option_time)
            p99_list_cluster_latency = 1000 * float(p99_list_cluster_data[0]['value'][1])
            print(f'P99 latency of LIST(cluster): {p99_list_cluster_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying p99 LIST(cluster) latency data from prometheus: {e}')
        try:
            num_list_cluster_data = prom_query.get_prometheus_data(
                query=promql_num_list_cluster, range_query=False, options=option_time)
            num_list_cluster = float(num_list_cluster_data[0]['value'][1])
            print(f'# LIST (cluster) requests: {int(num_list_cluster)}')
        except Exception as e:
            print(f'An error occurred while querying # LIST (cluster) req data from prometheus: {e}')
        try:
            avg_mutating_data = prom_query.get_prometheus_data(query=promql_avg_mutating, range_query=False,
                                                               options=option_time)
            avg_mutating_latency = 1000 * float(avg_mutating_data[0]['value'][1])
            print(f'Average latency of mutating: {avg_mutating_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying avg mutating latency data from prometheus: {e}')
        try:
            p99_mutating_data = prom_query.get_prometheus_data(query=promql_p99_mutating, range_query=False,
                                                               options=option_time)
            p99_mutating_latency = 1000 * float(p99_mutating_data[0]['value'][1])
            print(f'P99 latency of mutating: {p99_mutating_latency}ms')
        except Exception as e:
            print(f'An error occurred while querying p99 mutating latency data from prometheus: {e}')
        try:
            num_mutating_data = prom_query.get_prometheus_data(
                query=promql_num_mutating, range_query=False, options=option_time)
            num_mutating = float(num_mutating_data[0]['value'][1])
            print(f'# mutating requests: {int(num_mutating)}')
        except Exception as e:
            print(f'An error occurred while querying # mutating req data from prometheus: {e}')
