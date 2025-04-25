import argparse
import os
import subprocess
import multiprocessing
from datetime import datetime
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.utils import utils


def clean_up(root_dir: str):  # Perform clean-up, root_dir: python_path
    # logging.info('Start clean program')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start clean program.')
    cmd_str = f'source /home/ljy/miniconda3/bin/activate k8stest && python {root_dir}/scripts/clean.py'
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End clean program')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End clean program.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'clean_up() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - clean_up() failed: {e}.')


def create_node(root_dir: str, node_num: int):  # root_dir: python_path
    # logging.info('Create KWOK node')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start creating KWOK nodes.')
    cmd_str = (f'go run {root_dir}/perf-tests/clusterloader2/cmd/clusterloader.go '
               f'--testconfig={root_dir}/perf-tests/clusterloader2/density/test_config_kwok_node.yaml '
               f'--provider=local '
               f'--provider-configs=ROOT_KUBECONFIG={root_dir}/config/test-apiserver-config.yaml '
               f'--kubeconfig={root_dir}/config/test-apiserver-config.yaml --v=2 '
               f'--enable-exec-service=false --enable-prometheus-server=false --nodes={node_num} 2>&1')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End creating KWOK node')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End creating KWOK nodes.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'create_node() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - create_node() failed: {e}.')


def run_cluster_loader(root_dir: str, output_dir: str,
                       node_num: int):  # Function to run clusterloader2, root_dir: python_path
    # logging.info('Start clusterloader2 test')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start clusterloader2 test.')
    cmd_str = (f'go run {root_dir}/perf-tests/clusterloader2/cmd/clusterloader.go '
               f'--testconfig={root_dir}/perf-tests/clusterloader2/density/test_config_kwok.yaml '
               f'--provider=local '
               f'--provider-configs=ROOT_KUBECONFIG={root_dir}/config/test-apiserver-config.yaml '
               f'--kubeconfig={root_dir}/config/test-apiserver-config.yaml --v=2 '
               f'--enable-exec-service=false --enable-prometheus-server=true '
               f'--tear-down-prometheus-server=false --nodes={node_num} --v=2 2>&1 '
               f'| tee {output_dir}/{node_num}node-cl2.txt')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End clusterloader2 test')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End clusterloader2 test.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'run_cluster_loader() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - run_cluster_loader() failed: {e}.')


def stress_test(stress_test_dir: str, output_dir: str, duration: str, node_num: int, rps: int,
                res_num: int):  # stress_test_dir: python_path/Stress-Test/
    # logging.info('Start stress test')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start stress test.')
    cmd_str = (f'go run {stress_test_dir}/main.go -rps={rps} -resnum={res_num} -duration={duration} 2>&1 '
               f'| tee {output_dir}/{node_num}node-stress-test.txt')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End stress test')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End stress test.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'stress_test() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - stress_test() failed: {e}.')


def mysql_test(output_dir: str, node_num: int):
    # logging.info('Start mysql test')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start mysql test.')
    cmd_str = (f'sysbench /usr/share/sysbench/tests/include/oltp_legacy/oltp.lua --db-driver=mysql '
               f'--mysql-host={utils.WORKER_NODE_IP} --mysql-port={utils.PORT_MYSQL} --mysql-user=root '
               f'--mysql-password=123456 --mysql-db=testdb --oltp-table-size=200000 --oltp-tables-count=4 --threads=4 '
               f'--events=10000 --time=300 --report-interval=3 run 2>&1 | '
               f'tee {output_dir}/{node_num}node-mysql-test.txt')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End mysql test')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End mysql test.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'mysql_test() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - mysql_test() failed: {e}.')


def redis_test(output_dir: str, node_num: int):
    # logging.info('Start redis test')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start redis test.')
    cmd_str = (f'redis-benchmark -t set,get -h {utils.WORKER_NODE_IP} -p {utils.PORT_REDIS} -c 100 -n 1000000 2>&1 '
               f'| tee {output_dir}/{node_num}node-redis-test.txt')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End redis test')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End redis test.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'redis_test() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - redis_test() failed: {e}.')


def nginx_test(output_dir: str, duration: str, node_num: int):
    # logging.info('Start nginx test')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start nginx test.')
    cmd_str = (f'wrk -t 12 -c 100 -d {duration} --latency http://{utils.WORKER_NODE_IP}:{utils.PORT_NGINX} 2>&1 '
               f'| tee {output_dir}/{node_num}node-nginx-test.txt')
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End nginx test')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End nginx test.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'nginx_test() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - nginx_test() failed: {e}.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parallel tasks runner for clusterloader2 and stress testing.')
    parser.add_argument('--run_cl', action='store_true', help='Run clusterloader2 to simulate high-load situation')
    parser.add_argument('--node', type=int, default=1000,
                        help='Number of nodes, default: 1000, stress test rps = 1/1000node, res_num = 60 + 20/1000node')
    parser.add_argument('--duration', type=str, default='10m', help='Duration of wrk test, default: 10m')
    parser.add_argument('--output', type=str, default=f'{python_path}/output/evaluation', help='Output file directory')
    parser.add_argument('--sp', type=str, default=f'{python_path}/stress-test', help='Stress test directory')
    parser.add_argument('--rt', type=str, default=python_path, help='Root directory')
    args = parser.parse_args()
    if args.run_cl:
        args.output += '-cl2'
    rps = int(1 * args.node / 1000)
    res_num = int(60 + 20 * args.node / 1000)
    if args.node > 0:
        # create node
        create_node(root_dir=args.rt, node_num=args.node)
    # run multiple tests simultaneously
    process_list = []
    cluster_loader_thread = multiprocessing.Process(
        target=run_cluster_loader, kwargs={'root_dir': args.rt, 'output_dir': args.output, 'node_num': args.node})
    if args.node > 0 and args.run_cl:
        process_list.append(cluster_loader_thread)
    stress_test_process = multiprocessing.Process(
        target=stress_test,
        kwargs={'stress_test_dir': args.sp,
                'output_dir': args.output,
                'duration': args.duration,
                'node_num': args.node,
                'rps': rps,
                'res_num': res_num})
    mysql_test_process = multiprocessing.Process(
        target=mysql_test,
        kwargs={'output_dir': args.output,
                'node_num': args.node})
    redis_test_process = multiprocessing.Process(
        target=redis_test,
        kwargs={'output_dir': args.output,
                'node_num': args.node})
    nginx_test_process = multiprocessing.Process(
        target=nginx_test,
        kwargs={'output_dir': args.output,
                'duration': "3m",
                'node_num': args.node})
    if args.node > 0:
        process_list.append(stress_test_process)
    process_list.append(mysql_test_process)
    process_list.append(redis_test_process)
    process_list.append(nginx_test_process)
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()
    # clean resource
    clean_up(args.rt)
    # logging.info("Finished")
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - stress-test.py finished.')
