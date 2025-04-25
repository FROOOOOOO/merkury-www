import argparse
import logging
import os
import subprocess
import multiprocessing
import sys
import time
from datetime import datetime
import signal

from main import master_comp_placement, main

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))  # repo dir
sys.path.insert(0, python_path)
from core.utils import utils


def run_stress_test(output_path: str, node_num: int, run_cl2: bool = False):
    # logging.info('Start stress test script')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start stress test script.')
    cmd_str = (f'source <conda path> <conda env> && python {python_path}/scripts/stress-test.py '
               f'--node {node_num} --output {output_path}')
    if run_cl2:
        cmd_str += ' --run_cl'
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End stress test script')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End stress test script.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'run_stress_test() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - run_stress_test() failed: {e}.')


def run_reset_cp(wider_fc: bool = False):
    # logging.info('Start reset control plane script')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Start reset control plane script.')
    cmd_str = (f'source <conda path> <conda env> && '
               f'python {python_path}/scripts/reset-control-plane.py')
    if wider_fc:
        cmd_str += ' --wider_fc'
    try:
        # logging.info(f'Command: {cmd_str}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Command: {cmd_str}.')
        subprocess.run(cmd_str, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        # logging.info('End reset control plane script')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - End reset control plane script.')
    except subprocess.CalledProcessError as e:
        # logging.error(f'run_reset_cp() failed: {e}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - ERROR - run_reset_cp() failed: {e}.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to run tests sequentially and automatically.')
    parser.add_argument('--node_num_start', type=int, default=1000, help='Node number started')
    parser.add_argument('--node_num_stop', type=int, default=10000, help='Node number stopped')
    parser.add_argument('--run_cl2', action='store_true', help='Run tests with cl2')
    parser.add_argument('--wo_cl2', action='store_true', help='Run tests without cl2')
    parser.add_argument('--wrapper', action='store_true', help='Run request wrapper')
    parser.add_argument('--output_path', type=str, default='1',
                        help=f'Relative path to output directory '
                             f'(\'{python_path}/output/evaluation/<wrapper-dir>/<baseline>\')')
    parser.add_argument('--baseline', type=str, required=True, help='Baseline name')
    parser.add_argument('--cpu_total', type=float, default=utils.NODE_ALLOCATABLE_CPU,
                        help='total allocatable CPU for each master node')
    parser.add_argument('--scheduler_placement', type=str, default=utils.MASTERS[0].name,
                        choices=[node.name for node in utils.MASTERS],
                        help='master scheduler placement')
    parser.add_argument('--controller_manager_placement', type=str, default=utils.MASTERS[0].name,
                        choices=[node.name for node in utils.MASTERS],
                        help='master controller-manager placement')
    parser.add_argument('--wider_tc', action='store_true',
                        help='Enable wider tc for apiservers (3000, default: 600).')
    args = parser.parse_args()
    wrapper_dir = 'no-wrapper'
    if args.wrapper:
        wrapper_dir = 'wrapper'
    stress_test_output_path = f'{python_path}/output/evaluation/{wrapper_dir}/{args.baseline}/{args.output_path}'
    node_num_list = [i for i in range(args.node_num_start, args.node_num_stop + 1, 1000)]
    logging.info(f'Auto-test script started. (node_num_list: {node_num_list}, run_cl2: {args.run_cl2}, '
                 f'baseline: {args.baseline}, scheduler_placement: {args.scheduler_placement}, '
                 f'controller_manager_placement: {args.controller_manager_placement})')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Auto-test script started. (node_num_list: '
          f'{node_num_list}, run_cl2: {args.run_cl2}, baseline: {args.baseline}, scheduler_placement: '
          f'{args.scheduler_placement}, controller_manager_placement: {args.controller_manager_placement})')
    running_main = args.baseline in ['weighted', 'p99', 'tsp', 'merkury', 'ga', 'disable_fc', 'mmck', 'reactive']
    # main params
    main_params = {'baseline': args.baseline,
                   'pred_model': 'latest',
                   'cpu_weight': 'cpu_load',
                   'cpu_total': args.cpu_total,
                   'mem_weight': 'req_num',
                   'interval': 5,
                   'dry_run': False,
                   'cpu_update_range': 0.2,
                   'mem_update_range': 0.2,
                   'load_threshold': 0.7 if args.baseline in ['weighted', 'merkury', 'ga', 'disable_fc', 'mmck'] else 0.0,
                   'disable_mem_alloc': True,
                   'calibration': args.baseline in ['merkury', 'ga', 'disable_fc', 'mmck'],
                   'wider_tc': args.wider_tc}
    current_log_handler = None
    if args.wo_cl2:
        # without cl2
        if running_main:
            node_num_list_1 = [0]
            node_num_list_1.extend(node_num_list)
        else:
            node_num_list_1 = node_num_list
        for node_num in node_num_list_1:
            master_comp_placement(comp=utils.KUBE_SCHEDULER, master_node=args.scheduler_placement)
            master_comp_placement(comp=utils.KUBE_CONTROLLER_MANAGER, master_node=args.controller_manager_placement)
            if not running_main:
                run_stress_test(output_path=stress_test_output_path, node_num=node_num, run_cl2=False)
                run_reset_cp(wider_fc=args.wider_fc)
            else:
                if args.wrapper:
                    log_file_name = f'{python_path}/logs/{args.baseline}-wrapper-{node_num}.log'
                else:
                    log_file_name = f'{python_path}/logs/{args.baseline}-{node_num}.log'
                if current_log_handler is not None:
                    current_log_handler.close()
                    logging.getLogger().removeHandler(current_log_handler)
                logging.basicConfig(filename=log_file_name, filemode='a',
                                    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                current_log_handler = logging.getLogger().handlers[0]
                stress_test_process = multiprocessing.Process(
                    target=run_stress_test, kwargs={'output_path': stress_test_output_path,
                                                    'node_num': node_num,
                                                    'run_cl2': False})
                main_process = multiprocessing.Process(target=main, kwargs=main_params)
                main_process.start()
                stress_test_process.start()
                stress_test_process.join()
                os.kill(main_process.pid, signal.SIGINT)
                main_process.join()
            time.sleep(15)
    if args.run_cl2:
        # with cl2
        if not args.wo_cl2 and running_main:
            node_num_list_2 = [0]
            node_num_list_2.extend(node_num_list)
        else:
            node_num_list_2 = node_num_list
        for node_num in node_num_list_2:
            master_comp_placement(comp=utils.KUBE_SCHEDULER, master_node=args.scheduler_placement)
            master_comp_placement(comp=utils.KUBE_CONTROLLER_MANAGER, master_node=args.controller_manager_placement)
            if not running_main:
                run_stress_test(output_path=stress_test_output_path, node_num=node_num, run_cl2=True)
                run_reset_cp(wider_fc=args.wider_fc)
            else:
                if args.wrapper:
                    log_file_name = f'{python_path}/logs/{args.baseline}-wrapper-{node_num}-cl2.log'
                else:
                    log_file_name = f'{python_path}/logs/{args.baseline}-{node_num}-cl2.log'
                if current_log_handler is not None:
                    current_log_handler.close()
                    logging.getLogger().removeHandler(current_log_handler)
                logging.basicConfig(filename=log_file_name, filemode='a',
                                    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                current_log_handler = logging.getLogger().handlers[0]
                stress_test_process = multiprocessing.Process(
                    target=run_stress_test, kwargs={'output_path': stress_test_output_path,
                                                    'node_num': node_num,
                                                    'run_cl2': True})
                main_process = multiprocessing.Process(target=main, kwargs=main_params)
                main_process.start()
                stress_test_process.start()
                stress_test_process.join()
                os.kill(main_process.pid, signal.SIGINT)
                main_process.join()
            if node_num > 0:
                # check clusterloader2 result
                cl2_log_path = f'{stress_test_output_path}-cl2/{node_num}node-cl2.txt'
                cl2_test_pass = True
                with open(cl2_log_path, 'r') as f:
                    lines = f.readlines()
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if line.endswith('Step "[step: 04] Waiting for saturation pods to be running" ended'):
                            next_line = lines[i + 1].strip()
                            if not next_line.endswith('Step "[step: 05] Collecting saturation pod measurements" started'):
                                # logging.warning(f'Clusterloader2 test for # node {node_num} failed: {next_line}')
                                print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - WARNING - Clusterloader2 test for '
                                    f'# node {node_num} failed: {next_line}')
                                cl2_test_pass = False
                                break
                if not cl2_test_pass:
                    break
            time.sleep(15)
    if current_log_handler is not None:
        current_log_handler.close()
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - INFO - Auto-test script finished.')
