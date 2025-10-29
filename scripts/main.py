"""
   Copyright 2025 FROOOOOOO and Ma-YuXin

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import logging
import argparse
import threading
import multiprocessing
import os
import sys
import time

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.recommender import recommender
from core.updater import tc_updater, res_updater
from core.utils import utils, prom_query


def run_recommender(rec: recommender.Recommender, shutdown_event):
    rec.start()
    while not shutdown_event.is_set():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    rec.shutdown()


def main(baseline, cpu_total, pred_model, cpu_weight, mem_weight, interval, dry_run, cpu_update_range, mem_update_range,
         disable_mem_alloc, calibration, load_threshold, wider_tc):
    stop_event = multiprocessing.Event()
    co_located_components = [utils.KUBE_APISERVER, utils.KUBE_CONTROLLER_MANAGER, utils.KUBE_SCHEDULER]
    updater_tc = tc_updater.TcUpdater(enable_logging=False)
    updater_res = res_updater.ResourceUpdater(collocated_components=co_located_components, enable_logging=False)
    if not dry_run:
        # reset resource and tc
        updater_tc.reset_tc(wider_apiserver_tcp=wider_tc)
        updater_res.reset_resource_limit_of_all_components()
    # recommender
    test_recommender_0 = recommender.Recommender(
        master_name=utils.MASTERS[0].name, master_addr=utils.MASTERS[0].ip, cpu_total=cpu_total,
        co_located_components=co_located_components, is_master=True, baseline=baseline, pred_model=pred_model,
        dry_run=dry_run, cpu_weight=cpu_weight, mem_weight=mem_weight, cpu_update_range=cpu_update_range,
        mem_update_range=mem_update_range, disable_mem_alloc=disable_mem_alloc, alloc_calibration=calibration,
        load_threshold=load_threshold)
    test_recommender_1 = recommender.Recommender(
        master_name=utils.MASTERS[1].name, master_addr=utils.MASTERS[1].ip, cpu_total=cpu_total,
        co_located_components=co_located_components, is_master=False, baseline=baseline, pred_model=pred_model,
        dry_run=dry_run, cpu_weight=cpu_weight, mem_weight=mem_weight, cpu_update_range=cpu_update_range,
        mem_update_range=mem_update_range, disable_mem_alloc=disable_mem_alloc, alloc_calibration=calibration,
        load_threshold=load_threshold)
    test_recommender_2 = recommender.Recommender(
        master_name=utils.MASTERS[2].name, master_addr=utils.MASTERS[2].ip, cpu_total=cpu_total,
        co_located_components=co_located_components, is_master=False, baseline=baseline, pred_model=pred_model,
        dry_run=dry_run, cpu_weight=cpu_weight, mem_weight=mem_weight, cpu_update_range=cpu_update_range,
        mem_update_range=mem_update_range, disable_mem_alloc=disable_mem_alloc, alloc_calibration=calibration,
        load_threshold=load_threshold)
    print(f'Experiment started: (baseline: {baseline}, pred_model: {pred_model}, load_threshold: {load_threshold}, '
          f'cpu_weight: {cpu_weight}, mem_weight: {mem_weight}, disable_mem_alloc: {disable_mem_alloc}, '
          f'cpu_update_range: {cpu_update_range}, mem_update_range: {mem_update_range}, calibration: {calibration})')
    recommender_processes = []
    process = multiprocessing.Process(target=run_recommender, args=(test_recommender_0, stop_event))
    recommender_processes.append(process)
    process.start()
    process = multiprocessing.Process(target=run_recommender, args=(test_recommender_1, stop_event))
    recommender_processes.append(process)
    process.start()
    process = multiprocessing.Process(target=run_recommender, args=(test_recommender_2, stop_event))
    recommender_processes.append(process)
    process.start()
    cost_monitor_processes = [multiprocessing.Process(
        target=utils.monitor_cost, args=(process.pid, interval, stop_event))
        for process in recommender_processes]
    for cost_monitor_process in cost_monitor_processes:
        cost_monitor_process.start()
    pod_monitor_thread = threading.Thread(
        target=utils.monitor_pods, args=(stop_event, 'kube-system', 'control=test-master'))
    pod_monitor_thread.daemon = True
    pod_monitor_thread.start()
    try:
        print('Waiting for Ctrl+C or SIGINT to stop...')
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for recommender_proc in recommender_processes:
            recommender_proc.join()
        for cost_monitor_process in cost_monitor_processes:
            cost_monitor_process.join()
        pod_monitor_thread.join()
        if not dry_run:
            # reset resource and tc after experiment completion
            updater_tc.reset_tc(wider_apiserver_tcp=wider_tc)
            updater_res.reset_resource_limit_of_all_components()
            updater_res.reset_resource_limit_of_all_components(restart=True)


def master_comp_placement(comp: str, master_node: str):
    if comp not in [utils.KUBE_SCHEDULER, utils.KUBE_CONTROLLER_MANAGER]:
        print(f'master_comp_placement(): invalid comp: {comp}')
        return
    rsu = res_updater.ResourceUpdater(enable_logging=False)
    while True:
        timestamp = int(time.time())
        master_node_index = int(master_node.split('-')[-1])  # master node name must be like: xxx-0|1|2
        instance_comp = utils.MASTERS[master_node_index].ip + ':' + utils.PORTS[comp]
        promql_master_status_comp = 'sum(leader_election_master_status{instance=\'%s\'})' % instance_comp
        option_time = f'&time={timestamp}'
        master_data = None
        try:
            master_data = prom_query.get_prometheus_data(
                query=promql_master_status_comp, range_query=False, options=option_time)
        except Exception as e:
            print(f'master_comp_placement(): an error occurred while querying master status data from prometheus: {e}.')
        if master_data is not None:
            is_master = int(master_data[0]['value'][1]) == 1
            if is_master:
                print(f'master_comp_placement(): master instance of {comp} has been successfully placed '
                      f'on {master_node}.')
                break
            else:
                print(f'master_comp_placement(): master instance of {comp} has not been successfully placed '
                      f'on {master_node}, will retry.')
        non_master_node_index = [i for i in range(len(utils.MASTERS)) if i != master_node_index]
        for index in non_master_node_index:
            rsu.delete_pod(comp, f'{comp}-{index}-0')  # component name must be like: kube-xxx-0|1|2-0
        # wait and check
        time.sleep(30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Main function to run metric fetchers, recommenders, and updaters.')
    parser.add_argument('--log_level', '-l', type=int, choices=[0, 1, 2, 3, 4], default=3,
                        help='Logging level:\n\t0--CRITICAL\n\t1--ERROR\n\t2--WARNING\n\t3--INFO(default)\n\t4--DEBUG')
    parser.add_argument('--file_name', '-f', type=str, default='merkury',
                        help='log file name (default: merkury), logs are stored in '
                             '/mnt/data/nfs/ljy/code/MerKury/logs/<FILE_NAME>.log')
    parser.add_argument('--cpu_total', type=float, default=utils.NODE_ALLOCATABLE_CPU,
                        help='total allocatable CPU for each master node')
    parser.add_argument('--baseline', '-b', type=str, default='merkury',
                        choices=['merkury', 'p99', 'tsp', 'weighted', 'ga', 'disable_tc', 'mmck', 'reactive'],
                        help=('Resource allocation baseline: \'merkury\'--MerKury(default), \'p99\'--99 percentile, '
                              '\'tsp\'--time series prediction, \'weighted\'--allocate according to weight, '
                              '\'ga\'--brute force ga optimizer, \'disable_tc\'--MerKury without tc optimization, '
                              '\'mmck\'--MerKury with M/M/c/K queue model, \'reactive\'--reactive strategy'))
    parser.add_argument('--pred_model', '-p', type=str, default='latest',
                        choices=['latest', 'average', 'power_decay', 'exponential_decay', 'ARIMA', 'VARMA'],
                        help=('time series prediction model to apply (applicable for baselines except \'p99\'): '
                              '\'latest\'--take the latest as prediction (default), '
                              '\'average\'--take average value over the past periods as prediction, '
                              '\'power_decay\''
                              '--take power-decay-weighted average value over the past periods as prediction, '
                              '\'exponential_decay\''
                              '--takeexponential-decay-weighted average value over the past periods as prediction, '
                              '\'ARIMA\'--apply ARIMA model, '
                              '\'VARMA\'--apply VARMA model (only applicable for multidimensional values）'))
    parser.add_argument('--cpu_weight', '-c', type=str, default='req_num',
                        choices=['req_num', 'cpu_load', 'mem_load', 'mem_load_execution', 'mem_load_queuing'],
                        help=('cpu allocation weight (applicable for \'weighted\' baseline): '
                              '\'req_num\'--request number, \'cpu_load\'--cpu load, '
                              '\'mem_load\'--total memory load, '
                              '\'mem_load execution\'--memory load during execution, '
                              '\'mem_load queuing\'--memory load during queuing'))
    parser.add_argument('--mem_weight', '-m', type=str, default='req_num',
                        choices=['req_num', 'cpu_load', 'mem_load', 'mem_load_execution', 'mem_load_queuing'],
                        help=('memory allocation weight (applicable for \'weighted\' baseline): '
                              '\'req_num\'--request number, \'cpu_load\'--cpu load, '
                              '\'mem_load\'--total memory load, '
                              '\'mem_load_execution\'--memory load during execution, '
                              '\'mem_load_queuing\'--memory load during queuing'))
    parser.add_argument('--interval', '-i', type=int, default=5,
                        help='sampling interval of cost monitoring, unit: s, default: 5')
    parser.add_argument('--dry_run', action='store_true', help='dry run (only recommend, no update)')
    parser.add_argument('--cpu_update_range', type=float, default=0.1,
                        help='cpu update range (from 0 to 1), default: 0.1')
    parser.add_argument('--mem_update_range', type=float, default=0.1,
                        help='memory update range (from 0 to 1), default: 0.1')
    parser.add_argument('--load_threshold', type=float, default=0.0, help='load threshold (>=0), default: 0.0')
    parser.add_argument('--disable_mem_alloc', action='store_true', help='disable memory allocation during execution')
    parser.add_argument('--calibration', action='store_true', help='enable allocation calibration')
    parser.add_argument('--wider_tc', action='store_true',
                        help='Enable wider tc for apiservers (3000, default: 600).')
    parser.add_argument('--enable_placement', action='store_true', help='Enable placement')
    parser.add_argument('--scheduler_placement', type=str, default=utils.MASTERS[0].name,
                        choices=[node.name for node in utils.MASTERS],
                        help='master scheduler placement')
    parser.add_argument('--controller_manager_placement', type=str, default=utils.MASTERS[0].name,
                        choices=[node.name for node in utils.MASTERS],
                        help='master controller-manager placement')
    args = parser.parse_args()
    log_level = logging.INFO
    if args.log_level == 0:
        log_level = logging.CRITICAL
    elif args.log_level == 1:
        log_level = logging.ERROR
    elif args.log_level == 2:
        log_level = logging.WARNING
    elif args.log_level == 3:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logging.basicConfig(filename=f'{python_path}/logs/{args.file_name}.log', filemode='a',
                        level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')  # filename can change
    if not args.dry_run and args.enable_placement:
        master_comp_placement(comp=utils.KUBE_SCHEDULER, master_node=args.scheduler_placement)
        master_comp_placement(comp=utils.KUBE_CONTROLLER_MANAGER, master_node=args.controller_manager_placement)
    # time.sleep(30)  # waiting for placement settled
    main(baseline=args.baseline, pred_model=args.pred_model, cpu_weight=args.cpu_weight, mem_weight=args.mem_weight,
         interval=args.interval, dry_run=args.dry_run, cpu_update_range=args.cpu_update_range,
         load_threshold=args.load_threshold, mem_update_range=args.mem_update_range, cpu_total=args.cpu_total,
         disable_mem_alloc=args.disable_mem_alloc, calibration=args.calibration, wider_tc=args.wider_tc)
