import time
import logging
import math
import decimal

import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from core.utils import utils
import core.utils.queue_model as uqm
from core.load_fetcher import p99_fetcher, merkury_fetcher
from core.recommender import ga_optimizer, predictor, simple_ga
from core.queue_model import queue_model
from core.updater import res_updater, tc_updater


class Recommender:
    """
    recommender for one master node, including fetchers for co-located components
    """

    def __init__(
            self, master_name: str = utils.MASTERS[0].name, master_addr: str = utils.MASTERS[0].ip,
            kubelet_port: str = utils.PORTS[utils.KUBELET],
            apiserver_port: str = utils.PORTS[utils.KUBE_APISERVER],
            scheduler_port: str = utils.PORTS[utils.KUBE_SCHEDULER],
            cm_port: str = utils.PORTS[utils.KUBE_CONTROLLER_MANAGER],
            etcd_port: str = utils.PORTS[utils.ETCD], co_located_components: list | None = None,
            cpu_total: float = utils.NODE_ALLOCATABLE_CPU, cpu_request: dict = utils.DEFAULT_CPU_REQ,
            mem_total: int = utils.NODE_ALLOCATABLE_MEMORY, mem_request: dict = utils.DEFAULT_MEM_REQ,
            cpu_update_range: float = utils.CPU_UPDATE_RANGE, mem_update_range: float = utils.MEMORY_UPDATE_RANGE,
            alloc_calibration: bool = False, load_threshold: float = 0.0, is_master: bool = False,
            load_query_interval: int = 60, recommend_interval: int = 60, buffer_size: int = 60,
            disable_mem_alloc: bool = False, enable_logging: bool = True, dry_run: bool = False,
            baseline: str = 'merkury', pred_model: str = 'latest', pred_calibration: bool = False,
            cpu_weight: str = 'req_num', mem_weight: str = 'req_num'):
        self.master_name = master_name
        self.master_addr = master_addr
        self.co_located_components = []
        if co_located_components is None:
            self.co_located_components = utils.CONTROL_PLANE_COMPONENTS
        else:
            for comp in co_located_components:
                if comp in utils.CONTROL_PLANE_COMPONENTS and comp not in self.co_located_components:
                    self.co_located_components.append(comp)
        self.co_located_components.sort()
        self.ports = {utils.KUBELET: kubelet_port}
        for comp in utils.CONTROL_PLANE_COMPONENTS:
            if comp == utils.ETCD:
                self.ports[comp] = etcd_port
            if comp == utils.KUBE_APISERVER:
                self.ports[comp] = apiserver_port
            if comp == utils.KUBE_CONTROLLER_MANAGER:
                self.ports[comp] = cm_port
            if comp == utils.KUBE_SCHEDULER:
                self.ports[comp] = scheduler_port
        self.enable_logging = enable_logging
        self.dry_run = dry_run
        self.disable_mem_alloc = disable_mem_alloc
        self.recommend_interval = max(1, recommend_interval)  # unit: second
        self.load_query_interval = max(1, load_query_interval)
        if sum(cpu_request.values()) > cpu_total:
            raise ValueError(f'insufficient cpu_total, require at least {sum(cpu_request.values())} core, '
                             f'get only {cpu_total} core.')
        self.cpu_total = cpu_total  # total cpu allocatable (unit: core)
        self.cpu_request = cpu_request  # CPU request of each components (unit: core)
        if sum(mem_request.values()) > mem_total:
            raise ValueError(f'insufficient cpu_total, require at least {sum(mem_request.values())} MB, '
                             f'get only {mem_total} MB.')
        self.mem_total = mem_total  # total memory of the node (unit: 10^6B)
        self.mem_request = mem_request
        self.buffer_size = buffer_size
        if baseline in ['p99', 'tsp', 'weighted', 'evo-alg', 'merkury']:
            self.baseline = baseline
        else:
            self.baseline = 'ours'
        if pred_model in ['latest', 'average', 'power_decay', 'exponential_decay', 'ARIMA', 'VARMA']:
            self.pred_model = pred_model
        else:
            self.pred_model = 'latest'
        if cpu_weight in ['req_num', 'cpu_load', 'mem_load', 'mem_load_execution', 'mem_load_queuing']:
            self.cpu_weight = cpu_weight  # for 'weighted' baseline
        else:
            self.cpu_weight = 'req_num'
        if mem_weight in ['req_num', 'cpu_load', 'mem_load', 'mem_load_execution', 'mem_load_queuing']:
            self.mem_weight = mem_weight  # for 'weighted' baseline
        else:
            self.mem_weight = 'req_num'
        self.is_master = is_master  # only master recommender can edit fc param
        self._recommendation_buffer = []  # record latest n RecommendationForNodes (n=buffer_size)
        self._metrics_buffer = []  # record latest n KubeRequestsSummary (n=buffer_size)
        self.alloc_calibration = alloc_calibration  # whether enable allocation calibration
        self.pred_calibration = pred_calibration  # whether enable load prediction calibration
        self.load_threshold = load_threshold
        # do recommendation only when predicted CPU load >= cpu_total * load_threshold
        self.cpu_update_range = cpu_update_range
        self.mem_update_range = mem_update_range
        self.last_state_recommended = False  # only when last_state_recommended == True will update_range be considered
        # load fetchers
        self.fetchers = {}
        for comp in self.co_located_components:
            if self.baseline == 'p99':
                self.fetchers[comp] = simple_fetcher.SimpleFetcher(
                    container=comp, node_name=self.master_name, node_addr=self.master_addr,
                    kubelet_port=self.ports[utils.KUBELET], enable_logging=self.enable_logging)
            else:
                self.fetchers[comp] = load_fetcher.LoadFetcher(
                    container=comp, node_name=self.master_name, node_addr=self.master_addr,
                    kubelet_port=self.ports[utils.KUBELET], container_port=self.ports[comp],
                    concurrency=utils.CONCURRENCY_DICT[comp], cpu_utl=utils.CPU_UTL_DICT[comp],
                    enable_logging=self.enable_logging)
        # allocation weights, allocation_weights[code][comp] = w, if there are n co-located components, according to
        # whether each component is busy, code is an n-bit binary
        self.allocation_weights = utils.nested_dict()
        if self.baseline == 'merkury':
            code_length = len(self.co_located_components)
            for i in range(int(2 ** code_length)):
                binary_code = bin(i)[2:].zfill(code_length)
                for comp in self.co_located_components:
                    self.allocation_weights[binary_code][comp] = 1.0
        self.rs_updater = res_updater.ResourceUpdater(
            cpu_total=self.cpu_total, mem_total=self.mem_total, cpu_update_range=self.cpu_update_range,
            mem_update_range=self.mem_update_range, enable_logging=self.enable_logging)
        self.tc_updater = tc_updater.TcUpdater(enable_logging=self.enable_logging)
        self._scheduler = BackgroundScheduler()
        self.recommend_job = self._scheduler.add_job(
            self._recommend, trigger='interval', seconds=self.recommend_interval)
        # self.load_query_job = self._scheduler.add_job(
        #     self._load_query, trigger='interval', seconds=self.load_query_interval)

    def __str__(self) -> str:
        return f'Recommender (node {self.master_name} ({self.master_addr}))'

    # get methods
    def get_latest_recommendation(self) -> utils.RecommendationForNode | None:
        if len(self._recommendation_buffer) > 0:
            return self._recommendation_buffer[-1]

    def get_latest_metrics(self):
        if len(self._metrics_buffer) > 0:
            return self._metrics_buffer[-1]

    # prediction
    def predict_upcoming_metrics(self) -> None | dict | utils.KubeRequestsSummary:
        if len(self._metrics_buffer) == 0:
            return
        if self.baseline in ['weighted', 'ours']:
            data = {}
            for comp in utils.CONTROL_PLANE_COMPONENTS:
                data[f'{comp}_req_num'] = [krs.get_req_num(comp) for krs in self._metrics_buffer]
                data[f'{comp}_cpu_load'] = [krs.get_cpu_load(comp) for krs in self._metrics_buffer]
                data[f'{comp}_m_e'] = [krs.get_memory_load_e(comp) for krs in self._metrics_buffer]
                data[f'{comp}_m_q'] = [krs.get_memory_load_q(comp) for krs in self._metrics_buffer]
            timestamps = [krs.timestamp for krs in self._metrics_buffer]
            tws = [krs.time_window for krs in self._metrics_buffer]
            # predict
            pred, tw = predictor.Predictor(
                data=data, start_timestamps=timestamps, time_windows=tws, model=self.pred_model).predict()
            if pred is None:
                return
            # scheduler load calibration to avoid classifying scheduler whose req_num == 0 but req_in_queue > 0
            # into idle components mistakenly
            if utils.KUBE_SCHEDULER in self.co_located_components:
                comp = utils.KUBE_SCHEDULER
                req_in_queue = self.fetchers[comp].get_req_in_queue()
                req_num = self._metrics_buffer[-1].get_req_num(comp)
                if req_in_queue > 0 and req_num == 0:
                    pred[f'{comp}_req_num'] = 1
            if self.pred_calibration:
                # calibrate prediction due to cpu throttle
                for comp in self.co_located_components:
                    cpu_throttle = self.fetchers[comp].get_cpu_throttle()
                    if comp in [utils.KUBE_APISERVER, utils.ETCD]:
                        calibrate_factor = 1 + cpu_throttle
                    else:
                        req_in_queue = self.fetchers[comp].get_req_in_queue()
                        req_num = self._metrics_buffer[-1].get_req_num(comp)
                        if req_in_queue > 0 and req_num == 0:
                            req_num = 1
                        calibrate_factor = 1 + req_in_queue * cpu_throttle / (req_num if req_num > 0 else 1)
                    pred[f'{comp}_req_num'] = int(pred[f'{comp}_req_num'] * calibrate_factor)
                    pred[f'{comp}_cpu_load'] *= calibrate_factor
                    pred[f'{comp}_m_e'] *= calibrate_factor
                    pred[f'{comp}_m_q'] *= calibrate_factor
            # construct krs
            crs_etcd = utils.CompRequestsSummary(
                req_num=int(pred['etcd_req_num']), req_load=utils.KubeLoad(
                    cpu_load=pred['etcd_cpu_load'], mem_load_execution=pred['etcd_m_e'],
                    mem_load_queuing=pred['etcd_m_q']))
            crs_apiserver = utils.CompRequestsSummary(
                req_num=int(pred['kube-apiserver_req_num']), req_load=utils.KubeLoad(
                    cpu_load=pred['kube-apiserver_cpu_load'], mem_load_execution=pred['kube-apiserver_m_e'],
                    mem_load_queuing=pred['kube-apiserver_m_q']))
            crs_scheduler = utils.CompRequestsSummary(
                req_num=int(pred['kube-scheduler_req_num']), req_load=utils.KubeLoad(
                    cpu_load=pred['kube-scheduler_cpu_load'], mem_load_execution=pred['kube-scheduler_m_e'],
                    mem_load_queuing=pred['kube-scheduler_m_q']))
            crs_cm = utils.CompRequestsSummary(
                req_num=int(pred['kube-controller-manager_req_num']), req_load=utils.KubeLoad(
                    cpu_load=pred['kube-controller-manager_cpu_load'],
                    mem_load_execution=pred['kube-controller-manager_m_e'],
                    mem_load_queuing=pred['kube-controller-manager_m_q']))
            krs = utils.KubeRequestsSummary(time_window=tw, apiserver_load=crs_apiserver, etcd_load=crs_etcd,
                                            cm_load=crs_cm, scheduler_load=crs_scheduler)
            utils.logging_or_print(f'{str(self)}: prediction of upcoming metrics - {str(krs)}',
                                   enable_logging=self.enable_logging, level=logging.INFO)
            return krs

    def _recommend_p99(self):
        start = time.time()
        cpu_alloc, mem_alloc = {}, {}
        for comp in self.co_located_components:
            cpu_alloc[comp] = int(self.fetchers[comp].get_p99_cpu() * utils.AMPLIFIER)
            if self.disable_mem_alloc:
                mem_alloc[comp] = self.mem_total
            else:
                mem_alloc[comp] = int(self.fetchers[comp].get_p99_memory() * utils.AMPLIFIER)
        self._generate_final_recommendation(start, cpu_alloc, mem_alloc)

    def _recommend_tsp(self):
        start = time.time()
        prediction = self.predict_upcoming_metrics()
        if prediction is None:
            utils.logging_or_print(
                f'{str(self)}: no prediction for cpu and memory utilization, recommendation canceled.',
                enable_logging=self.enable_logging, level=logging.WARNING)
            return
        cpu_alloc, mem_alloc = {}, {}
        for comp in self.co_located_components:
            cpu_alloc[comp] = int(prediction[f'{comp}_cpu'] * utils.AMPLIFIER)
            if self.disable_mem_alloc:
                mem_alloc[comp] = self.mem_total
            else:
                mem_alloc[comp] = int(prediction[f'{comp}_memory'] * utils.AMPLIFIER)
        self._generate_final_recommendation(start, cpu_alloc, mem_alloc)

    def _recommend_weighted(self):
        start = time.time()
        prediction = self.predict_upcoming_metrics()
        if prediction is None:
            utils.logging_or_print(
                f'{str(self)}: no prediction, recommendation canceled.',
                enable_logging=self.enable_logging, level=logging.WARNING)
            return
        if not self._load_check(prediction, start):
            return
        weights_cpu = {}
        weights_mem = {}

        def get_weight(weight: str, cmp: str):
            if weight == 'req_num':
                return prediction.get_req_num(cmp)
            elif weight == 'cpu_load':
                return prediction.get_cpu_load(cmp)
            elif weight == 'mem_load_execution':
                return prediction.get_memory_load_e(cmp)
            elif weight == 'mem_load_queuing':
                return prediction.get_memory_load_q(cmp)
            else:  # mem load = m_e + m_q
                return prediction.get_memory_load_e(cmp) + prediction.get_memory_load_q(cmp)

        for comp in self.co_located_components:
            weights_cpu[comp] = get_weight(self.cpu_weight, comp)
            weights_mem[comp] = get_weight(self.mem_weight, comp)
        cpu_alloc = self._alloc_resource_according_to_weights(
            weights_cpu, lower_bounds=self.cpu_request, allocatable=self.cpu_total, re_alloc=False)
        if self.disable_mem_alloc:
            mem_alloc = {comp: self.mem_total for comp in self.co_located_components}
        else:
            mem_alloc = self._alloc_resource_according_to_weights(
                weights_mem, lower_bounds=self.mem_request, allocatable=self.mem_total, re_alloc=False)
        # unit transformation
        for comp in self.co_located_components:
            cpu_alloc[comp] = int(cpu_alloc[comp] * 1000)
            mem_alloc[comp] = int(mem_alloc[comp])
        self._generate_final_recommendation(start, cpu_alloc, mem_alloc)

    def _recommend_evo_alg(self, strategy_gus: str = 'balance', strategy_lus: str = 'optimistic'):
        if strategy_gus not in ['greedy', 'balance']:
            strategy_gus = 'balance'  # global unsteady state optimization strategy: total CPU <= total CPU load
        if strategy_lus not in ['optimistic', 'pessimistic']:
            # local unsteady state optimization strategy: max f(x) <= CPU load / time window
            strategy_lus = 'optimistic'
        start = time.time()
        concurrency_rec, ql_rec = {}, {}
        cpu_alloc, mem_alloc = {}, {}
        for comp in self.co_located_components:
            concurrency_rec[comp], ql_rec[comp], cpu_alloc[comp], mem_alloc[comp] = 0, 0, 0, 0
            if self.disable_mem_alloc:
                mem_alloc[comp] = self.mem_total
        # 1. predict req_num and req_load
        upcoming_kube_requests = self.predict_upcoming_metrics()
        if upcoming_kube_requests is None:
            utils.logging_or_print(f'{str(self)}: no request load available, recommendation canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        if not self._load_check(prediction=upcoming_kube_requests, ts=start):
            return
        cpu_left = self.cpu_total
        mem_left = self.mem_total
        idle_components, busy_components = self._get_idle_and_busy_components(upcoming_kube_requests)
        # 2. allocate resources for idle components (controller-manager, scheduler)
        latest_recommendation = self.get_latest_recommendation()
        for comp in idle_components:
            # f*, q* unchanged (if last recommendation unavailable, f* = min(argmax_x(f(x))), q* = 1)
            if latest_recommendation is not None and comp in latest_recommendation.recommendations.keys():
                concurrency_rec[comp] = latest_recommendation.recommendations[comp].concurrency
                ql_rec[comp] = latest_recommendation.recommendations[comp].max_ql
            else:
                _, x, y = self.fetchers[comp].get_cpu_con_mapping()
                concurrency_rec[comp] = uqm.get_concurrency_rec(x, y)
                ql_rec[comp] = 1
            # c* = c_0, m* = m_0
            c_0 = self.fetchers[comp].get_cpu_baseline()
            if comp in self.cpu_request and self.cpu_request[comp] > c_0 * utils.AMPLIFIER:
                cpu_alloc[comp] = self.cpu_request[comp]
            else:
                cpu_alloc[comp] = c_0 * utils.AMPLIFIER
            cpu_left -= cpu_alloc[comp]
            if not self.disable_mem_alloc:
                m_0 = self.fetchers[comp].get_mem_baseline()
                if comp in self.mem_request and self.mem_request[comp] > m_0 * utils.AMPLIFIER:
                    mem_alloc[comp] = self.mem_request[comp]
                else:
                    mem_alloc[comp] = m_0 * utils.AMPLIFIER
                mem_left -= mem_alloc[comp]
        cpu_lb, cpu_ub = {}, {}
        for comp in busy_components:
            cpu_lb[comp] = upcoming_kube_requests.get_cpu_load(comp) / (1000 * upcoming_kube_requests.time_window)
            cpu_ub[comp] = self.fetchers[comp].get_y_max()
        # 3. allocate resources for other components
        # 3.1 check whether steady-state for every component can be met
        steady_state, steady_state_global, steady_state_local, unsteady_components = (
            self._steady_state_check(busy_components, cpu_left, cpu_lb, cpu_ub))
        # 3.2 f* = min(argmax_x(f(x)))
        qm = {}
        for comp in busy_components:
            _, x, y = self.fetchers[comp].get_cpu_con_mapping()
            concurrency_rec[comp] = uqm.get_concurrency_rec(x, y)
            qm[comp] = queue_model.QueueModelwithTruncation(
                concurrency=x, cpu_utl=y, total_load=upcoming_kube_requests.get_cpu_load(comp),
                delta_t=upcoming_kube_requests.time_window, req_num=upcoming_kube_requests.get_req_num(comp))
        req_num = {comp: upcoming_kube_requests.get_req_num(comp) for comp in busy_components}
        # 3.3 CPU allocation
        if steady_state:
            ga = simple_ga.SimpleGA(cpu_total=cpu_left, queue_models=qm, weights=self.allocation_weights,
                                    cpu_granularity=50)
            f_c_q_alloc, _, _ = ga.run()
            if f_c_q_alloc is None:
                utils.logging_or_print(
                    f'{str(self)}: GA optimizer failed to allocate CPU, '
                    f'degenerate to allocate CPU according to #req.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
                # degenerate to 'weighted num'
                cpu_alloc_temp = self._alloc_resource_according_to_weights(
                    weights=req_num, lower_bounds=cpu_lb, upper_bounds=cpu_ub, allocatable=cpu_left, re_alloc=True)
                for comp in busy_components:
                    cpu_alloc[comp] = cpu_alloc_temp[comp]
                    cpu_left -= cpu_alloc[comp]
                    ql_rec[comp] = qm[comp].get_ql_rec(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
            else:
                for comp in busy_components:
                    cpu_alloc[comp] = f_c_q_alloc[comp][1]
                    cpu_left -= cpu_alloc[comp]
                    ql_rec[comp] = f_c_q_alloc[comp][2]
        elif steady_state_global:  # only local unsteady
            if len(unsteady_components) == len(busy_components):  # all components are local unsteady
                cpu_alloc_temp = self._alloc_resource_according_to_weights(
                    weights=req_num, lower_bounds=self.cpu_request, allocatable=cpu_left, re_alloc=False)
                for comp in unsteady_components:
                    cpu_alloc[comp] = cpu_alloc_temp[comp]
                    cpu_left -= cpu_alloc[comp]
                    load_left = qm[comp].calc_load_left(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
                    ql_rec[comp] = math.ceil(upcoming_kube_requests.get_req_num(comp) * float(load_left) /
                                             upcoming_kube_requests.get_cpu_load(comp))
            else:  # only some component(s) is(are) local unsteady
                # allocate CPU and q for unsteady components
                for comp in unsteady_components:
                    if strategy_lus == 'pessimistic':
                        cpu_alloc[comp] = max(self.cpu_request[comp], cpu_ub[comp])
                    if strategy_lus == 'optimistic':
                        cpu_alloc[comp] = max(self.cpu_request[comp], cpu_lb[comp])
                    cpu_left -= cpu_alloc[comp]
                    load_left = qm[comp].calc_load_left(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
                    ql_rec[comp] = math.ceil(upcoming_kube_requests.get_req_num(comp) * float(load_left) /
                                             upcoming_kube_requests.get_cpu_load(comp))
                # use GA to allocate CPU for other components (if failed to solve, degenerate to weighted num)
                qm_ss = {comp: qm[comp] for comp in busy_components if comp not in unsteady_components}
                ga = simple_ga.SimpleGA(cpu_total=cpu_left, queue_models=qm_ss, weights=self.allocation_weights,
                                        cpu_request=self.cpu_request, cpu_granularity=50)
                f_c_q_alloc, _, _ = ga.run()
                if f_c_q_alloc is None:
                    utils.logging_or_print(
                        f'{str(self)}: GA optimizer failed to allocate CPU, '
                        f'degenerate to allocate CPU according to #req.',
                        enable_logging=self.enable_logging, level=logging.WARNING)
                    # degenerate to 'weighted num'
                    req_num_ss = {comp: req_num[comp] for comp in busy_components if comp in qm_ss}
                    cpu_alloc_temp = self._alloc_resource_according_to_weights(
                        weights=req_num_ss, lower_bounds=cpu_lb, upper_bounds=cpu_ub, allocatable=cpu_left,
                        re_alloc=True)
                    for comp in qm_ss:
                        cpu_alloc[comp] = cpu_alloc_temp[comp]
                        cpu_left -= cpu_alloc[comp]
                        ql_rec[comp] = qm[comp].get_ql_rec(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
                else:
                    for comp in qm_ss:
                        cpu_alloc[comp] = f_c_q_alloc[comp][1]
                        cpu_left -= cpu_alloc[comp]
                        ql_rec[comp] = f_c_q_alloc[comp][2]
        else:  # global unsteady
            if strategy_gus == 'greedy':
                sorted_components = sorted(busy_components, key=upcoming_kube_requests.get_cpu_load)
                for comp in busy_components:
                    cpu_left -= self.cpu_request[comp]
                for comp in sorted_components:
                    cpu_left += self.cpu_request[comp]
                    cpu_alloc[comp] = min(cpu_lb[comp], cpu_left)
                    if comp in unsteady_components and strategy_lus == 'pessimistic':
                        if cpu_alloc[comp] > cpu_ub[comp]:
                            cpu_alloc[comp] = max(cpu_ub[comp], self.cpu_request[comp])
                    cpu_left -= cpu_alloc[comp]
            if strategy_gus == 'balance':
                cpu_alloc_temp = self._alloc_resource_according_to_weights(
                    weights={comp: cpu_lb[comp] for comp in busy_components}, lower_bounds=self.cpu_request,
                    upper_bounds=cpu_ub, allocatable=cpu_left, re_alloc=strategy_lus == 'pessimistic')
                for comp in busy_components:
                    cpu_alloc[comp] = cpu_alloc_temp[comp]
                    cpu_left -= cpu_alloc[comp]
            # q*
            for comp in busy_components:
                if qm[comp].meet_steady_state_condition(cpu_alloc=cpu_alloc[comp], flow_ctrl=concurrency_rec[comp]):
                    ql_rec[comp] = qm[comp].get_ql_rec(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
                else:
                    load_left = qm[comp].calc_load_left(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp])
                    ql_rec[comp] = math.ceil(upcoming_kube_requests.get_req_num(comp) * float(load_left) /
                                             upcoming_kube_requests.get_cpu_load(comp))
        # final cpu allocation
        if cpu_left > 0:
            second_alloc = self._alloc_resource_according_to_weights(
                weights={comp: cpu_lb[comp] for comp in busy_components}, allocatable=cpu_left,
                lower_bounds={comp: 0 for comp in busy_components}, re_alloc=False)
            for comp in busy_components:
                cpu_alloc[comp] += second_alloc[comp]
        # 3.4 m* = m_min,i * M/m_min
        m0, mem_est = {}, {}
        mem_est_total = 0
        for comp in busy_components:
            m0[comp] = self.fetchers[comp].get_mem_baseline()
            if qm[comp].meet_steady_state_condition(cpu_alloc=cpu_alloc[comp], flow_ctrl=concurrency_rec[comp],
                                                    max_ql=ql_rec[comp]):
                sl = qm[comp].calc_l(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp], max_ql=ql_rec[comp])
                l_q = qm[comp].calc_l_q(flow_ctrl=concurrency_rec[comp], cpu_alloc=cpu_alloc[comp], max_ql=ql_rec[comp])
            else:
                sl = concurrency_rec[comp] + ql_rec[comp]
                l_q = ql_rec[comp]
            m_e = upcoming_kube_requests.get_memory_load_e(comp) / upcoming_kube_requests.get_req_num(comp)
            m_q = upcoming_kube_requests.get_memory_load_q(comp) / upcoming_kube_requests.get_req_num(comp)
            mem_est[comp] = m0[comp] + float(l_q) * m_q + float(sl - l_q) * m_e
            mem_est_total += mem_est[comp]
        if mem_est_total >= mem_left:
            if self.disable_mem_alloc:
                mem_total = mem_est_total
            else:
                mem_total = mem_est_total + sum(mem_alloc.values())
            utils.logging_or_print(
                f'{str(self)}: high risk of OOM (total memory: {self.mem_total}MB, '
                f'estimated memory: {mem_total}MB).', enable_logging=self.enable_logging, level=logging.WARNING)
        if not self.disable_mem_alloc:
            for comp in busy_components:
                if comp in self.mem_request:
                    mem_left -= self.mem_request[comp]
            for comp in busy_components:
                mem_alloc[comp] = mem_left * mem_est[comp] / mem_est_total
                if comp in self.mem_request:
                    mem_alloc[comp] += self.mem_request[comp]
        # unit transformation
        for comp in self.co_located_components:
            cpu_alloc[comp] = int(cpu_alloc[comp] * 1000)
            mem_alloc[comp] = int(mem_alloc[comp])
        # 4. generate final recommendation
        self._generate_final_recommendation(
            ts=start, cpu_alloc=cpu_alloc, mem_alloc=mem_alloc, concurrency_rec=concurrency_rec, ql_rec=ql_rec)

    def _recommend_merkury(self, brute_force: bool = False, disable_fc: bool = False, strategy_gus: str = 'balance',
                        strategy_lus: str = 'optimistic') -> (bool, utils.KubeRequestsSummary):
        """
        MerKury recommendation algorithm
        :param brute_force: whether using brute-force algorithm in optimizer
        :param disable_fc: whether disable flow control params recommendation
        :param strategy_gus: allocation strategy for global unsteady state
        :param strategy_lus: allocation strategy for local unsteady state
        :return: whether recommended and updated, upcoming kube request (for allocation calibration)
        """
        if strategy_gus not in ['balance', 'greedy']:
            strategy_gus = 'balance'
        if strategy_lus not in ['optimistic', 'pessimistic']:
            strategy_lus = 'optimistic'
        rec_start = time.time()
        f_rec, q_rec, c_rec, m_rec = {}, {}, {}, {}
        for comp in self.co_located_components:
            f_rec[comp], q_rec[comp], c_rec[comp], m_rec[comp] = 0, 0, 0, 0
            if self.disable_mem_alloc:
                m_rec[comp] = self.mem_total
        # 1. predict req num and load
        upcoming_kube_requests = self.predict_upcoming_metrics()  # no prediction calibration
        if upcoming_kube_requests is None:
            utils.logging_or_print(f'{str(self)}: no request load available, recommendation canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return False, upcoming_kube_requests
        if not self._load_check(prediction=upcoming_kube_requests, ts=rec_start):
            return False, upcoming_kube_requests
        # 2. idle components (controller-manager and scheduler) resource allocation
        cpu_left = self.cpu_total
        mem_left = self.mem_total
        idle_comp, busy_comp = self._get_idle_and_busy_components(upcoming_kube_requests)
        busy_comp_code = self._get_busy_comp_code(busy_comp)
        for comp in idle_comp:
            # f*, q* skipped because it's only available for apiserver
            # c* = max(cpu_request, c_0 * AMPLIFIER), m* = max(mem_request, m_0 * AMPLIFIER)
            c_0 = self.fetchers[comp].get_cpu_baseline()
            if comp in self.cpu_request and self.cpu_request[comp] > c_0 * utils.AMPLIFIER:
                c_rec[comp] = self.cpu_request[comp]
            else:
                c_rec[comp] = c_0 * utils.AMPLIFIER
            cpu_left -= c_rec[comp]
            if not self.disable_mem_alloc:
                m_0 = self.fetchers[comp].get_mem_baseline()
                if comp in self.mem_request and self.mem_request[comp] > m_0 * utils.AMPLIFIER:
                    m_rec[comp] = self.mem_request[comp]
                else:
                    m_rec[comp] = m_0 * utils.AMPLIFIER
                mem_left -= m_rec[comp]
        # 3. busy components resource allocation
        # get concurrency from Prometheus (if disable_fc)
        default_concurrency = 600
        max_concurrency = (self.fetchers[utils.KUBE_APISERVER].
                           query_max_concurrency(timestamp=int(rec_start)))
        if max_concurrency is not None:
            default_concurrency = max_concurrency
        comp_stats, cpu_lb, cpu_ub = {}, {}, {}
        req_num_cm, req_num_scheduler = 0, 0  # req num between apiserver <-> cm/scheduler
        calibrate_factor_cm = 1.0
        # 3.1 controller-manager load calibration
        if utils.KUBE_CONTROLLER_MANAGER in busy_comp:
            comp = utils.KUBE_CONTROLLER_MANAGER
            cpu_load = upcoming_kube_requests.get_cpu_load(comp)
            req_num = upcoming_kube_requests.get_req_num(comp)
            if req_num > 0:
                req_num_cm = self.fetchers[comp].get_req_num_inner()
                req_in_queue = self.fetchers[comp].get_req_in_queue()
                req_in_queue_last = self.fetchers[comp].get_req_in_queue_last()
                calibrate_factor_cm = min(3.0, req_in_queue / (req_in_queue_last + 1))
            else:  # when cpu_throttle closed to 1, req_num, req_num_cm and req_in_queue are all empty
                cpu_throttle = self.fetchers[comp].get_cpu_throttle()
                calibrate_factor_cm = 1 + cpu_throttle
            cpu_load_calibrated = calibrate_factor_cm * cpu_load
            cpu_lb[comp] = cpu_load_calibrated / (upcoming_kube_requests.time_window * 1000)
            cpu_lb[comp] *= utils.AMPLIFIER
            if len(busy_comp) > 1:  # calibrate according to allocation weights
                other_busy_comp_weights = 0.0
                for i, cmp in enumerate(self.allocation_weights[busy_comp_code]):
                    if busy_comp_code[i] == '1' and cmp != comp:
                        other_busy_comp_weights += self.allocation_weights[busy_comp_code][cmp]
                other_busy_comp_weights /= len(busy_comp) - 1
                cpu_lb[comp] *= self.allocation_weights[busy_comp_code][comp] / other_busy_comp_weights
            cpu_ub[comp] = self.fetchers[comp].get_y_max()
        # 3.2 construct component stats, cpu_lb and cpu_ub for steady state check and optimization
        if utils.KUBE_SCHEDULER in busy_comp:
            comp = utils.KUBE_SCHEDULER
            req_num_scheduler = self.fetchers[comp].get_req_num_inner()
            comp_stats[comp] = utils.AsyncCompStats(
                cpu_load=upcoming_kube_requests.get_cpu_load(comp),
                cpu_throttle=self.fetchers[comp].get_cpu_throttle(),
                req_num=upcoming_kube_requests.get_req_num(comp),
                req_in_queue=self.fetchers[comp].get_req_in_queue(),
                delta_t=upcoming_kube_requests.time_window)
            cpu_lb[comp] = self.cpu_request[comp]
            cpu_ub[comp] = self.fetchers[comp].get_y_max()
        for comp in busy_comp:
            if comp in [utils.KUBE_APISERVER, utils.ETCD]:
                _, concurrency, cpu_utl = self.fetchers[comp].get_cpu_con_mapping()
                cpu_load_total = upcoming_kube_requests.get_cpu_load(comp)
                req_num_total = upcoming_kube_requests.get_req_num(comp)
                req_num_outside = max(0, req_num_total - req_num_cm - req_num_scheduler)
                req_num_calibrated = req_num_outside + req_num_scheduler + calibrate_factor_cm * req_num_cm
                cpu_load_calibrated = cpu_load_total * req_num_calibrated / req_num_total
                comp_stats[comp] = utils.SyncCompStats(
                    concurrency=concurrency, cpu_utl=cpu_utl, cpu_load=cpu_load_calibrated,
                    cpu_throttle=self.fetchers[comp].get_cpu_throttle(),
                    req_num=int(req_num_calibrated), req_scheduler=req_num_scheduler,
                    req_cm=int(calibrate_factor_cm * req_num_cm),
                    delta_t=upcoming_kube_requests.time_window)
                # remove scheduler cpu load to calculate cpu_lb for steady state check
                req_num_calibrated_wo_scheduler = req_num_calibrated - req_num_scheduler
                cpu_lb[comp] = (cpu_load_total * req_num_calibrated_wo_scheduler / req_num_total /
                                (upcoming_kube_requests.time_window * 1000))
                cpu_ub[comp] = self.fetchers[comp].get_y_max()
        # 3.3 steady state check
        steady_state, steady_state_global, steady_state_local, unsteady_comp = (
            self._steady_state_check(busy_comp, cpu_left, cpu_lb, cpu_ub))

        def get_f_and_q_for_apiserver(cpu_apiserver: float, cpu_scheduler: float) -> (int, int):
            f_recommendation, q_recommendation = 0, 0
            if not disable_fc:
                comp = utils.KUBE_APISERVER
                apiserver_stats = comp_stats[comp]
                concurrency = apiserver_stats.get_concurrency()
                cpu_utl = apiserver_stats.get_cpu_utl()
                if concurrency is None or cpu_utl is None or len(concurrency) != len(cpu_utl):
                    concurrency = utils.CONCURRENCY_DICT[comp]
                    cpu_utl = utils.CPU_UTL_DICT[comp]
                    f_recommendation = default_concurrency
                else:
                    y = np.array(cpu_utl)
                    y[y > cpu_apiserver] = cpu_apiserver
                    f_recommendation = uqm.get_concurrency_rec(concurrency, y)
                # load calibration according to scheduler CPU allocation
                if utils.KUBE_SCHEDULER in busy_comp:
                    scheduler_stats = comp_stats[utils.KUBE_SCHEDULER]
                    n_q = scheduler_stats.get_req_in_queue()
                    n_r = scheduler_stats.get_req_num()
                    delta_t = scheduler_stats.get_delta_t_ms()
                    c = scheduler_stats.get_cpu_load()
                    n_rbi = apiserver_stats.get_req_scheduler()
                    n_ri = apiserver_stats.get_req_num()
                    scale_factor = 1 + (min(n_q / n_r, cpu_scheduler * delta_t / c) - 1) * n_rbi / n_ri
                    qm_apiserver = queue_model.QueueModelwithTruncation(
                        concurrency=concurrency, cpu_utl=cpu_utl,
                        total_load=apiserver_stats.get_cpu_load() * scale_factor,
                        req_num=int(apiserver_stats.get_req_num() * scale_factor),
                        delta_t=apiserver_stats.get_delta_t()
                    )
                else:
                    qm_apiserver = queue_model.QueueModelwithTruncation(
                        concurrency=concurrency, cpu_utl=cpu_utl,
                        total_load=apiserver_stats.get_cpu_load(),
                        req_num=apiserver_stats.get_req_num(),
                        delta_t=apiserver_stats.get_delta_t()
                    )
                if cpu_apiserver > float(qm_apiserver.get_cpu_lower_bound()):
                    q_recommendation = qm_apiserver.get_ql_rec(flow_ctrl=f_recommendation,
                                                               cpu_alloc=cpu_apiserver)
                else:
                    load_left = qm_apiserver.calc_load_left(flow_ctrl=f_recommendation,
                                                            cpu_alloc=cpu_apiserver)
                    q_recommendation = math.ceil(upcoming_kube_requests.get_req_num(comp) * float(load_left) /
                                                 upcoming_kube_requests.get_cpu_load(comp))
            return f_recommendation, q_recommendation

        def allocate_degradation(components: list, cpu_allocatable: float, cpu_lower_bounds: dict,
                                 cpu_upper_bounds: dict | None = None, re_allocation: bool = False) -> dict:
            f_recommendation = {comp: 0 for comp in components}
            c_recommendation = {comp: 0.0 for comp in components}
            q_recommendation = {comp: 0 for comp in components}
            cpu_load = {comp: upcoming_kube_requests.get_cpu_load(comp)
                        for comp in busy_comp if comp != utils.KUBE_CONTROLLER_MANAGER}
            cpu_alloc_tmp = self._alloc_resource_according_to_weights(
                weights={comp: cpu_load[comp] for comp in components
                         if comp != utils.KUBE_CONTROLLER_MANAGER},
                lower_bounds=cpu_lower_bounds, upper_bounds=cpu_upper_bounds, allocatable=cpu_allocatable,
                re_alloc=re_allocation)
            # CPU allocation
            for comp in components:
                if comp == utils.KUBE_CONTROLLER_MANAGER:
                    c_recommendation[comp] = cpu_lower_bounds[comp]
                else:
                    c_recommendation[comp] = cpu_alloc_tmp[comp]
            if utils.KUBE_APISERVER in components:
                (f_recommendation[utils.KUBE_APISERVER],
                 q_recommendation[utils.KUBE_APISERVER]) = get_f_and_q_for_apiserver(
                    cpu_apiserver=c_recommendation[utils.KUBE_APISERVER],
                    cpu_scheduler=c_recommendation[utils.KUBE_SCHEDULER])
            return {comp: [f_recommendation[comp], c_recommendation[comp], q_recommendation[comp]]
                    for comp in components}

        def allocate_resource_for_steady_comp(steady_components: list, cpu_allocatable: float,
                                              bc_code: str) -> dict:
            f_recommendation = {comp: 0 for comp in steady_components}
            c_recommendation = {comp: 0.0 for comp in steady_components}
            q_recommendation = {comp: 0 for comp in steady_components}
            adaptive_granularity = max(1, int(utils.GA_TIME_LIMIT / utils.GA_TIME_SPACE_RATIO /
                                              (cpu_allocatable - sum([cpu_lb[comp] for comp in steady_components]))))
            cpu_allocatable_for_opt = cpu_allocatable
            if utils.KUBE_CONTROLLER_MANAGER in steady_components:
                cpu_allocatable_for_opt -= cpu_lb[utils.KUBE_CONTROLLER_MANAGER]
            opt = ga_optimizer.Optimizer(
                cpu_total=cpu_allocatable_for_opt, cpu_request=self.cpu_request, cpu_granularity=adaptive_granularity,
                component_stats={comp: comp_stats[comp] for comp in steady_components
                                 if comp not in [utils.KUBE_SCHEDULER,
                                                 utils.KUBE_CONTROLLER_MANAGER]},
                scheduler=comp_stats[utils.KUBE_SCHEDULER] if utils.KUBE_SCHEDULER in steady_components else utils.AsyncCompStats(empty=True),
                weights=self.allocation_weights[bc_code], brute_force=brute_force, disable_fc=disable_fc,
                default_concurrency=default_concurrency)
            f_c_q_alloc, _, _ = opt.run()
            if f_c_q_alloc is None:  # degenerate to 'weighted CPU load'
                utils.logging_or_print(
                    f'{str(self)}: Optimizer failed to allocate CPU, '
                    f'degenerate to allocate CPU according to CPU load weights.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
                return allocate_degradation(components=steady_components, cpu_allocatable=cpu_allocatable,
                                            cpu_lower_bounds=cpu_lb, cpu_upper_bounds=cpu_ub, re_allocation=True)
            else:  # optimizer successfully returns solution
                for comp in steady_components:
                    if comp == utils.KUBE_CONTROLLER_MANAGER:
                        c_recommendation[comp] = cpu_lb[comp]
                    else:
                        c_recommendation[comp] = f_c_q_alloc[comp][1]
                    cpu_allocatable -= c_recommendation[comp]
                    if comp == utils.KUBE_APISERVER:
                        f_recommendation[comp] = f_c_q_alloc[comp][0]
                        q_recommendation[comp] = f_c_q_alloc[comp][2]
                return {comp: [f_recommendation[comp], c_recommendation[comp], q_recommendation[comp]]
                        for comp in steady_components}

        if steady_state:  # steady state
            alloc = allocate_resource_for_steady_comp(steady_components=busy_comp, cpu_allocatable=cpu_left,
                                                      bc_code=busy_comp_code)
            for comp in busy_comp:
                f_rec[comp] = alloc[comp][0]
                c_rec[comp] = alloc[comp][1]
                q_rec[comp] = alloc[comp][2]
                cpu_left -= c_rec[comp]
        elif steady_state_global:  # only local unsteady
            steady_comp = [comp for comp in busy_comp if comp not in unsteady_comp]
            if len(steady_comp) == 0:  # all components are locally unsteady
                alloc = allocate_degradation(components=unsteady_comp, cpu_allocatable=cpu_left,
                                             cpu_lower_bounds=cpu_lb)
                for comp in busy_comp:
                    f_rec[comp] = alloc[comp][0]
                    c_rec[comp] = alloc[comp][1]
                    q_rec[comp] = alloc[comp][2]
                    cpu_left -= c_rec[comp]
            else:  # only some component(s) is(are) locally unsteady
                # allocate CPU for unsteady components
                for comp in unsteady_comp:
                    if strategy_lus == 'pessimistic':
                        c_rec[comp] = max(self.cpu_request[comp], cpu_ub[comp])
                    if strategy_lus == 'optimistic':
                        c_rec[comp] = max(self.cpu_request[comp], cpu_lb[comp])
                    cpu_left -= c_rec[comp]
                # use GA to allocate CPU for other components (if failed to solve, degrade to weighted CPU load)
                alloc = allocate_resource_for_steady_comp(steady_components=steady_comp, cpu_allocatable=cpu_left,
                                                          bc_code=busy_comp_code)
                for comp in steady_comp:
                    f_rec[comp] = alloc[comp][0]
                    c_rec[comp] = alloc[comp][1]
                    q_rec[comp] = alloc[comp][2]
                    cpu_left -= c_rec[comp]
                # apiserver f* and q* calculation
                if utils.KUBE_APISERVER in unsteady_comp:
                    (f_rec[utils.KUBE_APISERVER],
                     q_rec[utils.KUBE_APISERVER]) = get_f_and_q_for_apiserver(
                        cpu_apiserver=c_rec[utils.KUBE_APISERVER],
                        cpu_scheduler=c_rec[utils.KUBE_SCHEDULER])
        else:  # global unsteady
            if strategy_gus == 'greedy':
                sorted_comp = sorted(busy_comp, key=upcoming_kube_requests.get_cpu_load)
                for comp in busy_comp:
                    cpu_left -= self.cpu_request[comp]
                for comp in sorted_comp:
                    cpu_left += self.cpu_request[comp]
                    c_rec[comp] = min(cpu_lb[comp], cpu_left)
                    if comp in unsteady_comp and strategy_lus == 'pessimistic':
                        if c_rec[comp] > cpu_ub[comp]:
                            c_rec[comp] = max(cpu_ub[comp], self.cpu_request[comp])
                    cpu_left -= c_rec[comp]
            if strategy_gus == 'balance':
                cpu_alloc_temp = self._alloc_resource_according_to_weights(
                    weights={comp: cpu_lb[comp] for comp in busy_comp}, lower_bounds=self.cpu_request,
                    upper_bounds=cpu_ub, allocatable=cpu_left, re_alloc=strategy_lus == 'pessimistic')
                for comp in busy_comp:
                    c_rec[comp] = cpu_alloc_temp[comp]
                    cpu_left -= c_rec[comp]
            # apiserver f* and q* calculation
            f_rec[utils.KUBE_APISERVER], q_rec[utils.KUBE_APISERVER] = (
                get_f_and_q_for_apiserver(
                    cpu_apiserver=c_rec[utils.KUBE_APISERVER],
                    cpu_scheduler=c_rec[utils.KUBE_SCHEDULER]))
        # if cpu_left > 0, allocate CPU again for busy components
        if cpu_left > 0:
            second_alloc = self._alloc_resource_according_to_weights(
                weights={comp: cpu_lb[comp] for comp in busy_comp}, allocatable=cpu_left,
                lower_bounds={comp: 0 for comp in busy_comp}, re_alloc=False)
            for comp in busy_comp:
                c_rec[comp] += second_alloc[comp]
            # apiserver f* and q* calculation
            f_rec[utils.KUBE_APISERVER], q_rec[utils.KUBE_APISERVER] = (
                get_f_and_q_for_apiserver(
                    cpu_apiserver=c_rec[utils.KUBE_APISERVER],
                    cpu_scheduler=c_rec[utils.KUBE_SCHEDULER]))
        # estimate memory usage
        mem_baseline, mem_est = {}, {}
        mem_est_total = 0
        for comp in busy_comp:
            mem_baseline[comp] = self.fetchers[comp].get_mem_baseline()
            if comp == utils.KUBE_SCHEDULER:
                scheduler_stats = comp_stats[comp]
                n_q = scheduler_stats.get_req_in_queue()
                n_r = scheduler_stats.get_req_num()
                delta_t = scheduler_stats.get_delta_t_ms()
                c = scheduler_stats.get_cpu_load()
                mem_est[comp] = mem_baseline[comp] * min(n_q / n_r, c_rec[comp] * delta_t / c)
            elif comp == utils.KUBE_CONTROLLER_MANAGER:
                mem_est[comp] = mem_baseline[comp] * calibrate_factor_cm
            else:  # apiserver and etcd
                concurrency = comp_stats[comp].get_concurrency()
                cpu_utl = comp_stats[comp].get_cpu_utl()
                if concurrency is None or cpu_utl is None or len(concurrency) != len(cpu_utl):
                    concurrency = utils.CONCURRENCY_DICT[comp]
                    cpu_utl = utils.CPU_UTL_DICT[comp]
                # load calibration according to scheduler CPU allocation
                if utils.KUBE_SCHEDULER in busy_comp:
                    scheduler_stats = comp_stats[utils.KUBE_SCHEDULER]
                    n_q = scheduler_stats.get_req_in_queue()
                    n_r = scheduler_stats.get_req_num()
                    delta_t = scheduler_stats.get_delta_t_ms()
                    c = scheduler_stats.get_cpu_load()
                    n_rbi = comp_stats[comp].get_req_scheduler()
                    n_ri = comp_stats[comp].get_req_num()
                    scale_factor = 1 + (min(n_q / n_r, c_rec[utils.KUBE_SCHEDULER] * delta_t / c) - 1) * n_rbi / n_ri
                    qm = queue_model.QueueModelwithTruncation(
                        concurrency=concurrency, cpu_utl=cpu_utl,
                        total_load=comp_stats[comp].get_cpu_load() * scale_factor,
                        req_num=int(comp_stats[comp].get_req_num() * scale_factor),
                        delta_t=comp_stats[comp].get_delta_t()
                    )
                else:
                    qm = queue_model.QueueModelwithTruncation(
                        concurrency=concurrency, cpu_utl=cpu_utl,
                        total_load=comp_stats[comp].get_cpu_load(),
                        req_num=comp_stats[comp].get_req_num(),
                        delta_t=comp_stats[comp].get_delta_t()
                    )
                if qm.meet_steady_state_condition(cpu_alloc=c_rec[comp], flow_ctrl=f_rec[comp], max_ql=q_rec[comp]):
                    sl = qm.calc_l(flow_ctrl=f_rec[comp], cpu_alloc=c_rec[comp], max_ql=q_rec[comp])
                    l_q = qm.calc_l_q(flow_ctrl=f_rec[comp], cpu_alloc=c_rec[comp], max_ql=q_rec[comp])
                else:
                    sl = f_rec[comp] + q_rec[comp]
                    l_q = q_rec[comp]
                m_e = upcoming_kube_requests.get_memory_load_e(comp) / upcoming_kube_requests.get_req_num(comp)
                m_q = upcoming_kube_requests.get_memory_load_q(comp) / upcoming_kube_requests.get_req_num(comp)
                mem_est[comp] = mem_baseline[comp] + float(l_q) * m_q + float(sl - l_q) * m_e
            mem_est_total += mem_est[comp]
        if mem_est_total >= mem_left:
            if self.disable_mem_alloc:
                mem_total = mem_est_total
            else:
                mem_total = mem_est_total + sum(m_rec.values())
            utils.logging_or_print(
                f'{str(self)}: high risk of OOM (total memory: {self.mem_total} MB, '
                f'estimated memory: {mem_total} MB).', enable_logging=self.enable_logging, level=logging.WARNING)
        if not self.disable_mem_alloc:
            for comp in busy_comp:
                if comp in self.mem_request:
                    mem_left -= self.mem_request[comp]
            for comp in busy_comp:
                m_rec[comp] = mem_left * mem_est[comp] / mem_est_total
                if comp in self.mem_request:
                    m_rec[comp] += self.mem_request[comp]
        # unit transformation
        for comp in self.co_located_components:
            c_rec[comp] = int(c_rec[comp] * 1000)
            m_rec[comp] = int(m_rec[comp])
        # 4. generate final recommendation
        self._generate_final_recommendation(
            ts=rec_start, cpu_alloc=c_rec, mem_alloc=m_rec, concurrency_rec=f_rec, ql_rec=q_rec)
        return self.dry_run is False, upcoming_kube_requests

    # update methods
    def update_recommend_interval(self, new_interval: int):
        if new_interval <= 0:
            utils.logging_or_print(
                f'{str(self)}: invalid update interval, positive integer required, receive {new_interval} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        elif new_interval != self.recommend_interval:
            old_interval = self.recommend_interval
            self.recommend_interval = new_interval
            self._scheduler.reschedule_job(self.recommend_job.id, trigger='interval', seconds=new_interval)
            utils.logging_or_print(
                f'{str(self)}: recommend interval is updated from {old_interval} s to {new_interval} s.',
                enable_logging=self.enable_logging, level=logging.INFO)

    def update_load_query_interval(self, new_interval: int):
        if new_interval <= 0:
            utils.logging_or_print(
                f'{str(self)}: invalid update interval, positive integer required, receive {new_interval} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        elif new_interval != self.load_query_interval:
            old_interval = self.load_query_interval
            self.load_query_interval = new_interval
            self._scheduler.reschedule_job(self.load_query_job.id, trigger='interval', seconds=new_interval)
            utils.logging_or_print(
                f'{str(self)}: load query interval is updated from {old_interval} s to {new_interval} s.',
                enable_logging=self.enable_logging, level=logging.INFO)

    def update_buffer_size(self, new_buffer_size: int):
        if new_buffer_size <= 0:
            utils.logging_or_print(
                f'{str(self)}: invalid buffer size, positive integer required, receive {new_buffer_size} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        elif new_buffer_size != self.buffer_size:
            old_buffer_size = self.buffer_size
            self.buffer_size = new_buffer_size
            # remove oldest buffer
            diff_rec = len(self._recommendation_buffer) - self.buffer_size
            diff_req = len(self._metrics_buffer) - self.buffer_size
            utils.logging_or_print(
                f'{str(self)}: buffer size is updated from {old_buffer_size} to {new_buffer_size}.',
                enable_logging=self.enable_logging, level=logging.INFO)
            if diff_rec > 0:
                self._recommendation_buffer = self._recommendation_buffer[diff_rec:]
            if diff_req > 0:
                self._metrics_buffer = self._metrics_buffer[diff_req:]

    # jobs - load query
    def _load_query(self):  # assume timestamps and time windows of fetchers are the same
        latest_metrics = self.get_latest_metrics()
        if self.baseline in ['tsp', 'p99']:  # p99 fetcher
            metrics = {}
            for comp in self.co_located_components:
                metrics['ts'] = self.fetchers[comp].get_latest_timestamp()
                metrics['tw'] = self.fetchers[comp].get_latest_time_window()
                if metrics['ts'] == 0:
                    utils.logging_or_print(
                        f'{str(self)}: fetcher timestamp of {comp} is invalid, query canceled.',
                        enable_logging=self.enable_logging, level=logging.WARNING)
                    return
                if latest_metrics is not None and metrics['ts'] == latest_metrics['ts']:
                    utils.logging_or_print(
                        f'{str(self)}: fetcher timestamp of {comp} is the same as the latest timestamp, '
                        f'query canceled.', enable_logging=self.enable_logging, level=logging.WARNING)
                    return  # same timestamp, ignore
                metrics[comp] = {}
                metrics[comp]['cpu'] = self.fetchers[comp].get_p99_cpu()
                metrics[comp]['memory'] = self.fetchers[comp].get_p99_memory()
            self._metrics_buffer.append(metrics)
            if latest_metrics is not None and metrics['ts'] < latest_metrics['ts']:
                self._metrics_buffer.sort(key=lambda x: x['ts'])
            diff = len(self._metrics_buffer) - self.buffer_size
            if diff > 0:
                self._metrics_buffer = self._metrics_buffer[diff:]
            utils.logging_or_print(
                f'{str(self)} has received a fetcher summary (end timestamp: {metrics["ts"]}).',
                enable_logging=self.enable_logging, level=logging.INFO)
        if self.baseline in ['weighted', 'merkury', 'ga', 'disable_fc']:  # merkury fetcher
            kube_requests = {}
            for comp in self.co_located_components:
                kube_requests[comp] = self.fetchers[comp].get_load()
                if kube_requests[comp][1] <= 0:
                    utils.logging_or_print(
                        f'{str(self)}: request load time window of {comp} is invalid, query canceled.',
                        enable_logging=self.enable_logging, level=logging.WARNING)
                    return
                if latest_metrics is not None and kube_requests[comp][0] == latest_metrics.get_end_timestamp():
                    utils.logging_or_print(
                        f'{str(self)}: request load timestamp of {comp} is the same as the latest timestamp, '
                        f'query canceled.', enable_logging=self.enable_logging, level=logging.WARNING)
                    return  # same timestamp, ignore
            ts = kube_requests[self.co_located_components[0]][0]
            tw = kube_requests[self.co_located_components[0]][1]
            ts -= tw  # convert end ts to start ts
            summary = utils.KubeRequestsSummary(
                timestamp=ts, time_window=tw,
                apiserver_load=kube_requests[utils.KUBE_APISERVER][2] if utils.KUBE_APISERVER in kube_requests.keys() else utils.CompRequestsSummary(),
                etcd_load=kube_requests[utils.ETCD][2] if utils.ETCD in kube_requests.keys() else utils.CompRequestsSummary(),
                scheduler_load=kube_requests[utils.KUBE_SCHEDULER][2] if utils.KUBE_SCHEDULER in kube_requests.keys() else utils.CompRequestsSummary(),
                cm_load=kube_requests[utils.KUBE_CONTROLLER_MANAGER][2] if utils.KUBE_CONTROLLER_MANAGER in kube_requests.keys()
                else utils.CompRequestsSummary())
            self._metrics_buffer.append(summary)
            if latest_metrics is not None and ts < latest_metrics.timestamp:
                self._metrics_buffer.sort(key=lambda x: x.timestamp)
            diff = len(self._metrics_buffer) - self.buffer_size
            if diff > 0:
                self._metrics_buffer = self._metrics_buffer[diff:]
            utils.logging_or_print(
                f'{str(self)} has received a kube request summary (start timestamp:{ts}, time window: {tw} s).',
                enable_logging=self.enable_logging, level=logging.INFO)

    # jobs - recommend
    def _recommend(self):
        # 1. fetch latest load
        for comp in self.co_located_components:
            self.fetchers[comp].load_update()
        # 2. get latest load
        self._load_query()
        # 3. resource and fc allocation
        allocated = False
        upcoming_kube_requests = None
        if self.baseline == 'merkury':
            allocated, upcoming_kube_requests = self._recommend_merkury()
        elif self.baseline == 'evo-alg':
            self._recommend_evo_alg()
        elif self.baseline == 'p99':
            self._recommend_p99()
        elif self.baseline == 'tsp':
            self._recommend_tsp()
        elif self.baseline == 'weighted':
            self._recommend_weighted()
        else:
            return
        if self.baseline == 'merkury' and self.alloc_calibration and allocated:
            time.sleep(self.recommend_interval / 2)
            self._allocation_calibrate(upcoming_kube_requests)

    # frequently used methods
    def _alloc_resource_according_to_weights(self, weights: dict[str, float | int], allocatable: float,
                                             lower_bounds: dict[str, float | int],
                                             upper_bounds: dict[str, float | int] | None = None,
                                             re_alloc: bool = True) -> dict | None:
        _warn_str = ('must be a dict that contains 1-4 keys in (\'kube-apiserver\', \'etcd\', \'kube-scheduler\', and '
                     '\'kube-controller-manager\') and each value is a non-negative number.')
        total_weights = 0
        if type(weights) is not dict or len(weights) == 0:
            utils.logging_or_print(f'{str(self)}: weights {_warn_str}', enable_logging=self.enable_logging,
                                   level=logging.WARNING)
            return
        if type(allocatable) not in (int, float) or allocatable <= 0:
            utils.logging_or_print(f'{str(self)}: invalid allocatable',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        for comp in weights:
            if comp not in utils.CONTROL_PLANE_COMPONENTS or type(weights[comp]) not in [int, float, decimal.Decimal] \
                    or weights[comp] < 0:
                utils.logging_or_print(f'{str(self)}: weights {_warn_str}',
                                       enable_logging=self.enable_logging, level=logging.WARNING)
                return
            if comp not in lower_bounds:
                utils.logging_or_print(f'{str(self)}: cpu_lower_bounds {_warn_str}',
                                       enable_logging=self.enable_logging, level=logging.WARNING)
                return
            total_weights += float(weights[comp])
        alloc = {}
        for comp in weights:
            if total_weights == 0:
                alloc[comp] = (lower_bounds[comp] + (allocatable - sum(lower_bounds.values())) /
                               len(weights))
            else:
                alloc[comp] = lower_bounds[comp] + (allocatable - sum(lower_bounds.values())) * float(
                    weights[comp]) / total_weights
        if re_alloc:
            realloc_cpu = {}
            over_alloc = 0
            over_alloc_comp = []
            under_alloc = 0
            under_alloc_comp = []
            for comp in weights:
                realloc_cpu[comp] = alloc[comp] - upper_bounds[comp]
                if realloc_cpu[comp] > 0:
                    over_alloc += realloc_cpu[comp]
                    over_alloc_comp.append(comp)
                else:
                    under_alloc -= realloc_cpu[comp]
                    if realloc_cpu[comp] < 0:
                        under_alloc_comp.append(comp)
            while over_alloc > 0 and under_alloc > 0:
                for comp1 in over_alloc_comp:
                    cpu_avail = realloc_cpu[comp1]
                    sum_weight = 0
                    for comp2 in under_alloc_comp:
                        sum_weight += float(weights[comp2])
                    for comp2 in under_alloc_comp:
                        if sum_weight > 0:
                            realloc = min(-realloc_cpu[comp2], cpu_avail * weights[comp2] / sum_weight)
                        else:  # if the weight of under_alloc_comp is 0, allocate evenly
                            realloc = min(-realloc_cpu[comp2], cpu_avail / len(under_alloc_comp))
                        alloc[comp2] += realloc
                        realloc_cpu[comp2] += realloc
                        alloc[comp1] -= realloc
                        realloc_cpu[comp1] -= realloc
                        over_alloc -= realloc
                        under_alloc -= realloc
                        if realloc_cpu[comp2] == 0:
                            under_alloc_comp.remove(comp2)
                        if realloc_cpu[comp1] == 0:
                            over_alloc_comp.remove(comp1)
        return alloc

    def _load_check(self, prediction: utils.KubeRequestsSummary, ts: float) -> bool:
        """
        check whether total CPU load >= cpu_total * load_threshold
        """
        cpu_load_total = (sum([prediction.get_cpu_load(comp) for comp in self.co_located_components]) /
                          (1000 * prediction.time_window))
        if cpu_load_total < self.cpu_total * self.load_threshold:
            utils.logging_or_print(
                f'{str(self)}: predicted CPU load {cpu_load_total:.2f} < '
                f'threshold {(self.cpu_total * self.load_threshold):.2f}, reset limits of all components.',
                enable_logging=self.enable_logging, level=logging.INFO)
            if not self.dry_run:
                for comp in self.co_located_components:
                    self.rs_updater.update_resource_with_k8s_client(
                        component=comp, pod_name=f'{comp}-{self.master_name[-1]}-0',
                        cpu_value=int(self.cpu_total * 1000), memory_value=self.mem_total, ignore_update_range=True)
            end = time.time()
            utils.logging_or_print(
                f'{str(self)}: finished updating, using {(end - ts):.3f} s.',
                enable_logging=self.enable_logging, level=logging.INFO)
            self.last_state_recommended = False
            return False
        return True

    def _get_idle_and_busy_components(self, upcoming_kube_requests: utils.KubeRequestsSummary):
        idle_comp, busy_comp = [], []
        for comp in self.co_located_components:
            master_state = self.fetchers[comp].get_master_state()
            if not master_state:
                idle_comp.append(comp)
            elif comp == utils.KUBE_SCHEDULER and upcoming_kube_requests.get_req_num(comp) == 0:
                idle_comp.append(comp)
            else:
                busy_comp.append(comp)
        return idle_comp, busy_comp

    def _get_busy_comp_code(self, busy_comp) -> str:
        busy_comp_code = ''
        busy_comp_code = busy_comp_code.zfill(len(self.co_located_components))
        busy_comp_code_list = list(busy_comp_code)
        for i, comp in enumerate(self.co_located_components):
            if comp in busy_comp:
                busy_comp_code_list[i] = '1'
        busy_comp_code = ''.join(busy_comp_code_list)
        return busy_comp_code

    def _steady_state_check(self, busy_components: list, cpu_left: float, cpu_lb: dict, cpu_ub: dict) \
            -> (bool, bool, bool, list):
        sum_cpu_lb = sum([cpu_lb[comp] for comp in busy_components])
        steady_state_global = sum_cpu_lb < cpu_left
        if not steady_state_global:
            warn_str = 'CPU needed for '
            for i, comp in enumerate(busy_components):
                warn_str += f'{comp}: {cpu_lb[comp]:.3f}core'
                if i != len(busy_components) - 1:
                    warn_str += ', '
            utils.logging_or_print(
                f'{str(self)}: CPU resources of node {self.master_name} ({self.master_addr}) are not '
                f'enough to handle the current request, please adopt load balancing or scaling strategies to ensure '
                f'the smooth operation of the cluster ({warn_str}; CPU left: {cpu_left:.3f}core).',
                enable_logging=self.enable_logging, level=logging.WARNING)
        steady_state_local = True
        unsteady_components = []
        for comp in busy_components:
            if cpu_ub[comp] <= cpu_lb[comp]:
                steady_state_local = False
                unsteady_components.append(comp)
                utils.logging_or_print(
                    f'{str(self)}: CPU resources upper bound for {comp} (node: {self.master_addr}) are '
                    f'not enough to handle the current request, please adopt load balancing or scaling strategies to '
                    f'ensure the smooth operation of the cluster (CPU needed for {comp}: {cpu_lb[comp]:.3f}'
                    f', current upper bound: {cpu_ub[comp]:.3f}).',
                    enable_logging=self.enable_logging, level=logging.WARNING)
        steady_state = steady_state_global and steady_state_local
        return steady_state, steady_state_global, steady_state_local, unsteady_components

    def _generate_final_recommendation(
            self, ts: float, cpu_alloc: dict, mem_alloc: dict, concurrency_rec: dict = None, ql_rec: dict = None):
        rec = {}
        for comp in self.co_located_components:
            if concurrency_rec is None or ql_rec is None:
                rec[comp] = utils.Recommendation(cpu_alloc=cpu_alloc[comp], memory_alloc=mem_alloc[comp])
            else:
                rec[comp] = utils.Recommendation(cpu_alloc=cpu_alloc[comp], memory_alloc=mem_alloc[comp],
                                                 concurrency=concurrency_rec[comp], max_ql=ql_rec[comp])
        node_rec = utils.RecommendationForNode(timestamp=int(ts), recommendations=rec)
        rec_end = time.time()
        rec_time = rec_end - ts
        self._recommendation_buffer.append(node_rec)
        diff = len(self._recommendation_buffer) - self.buffer_size
        if diff > 0:
            self._recommendation_buffer = self._recommendation_buffer[diff:]
        utils.logging_or_print(
            f'{str(self)}: new recommendation is appended ({str(node_rec)}), using {rec_time:.3f} s.',
            enable_logging=self.enable_logging, level=logging.INFO)
        if not self.dry_run:
            # update resource
            for comp in self.co_located_components:
                self.rs_updater.update_resource_with_k8s_client(
                    component=comp, pod_name=f'{comp}-{self.master_name[-1]}-0',
                    cpu_value=cpu_alloc[comp], memory_value=mem_alloc[comp],
                    ignore_update_range=self.last_state_recommended is False)
            if (concurrency_rec is not None and ql_rec is not None and self.is_master and
                    utils.KUBE_APISERVER in self.co_located_components):
                # update FC params
                self.tc_updater.update_fc_with_k8s_client(
                    concurrency=concurrency_rec[utils.KUBE_APISERVER], queue_length=ql_rec[utils.KUBE_APISERVER])
            update_end = time.time()
            update_time = update_end - rec_end
            utils.logging_or_print(
                f'{str(self)}: finished updating, using {update_time:.3f} s.',
                enable_logging=self.enable_logging, level=logging.INFO)
            self.last_state_recommended = True

    def _allocation_calibrate(self, upcoming_kube_requests: utils.KubeRequestsSummary):
        ts_now = int(time.time())
        ts_last = ts_now - self.recommend_interval
        # 1. get current allocation
        cpu_limit = {comp: None for comp in self.co_located_components}
        have_limit = True
        for comp in self.co_located_components:
            cpu_limit[comp], _ = self.rs_updater.get_pod_limit(
                component=comp, pod_name=f'{comp}-{self.master_name[-1]}-0')
            if cpu_limit[comp] is None:
                have_limit = False
        # 2. get cpu throttle
        cpu_throttle = {comp: -0.1 for comp in self.co_located_components}
        for comp in self.co_located_components:
            cpu_throttle_data = self.fetchers[comp].query_cpu_throttle(ts_now, ts_last)
            if cpu_throttle_data is not None:
                cpu_throttle[comp] = cpu_throttle_data
        # 3. get idle and busy components
        idle_components, busy_components = self._get_idle_and_busy_components(upcoming_kube_requests)
        # 4. calibrate allocation
        available_cpu = self.cpu_total * 1000  # cpu unallocated
        if have_limit:
            available_cpu -= sum(cpu_limit.values())
        available_components, bottleneck_components = [], []
        for comp in self.co_located_components:
            if cpu_limit[comp] is not None and cpu_throttle[comp] >= 0.0 and comp in busy_components:
                if cpu_throttle[comp] <= utils.THROTTLE_IDLE[comp]:
                    available_components.append(comp)
                elif cpu_throttle[comp] >= utils.THROTTLE_BOTTLENECK[comp]:
                    bottleneck_components.append(comp)
        if len(bottleneck_components) > 0:  # increase allocation weights for bottleneck components
            busy_comp_code = self._get_busy_comp_code(busy_components)
            for comp in bottleneck_components:
                self.allocation_weights[busy_comp_code][comp] += utils.WEIGHTS_CALIBRATOR
                utils.logging_or_print(
                    f'{str(self)}: allocation weights updated for {comp} (busy components code: {busy_comp_code}) '
                    f'- {self.allocation_weights[busy_comp_code][comp]}.', enable_logging=self.enable_logging,
                    level=logging.INFO)
        if available_cpu > 0 and len(bottleneck_components) > 0:
            for comp in available_components:
                lendable_cpu = int(utils.LENDABLE_PERCENTAGE * cpu_limit[comp])
                available_cpu += lendable_cpu
                cpu_limit[comp] -= lendable_cpu
                self.rs_updater.update_resource_with_k8s_client(
                    component=comp, pod_name=f'{comp}-{self.master_name[-1]}-0', cpu_value=cpu_limit[comp])
            bottleneck_throttle = 0.0
            for comp in bottleneck_components:
                bottleneck_throttle += cpu_throttle[comp]
            for comp in bottleneck_components:
                cpu_limit[comp] += int(available_cpu * cpu_throttle[comp] / bottleneck_throttle)
                self.rs_updater.update_resource_with_k8s_client(
                    component=comp, pod_name=f'{comp}-{self.master_name[-1]}-0', cpu_value=cpu_limit[comp])
            utils.logging_or_print(f'{str(self)}: allocation calibration finished.',
                                   enable_logging=self.enable_logging, level=logging.INFO)
        else:
            utils.logging_or_print(
                f'{str(self)}: no available CPU or bottleneck components, allocation calibration canceled.',
                enable_logging=self.enable_logging, level=logging.INFO)

    def start(self):
        if not self._scheduler.running:
            for comp in self.fetchers:
                self.fetchers[comp].start()
            # self._load_query()
            self._scheduler.start()  # start after fetchers start to get latest load
            if self._scheduler.running:
                utils.logging_or_print(
                    f'{str(self)} starts running.',
                    enable_logging=self.enable_logging, level=logging.INFO)
        else:
            utils.logging_or_print(f'{str(self)} has already been running.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)

    def shutdown(self, shutdown_all: bool = True):
        if self._scheduler.running:
            self._scheduler.shutdown()
        else:
            utils.logging_or_print(f'{str(self)} has already been shutdown.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
        if shutdown_all:
            for comp in self.fetchers:
                self.fetchers[comp].shutdown()
            utils.logging_or_print(
                f'{str(self)} and its metric fetchers have been shutdown.',
                enable_logging=self.enable_logging, level=logging.INFO)
        else:
            utils.logging_or_print(
                f'{str(self)} has been shutdown.',
                enable_logging=self.enable_logging, level=logging.INFO)
