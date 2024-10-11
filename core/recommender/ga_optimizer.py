import decimal
import math
import numpy as np
import random
from tqdm import tqdm
from core.utils import utils
from core.queue_model import queue_model
import time


class Optimizer:
    def __init__(self, cpu_total: int | float, component_stats: dict, scheduler: utils.AsyncCompStats, weights: dict,
                 cpu_granularity: int = utils.DEFAULT_CPU_GRANULARITY, cpu_request: dict = utils.DEFAULT_CPU_REQ,
                 brute_force: bool = False, disable_fc: bool = False, default_concurrency: int | None = None,
                 default_queue_length: int = utils.DEFAULT_QUEUE_LENGTH, pop_size: int = utils.GA_POP_SIZE,
                 max_gen: int = utils.GA_MAX_GEN, select_rate: float = utils.GA_SELECT_RATE,
                 cross_rate: float = utils.GA_CROSS_RATE, mutate_rate: float = utils.GA_MUTATE_RATE,
                 es_threshold: float = utils.GA_ES_THRESHOLD, es_max_gen: int = utils.GA_ES_MAX_GEN,
                 debugging: bool = False):
        # hyper params of GA
        if not isinstance(pop_size, int) or pop_size <= 0:
            raise ValueError(f'pop_size must be positive integer, received {pop_size} ({type(pop_size)}) instead')
        if not isinstance(max_gen, int) or max_gen <= 0:
            raise ValueError(f'max_gen must be positive integer, received {max_gen} ({type(max_gen)}) instead')
        if not isinstance(select_rate, float) or not 0 < select_rate < 1:
            raise ValueError(f'select_rate must be float in (0, 1), received {select_rate} instead')
        if not isinstance(cross_rate, float) or not 0 < cross_rate < 1:
            raise ValueError(f'cross_rate must be float in (0, 1), received {cross_rate} instead')
        if mutate_rate != 0 and (not isinstance(mutate_rate, float) or not 0 < mutate_rate < 1):
            raise ValueError(f'mutate_rate must be 0 or float in (0, 1), received {mutate_rate} instead')
        if not isinstance(es_threshold, float) or es_threshold <= 0:
            raise ValueError(f'es_threshold must be positive float, received {es_threshold} instead')
        if not isinstance(es_max_gen, int) or not 0 < es_max_gen < max_gen:
            raise ValueError(f'es_max_gen must be integer in (0, {max_gen}), received {es_max_gen} instead')
        self.pop_size = pop_size
        self.max_gen = max_gen
        self.select_rate = select_rate
        self.cross_rate = cross_rate
        self.mutate_rate = mutate_rate
        self.es_threshold = es_threshold
        self.es_max_gen = es_max_gen
        # options
        self.debugging = debugging  # for test only
        self.brute_force = brute_force
        self.disable_fc = disable_fc
        if disable_fc and default_concurrency is None:
            raise ValueError('missing default_concurrency when disable_fc')
        self.default_concurrency = default_concurrency
        self.default_queue_length = default_queue_length
        # cpu granularity and cpu total
        if not isinstance(cpu_granularity, int) or cpu_granularity <= 0:
            raise ValueError(f'cpu_granularity must be positive integer, received {cpu_granularity} '
                             f'({type(cpu_granularity)}) instead')
        self.cpu_granularity = cpu_granularity  # 1/n core
        if type(cpu_total) not in [int, float] or cpu_total <= 0:
            raise ValueError(f'cpu_total must be positive int or float, received {cpu_total} '
                             f'({type(cpu_total)}) instead')
        self.cpu_total = math.floor(cpu_total * self.cpu_granularity)
        # component stats for sync components (kube-apiserver and etcd)
        self.component_stats = {}
        self.cpu_requests = {}
        self.weights = {}
        if not isinstance(component_stats, dict):
            raise ValueError(f'component_stats must be dict, received {type(component_stats)} instead')
        for comp in component_stats:
            if comp in [utils.KUBE_APISERVER, utils.ETCD]:
                if not isinstance(component_stats[comp], utils.SyncCompStats):
                    raise ValueError(f'values of component_stats must be SyncCompStats, '
                                     f'received {type(component_stats[comp])} instead')
                if comp not in cpu_request:
                    raise ValueError('keys of component_stats must be in cpu_request')
                if type(cpu_request[comp]) not in [int, float]:
                    raise ValueError(f'values of cpu_request must be int or float, received '
                                     f'{type(cpu_request[comp])} instead')
                if not component_stats[comp].is_empty():
                    self.component_stats[comp] = component_stats[comp]
                    self.cpu_requests[comp] = math.ceil(cpu_request[comp] * self.cpu_granularity)
        # scheduler
        if not isinstance(scheduler, utils.AsyncCompStats):
            raise ValueError(f'scheduler must be AsyncCompStats, received {type(scheduler)} instead')
        self.scheduler = scheduler
        self.comp_num = len(self.component_stats)
        self.sample_to_comp = [comp for comp in self.component_stats]
        if not self.scheduler.is_empty():
            self.comp_num += 1
            self.sample_to_comp.append(utils.KUBE_SCHEDULER)
            self.cpu_requests[utils.KUBE_SCHEDULER] = (
                math.ceil(cpu_request[utils.KUBE_SCHEDULER] * self.cpu_granularity))
        if self.comp_num <= 0:
            raise ValueError('no component to optimize')
        # weights
        self.weights = {}
        if not isinstance(weights, dict):
            raise ValueError(f'weights must be dict, received {type(weights)} instead')
        for comp in weights:
            if comp not in utils.CONTROL_PLANE_COMPONENTS:
                raise ValueError(f'keys of weights must be one of {utils.CONTROL_PLANE_COMPONENTS}, '
                                 f'received {comp} instead')
            if type(weights[comp]) not in [int, float] or weights[comp] <= 0:
                raise ValueError(f'values of weights must be positive int or float, received {weights[comp]} '
                                 f'({type(weights[comp])}) instead')
            if (comp in self.component_stats or
                    (not self.scheduler.is_empty() and comp == utils.KUBE_SCHEDULER)):
                self.weights[comp] = weights[comp]
        # no scheduler - store queue models and calculate f_rec, cpu_lb and cpu_left
        if self.scheduler.is_empty():
            self.queue_models = {}
            self.cpu_lb = {}
            self.f_rec = {}
            for comp in self.component_stats:
                self.queue_models[comp] = queue_model.QueueModelwithTruncation(
                    concurrency=self.component_stats[comp].get_concurrency(),
                    cpu_utl=self.component_stats[comp].get_cpu_utl(),
                    total_load=self.component_stats[comp].get_cpu_load(),
                    req_num=self.component_stats[comp].get_req_num(),
                    delta_t=self.component_stats[comp].get_delta_t())
                cpu_lb_temp = float(self.queue_models[comp].get_cpu_lower_bound())
                qmlb_as_lb = cpu_lb_temp >= cpu_request[comp]
                cpu_lb = math.ceil(max(cpu_lb_temp, cpu_request[comp]) * self.cpu_granularity)
                if qmlb_as_lb and cpu_lb - (cpu_lb_temp * self.cpu_granularity) < 1e-3:
                    cpu_lb += 1
                self.cpu_lb[comp] = cpu_lb
                if self.brute_force:
                    self.f_rec[comp] = self.queue_models[comp].get_available_fc()
                else:
                    self.f_rec[comp] = self.queue_models[comp].get_flow_ctrl_rec()
            self.cpu_left = self.cpu_total - sum(self.cpu_lb.values())
        # with scheduler - calculate scheduler cpu range and
        # initialize qm_dict for less calculation when optimizer running
        else:
            self._scheduler_available_cpu = []  # available CPU choices of scheduler
            lb_0 = self.cpu_requests[utils.KUBE_SCHEDULER]
            ub_0 = self.cpu_total - sum([self.cpu_requests[comp] for comp in self.component_stats])
            alpha = self.scheduler.get_req_in_queue() / self.scheduler.get_req_num()
            threshold = self.scheduler.get_cpu_load() * alpha / self.scheduler.get_delta_t_ms()
            # case 1: alpha <= (c[scheduler] * delta_t) / (cpu_load)
            lb_1 = math.ceil(threshold * self.cpu_granularity)  # approachable
            sum_1 = 0
            for stats in self.component_stats.values():
                sum_1 += ((1 + (alpha - 1) * stats.get_req_scheduler() / stats.get_req_num())
                          * stats.get_cpu_load() / stats.get_delta_t_ms())
            ub_1 = self.cpu_total / self.cpu_granularity - sum_1
            ub_1_temp = math.floor(ub_1 * self.cpu_granularity)
            if ub_1 * self.cpu_granularity - ub_1_temp < 1e-3:
                ub_1 = ub_1_temp - 1
            else:
                ub_1 = ub_1_temp  # approachable
            self._scheduler_available_cpu.extend(list(range(max(lb_0, lb_1), min(ub_0, ub_1) + 1)))
            # case 2: alpha > (c[scheduler] * delta_t) / (cpu_load)
            ub_2a = math.floor(threshold * self.cpu_granularity)
            if ub_2a == lb_1:
                ub_2a -= 1  # no repeat
            ub_2b_num = self.cpu_total / self.cpu_granularity
            ub_2b_denom = 1
            for stats in self.component_stats.values():
                ub_2b_num -= ((1 - stats.get_req_scheduler() / stats.get_req_num()) *
                              stats.get_cpu_load() / stats.get_delta_t_ms())
                ub_2b_denom += (stats.get_req_scheduler() * stats.get_cpu_load() /
                                (stats.get_req_num() * self.scheduler.get_cpu_load()))
            ub_2b = ub_2b_num / ub_2b_denom
            ub_2b_temp = ub_2b * self.cpu_granularity
            if ub_2b_temp - math.floor(ub_2b_temp) < 1e-3:
                ub_2b = math.floor(ub_2b_temp) - 1
            else:
                ub_2b = math.floor(ub_2b_temp)
            self._scheduler_available_cpu.extend(list(range(lb_0, min(ub_0, ub_2a, ub_2b) + 1)))
            self._qm_dict = {comp: {} for comp in self.component_stats}
            self._scale_factor_dict = {comp: {} for comp in self.component_stats}

    def get_scale_factor_for_sync_comp(self, scheduler_cpu: int, comp: str) -> float | None:
        if (comp not in self.component_stats or self.scheduler.is_empty() or
                scheduler_cpu not in self._scheduler_available_cpu):
            return
        if scheduler_cpu in self._scale_factor_dict[comp]:
            return self._scale_factor_dict[comp][scheduler_cpu]
        else:
            n_q = self.scheduler.get_req_in_queue()
            n_r = self.scheduler.get_req_num()
            c_star = scheduler_cpu / self.cpu_granularity
            delta_t = self.scheduler.get_delta_t_ms()
            c = self.scheduler.get_cpu_load()
            stats = self.component_stats[comp]
            n_rbi = stats.get_req_scheduler()
            n_ri = stats.get_req_num()
            scale_factor = 1 + (min(n_q / n_r, c_star * delta_t / c) - 1) * n_rbi / n_ri
            self._scale_factor_dict[comp][scheduler_cpu] = scale_factor
        return scale_factor

    def _init_qm_dict(self, scheduler_cpu: int, comp: str) -> bool:  # return whether initialization successes
        if (comp not in self.component_stats or self.scheduler.is_empty() or
                scheduler_cpu not in self._scheduler_available_cpu):
            return False
        if scheduler_cpu not in self._qm_dict[comp]:
            scale_factor = self.get_scale_factor_for_sync_comp(scheduler_cpu, comp)
            if scale_factor is None:
                return False
            self._qm_dict[comp][scheduler_cpu] = queue_model.QueueModelwithTruncation(
                concurrency=self.component_stats[comp].get_concurrency(),
                cpu_utl=self.component_stats[comp].get_cpu_utl(),
                total_load=self.component_stats[comp].get_cpu_load() * scale_factor,
                req_num=int(self.component_stats[comp].get_req_num() * scale_factor),
                delta_t=self.component_stats[comp].get_delta_t_ms())
        return True

    def get_qm(self, scheduler_cpu: int, comp: str) -> queue_model.QueueModelwithTruncation | None:
        if (comp not in self.component_stats or self.scheduler.is_empty() or
                scheduler_cpu not in self._scheduler_available_cpu):
            return None
        if scheduler_cpu not in self._qm_dict[comp]:
            return None
        else:
            return self._qm_dict[comp][scheduler_cpu]

    def init_c(self) -> list:  # generate [c*] randomly, satisfying ∑c*=C and c*[i]>cpu_lb[i]
        # no scheduler - the same as ga_optimizer
        if self.scheduler.is_empty():
            if self.cpu_left < 0:
                return []
            if self.cpu_left == 0:
                return [self.cpu_lb[comp] for comp in self.cpu_requests]
            # generate a sorted list containing 0, rand * (comp_num-1), left
            points = sorted([0] + random.sample(range(1, self.cpu_left), self.comp_num - 1) + [self.cpu_left])
            # calculate diff between 2 neighboring numbers in the list (generated random number)
            numbers = [points[i + 1] - points[i] for i in range(self.comp_num)]
            # add cpu_lb[i] to corresponding random number
            cpu = [numbers[i] + self.cpu_lb[self.sample_to_comp[i]] for i in range(self.comp_num)]
            return cpu
        # with scheduler
        else:
            # 1. generate c*[-1] for scheduler
            if len(self._scheduler_available_cpu) == 0:
                return []
            else:
                scheduler_cpu = random.choice(self._scheduler_available_cpu)
                # 2. generate c* for sync components
                cpu_lb = []
                for comp in self.component_stats:
                    cpu_request = self.cpu_requests[comp]
                    scale_factor = self.get_scale_factor_for_sync_comp(scheduler_cpu, comp)
                    if scale_factor is None:
                        return []
                    cpu_load = scale_factor * self.component_stats[comp].get_cpu_load()
                    cpu_lb_temp = cpu_load / self.component_stats[comp].get_delta_t_ms() * self.cpu_granularity
                    if math.ceil(cpu_lb_temp) - cpu_lb_temp < 1e-3:
                        cpu_lb_temp = math.ceil(cpu_lb_temp) + 1  # approachable
                    else:
                        cpu_lb_temp = math.ceil(cpu_lb_temp)
                    cpu_lb.append(max(cpu_lb_temp, cpu_request))
                cpu_left = self.cpu_total - scheduler_cpu - sum(cpu_lb)
                points = sorted([0] + random.sample(range(1, cpu_left), self.comp_num - 2) + [cpu_left])
                numbers = [points[i + 1] - points[i] for i in range(self.comp_num - 1)]
                cpu = [numbers[i] + cpu_lb[i] for i in range(self.comp_num - 1)]
                cpu.append(scheduler_cpu)
                return cpu

    def init_q(self, c, comp, f: int | None = None) -> int:  # generate q* for single c*
        if comp == utils.KUBE_SCHEDULER:
            return 0
        if f is None:
            f = self.f_rec[comp]
        return self.queue_models[comp].get_ql_rec(
            flow_ctrl=f, cpu_alloc=decimal.Decimal(c / self.cpu_granularity))

    def init_f(self) -> list:  # generate [f*]
        return [random.choice(self.f_rec[self.sample_to_comp[i]]) for i in range(self.comp_num)]

    def init_sample(self) -> list:  # generate [(f*, c*, q*)] randomly, but not violating constraints
        cpu = self.init_c()
        if len(cpu) == 0:
            return []
        if self.disable_fc:
            sample = [[self.default_concurrency, cpu[i], self.default_queue_length] for i in range(self.comp_num)]
        else:
            if self.scheduler.is_empty():
                if self.brute_force:
                    f = self.init_f()
                    q = [self.init_q(cpu[i], self.sample_to_comp[i], f[i]) for i in range(self.comp_num)]
                    sample = [[f[i], cpu[i], q[i]] for i in range(self.comp_num)]
                else:
                    q = [self.init_q(cpu[i], self.sample_to_comp[i]) for i in range(self.comp_num)]
                    sample = [[self.queue_models[self.sample_to_comp[i]].get_flow_ctrl_rec(
                        cpu_alloc=cpu[i] / self.cpu_granularity), cpu[i], q[i]] for i in range(self.comp_num)]
            else:
                scheduler_cpu = cpu[-1]
                for comp in self.component_stats:
                    if scheduler_cpu not in self._qm_dict[comp]:
                        init_success = self._init_qm_dict(scheduler_cpu, comp)
                        if not init_success:
                            return []
                if self.brute_force:
                    f = [random.choice(self._qm_dict[comp][scheduler_cpu].get_available_fc())
                         for comp in self.component_stats]
                else:
                    f = [self._qm_dict[comp][scheduler_cpu].get_flow_ctrl_rec(cpu_alloc=cpu[i] / self.cpu_granularity)
                         for i, comp in enumerate(self.component_stats)]
                q = []
                for i in range(len(self.component_stats)):
                    q.append(self._qm_dict[self.sample_to_comp[i]][scheduler_cpu].get_ql_rec(
                        flow_ctrl=f[i], cpu_alloc=decimal.Decimal(cpu[i] / self.cpu_granularity)))
                f.append(0)
                q.append(0)
                sample = [[f[i], cpu[i], q[i]] for i in range(self.comp_num)]
        return sample

    def obj_func(self, sample: list) -> decimal.Decimal | float | int:
        if not isinstance(sample, list) or len(sample) == 0:
            raise ValueError(f'sample must be non-empty list, received {sample} instead')
        for s in sample:
            if len(s) != 3:
                raise ValueError(f'elements of sample must be list with 3 elements, received {s} instead')
        # for sync components: ∑ weights[comp] * w[comp]
        # for scheduler:
        # 1000 * weights[scheduler] / min(req_in_queue / delta_t, c[scheduler] * req_num / cpu_load)
        func_value = 0
        if self.scheduler.is_empty():
            for i in range(len(sample)):
                comp = self.sample_to_comp[i]
                f, c, q = sample[i][0], decimal.Decimal(sample[i][1] / self.cpu_granularity), sample[i][2]
                p_r = self.queue_models[comp].calc_p_reject(flow_ctrl=f, cpu_alloc=c, max_ql=q)
                sl = self.queue_models[comp].calc_l(flow_ctrl=f, cpu_alloc=c, max_ql=q)
                w = float(sl / (self.queue_models[comp].get_lambda() * (1 - p_r)))
                func_value += self.weights[comp] * w
        else:
            scheduler_cpu = sample[-1][1]
            # scheduler part func_value
            req_in_queue = self.scheduler.get_req_in_queue()
            req_num = self.scheduler.get_req_num()
            delta_t = self.scheduler.get_delta_t_ms()
            cpu_load = self.scheduler.get_cpu_load()
            estimated_scheduling_throughput = min(
                req_in_queue / delta_t, scheduler_cpu / self.cpu_granularity * req_num / cpu_load)  # unit: pod / ms
            # func_value -= self.weights[utils.KUBE_SCHEDULER] * estimated_scheduling_throughput
            func_value += (1000 * self.weights[utils.KUBE_SCHEDULER] /
                           (1 + estimated_scheduling_throughput))
            # sync components part func_value
            for i in range(len(self.component_stats)):
                comp = self.sample_to_comp[i]
                f, c, q = sample[i][0], decimal.Decimal(sample[i][1] / self.cpu_granularity), sample[i][2]
                if scheduler_cpu not in self._qm_dict[comp]:
                    init_success = self._init_qm_dict(scheduler_cpu, comp)
                    if not init_success:
                        return 0
                p_r = self._qm_dict[comp][scheduler_cpu].calc_p_reject(flow_ctrl=f, cpu_alloc=c, max_ql=q)
                sl = self._qm_dict[comp][scheduler_cpu].calc_l(flow_ctrl=f, cpu_alloc=c, max_ql=q)
                w = float(sl / (self._qm_dict[comp][scheduler_cpu].get_lambda() * (1 - p_r)))
                func_value += self.weights[comp] * w
        return func_value

    def fitness_function(self, sample):
        return -self.obj_func(sample)  # min obj_func() -> max fitness_func()

    def run(self) -> (dict | None, decimal.Decimal | float | int | None, list | None):
        if ((self.scheduler.is_empty() and self.cpu_left < 0) or
                (not self.scheduler.is_empty() and len(self._scheduler_available_cpu) == 0)):
            return None, None, None
        if self.comp_num == 1:  # only one component, no need to iterate
            comp = self.sample_to_comp[0]
            if comp == utils.KUBE_SCHEDULER:
                sample = [[0, self.cpu_total, 0]]
                best_fitness = self.fitness_function(sample)
                return {comp: [0, self.cpu_total / self.cpu_granularity, 0]}, best_fitness, [best_fitness]
            elif not self.brute_force:
                if self.disable_fc:
                    sample = [[self.default_concurrency, self.cpu_total, self.default_queue_length]]
                else:
                    sample = [[self.f_rec[comp], self.cpu_total,
                               self.init_q(c=self.cpu_total, comp=comp, f=self.f_rec[comp])]]
                best_fitness = self.fitness_function(sample)
                return ({comp: [sample[0][0], self.cpu_total / self.cpu_granularity, sample[0][2]]}, best_fitness,
                        [best_fitness])
        # init population
        population = []
        if self.debugging:
            start = time.time()
            with tqdm(total=self.pop_size, desc='generating samples', unit='sample') as pbar:
                while len(population) < self.pop_size:
                    population.append(self.init_sample())
                    pbar.update(1)
            end = time.time()
            if self.debugging:
                print(f'Time for sample generation: {end - start} s')
        else:
            while len(population) < self.pop_size:
                population.append(self.init_sample())

        def crossover_calibration(sample) -> list:
            if self.scheduler.is_empty():  # without scheduler
                excessive_cpu = [sample[i][1] - self.cpu_lb[self.sample_to_comp[i]] for i in range(self.comp_num)]
                norm_factor = self.cpu_left / sum(excessive_cpu)
                excessive_cpu = [round(ec * norm_factor) for ec in excessive_cpu]
                diff = sum(excessive_cpu) - self.cpu_left
                if diff > 0:
                    index_available = [i for i in range(self.comp_num) if excessive_cpu[i] > 0]
                    while diff != 0:
                        index_to_cal = random.choice(index_available)
                        excessive_cpu[index_to_cal] -= 1
                        if excessive_cpu[index_to_cal] == 0:
                            index_available.remove(index_to_cal)
                        diff -= 1
                elif diff < 0:
                    while diff != 0:
                        index_to_cal = random.choice([i for i in range(self.comp_num)])
                        excessive_cpu[index_to_cal] += 1
                        diff += 1
                if self.disable_fc:
                    calibrated_sample = [
                        [self.default_concurrency, excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]],
                         self.default_queue_length] for i in range(self.comp_num)]
                elif self.brute_force:
                    q = [self.init_q(excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], self.sample_to_comp[i],
                                     f=sample[i][0]) for i in range(self.comp_num)]
                    calibrated_sample = [[sample[i][0], excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], q[i]]
                                         for i in range(self.comp_num)]
                else:
                    q = [self.init_q(excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], self.sample_to_comp[i])
                         for i in range(self.comp_num)]
                    c = [excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]] for i in range(self.comp_num)]
                    calibrated_sample = [[self.queue_models[self.sample_to_comp[i]].get_flow_ctrl_rec(
                        cpu_alloc=c[i] / self.cpu_granularity), c[i], q[i]] for i in range(self.comp_num)]
                return calibrated_sample
            else:  # with scheduler - scheduler cpu unchanged, cpu for sync comp calibrated
                scheduler_cpu = sample[-1][1]
                sum_cpu_sync_comp = 0
                cpu_lb = []
                for i in range(len(self.component_stats)):
                    comp = self.sample_to_comp[i]
                    sum_cpu_sync_comp += sample[i][1]
                    if scheduler_cpu not in self._qm_dict[comp]:
                        init_success = self._init_qm_dict(scheduler_cpu, comp)
                        if not init_success:
                            return []
                    cpu_lb_temp = float(self._qm_dict[comp][scheduler_cpu].get_cpu_lower_bound()) * self.cpu_granularity
                    if math.ceil(cpu_lb_temp) - cpu_lb_temp < 1e-3:
                        cpu_lb_temp = math.ceil(cpu_lb_temp) + 1
                    else:
                        cpu_lb_temp = math.ceil(cpu_lb_temp)
                    cpu_lb.append(max(self.cpu_requests[comp], cpu_lb_temp))
                cpu_left = self.cpu_total - scheduler_cpu - sum(cpu_lb)
                cpu_alloc = [math.floor(sample[i][1] * cpu_left / sum_cpu_sync_comp)
                             for i in range(len(self.component_stats))]
                cpu_left -= sum(cpu_alloc)
                if cpu_left < 0:
                    return []
                while cpu_left > 0:
                    index_to_cal = random.choice(list(range(len(self.component_stats))))
                    cpu_alloc[index_to_cal] += 1
                    cpu_left -= 1
                cpu = [cpu_alloc[i] + cpu_lb[i] for i in range(len(self.component_stats))]
                cpu.append(scheduler_cpu)
                if self.disable_fc:
                    calibrated_sample = [[self.default_concurrency, cpu[i], self.default_queue_length]
                                         for i in range(self.comp_num)]
                else:
                    if self.brute_force:
                        f = [random.choice(self._qm_dict[comp][scheduler_cpu].get_available_fc())
                             for comp in self.component_stats]
                    else:
                        f = [self._qm_dict[comp][scheduler_cpu].get_flow_ctrl_rec(
                            cpu_alloc=cpu[i] / self.cpu_granularity) for i, comp in enumerate(self.component_stats)]
                    q = []
                    for i in range(len(self.component_stats)):
                        q.append(self._qm_dict[self.sample_to_comp[i]][scheduler_cpu].get_ql_rec(
                            flow_ctrl=f[i], cpu_alloc=decimal.Decimal(cpu[i] / self.cpu_granularity)))
                    f.append(0)
                    q.append(0)
                    calibrated_sample = [[f[i], cpu[i], q[i]] for i in range(self.comp_num)]
                return calibrated_sample

        def iterate(pop) -> list:
            # fitness
            fitnesses = [self.fitness_function(individual) for individual in pop]
            # select
            individuals_indices = np.argsort(fitnesses)[::-1][:int(len(pop) * self.select_rate)]
            pop = [pop[i] for i in individuals_indices]
            # crossover (not applicable when there is only 1 component)
            if self.comp_num > 1:
                for _ in range(int((self.pop_size - len(pop)) / 2)):
                    if random.random() < self.cross_rate:
                        parent1 = random.choice(pop)
                        parent2 = random.choice(pop)
                        # sample contains comp_num [f*, c*, q*], if only sample[0] is crossover, encoded as 0...01
                        # there are [0...01, 1...10] 2^(comp_num) - 2 possibilities of crossover points
                        cross_point_code = random.randint(1, 2 ** self.comp_num - 2)
                        cross_points = utils.int_to_binary_list(cross_point_code, self.comp_num)
                        child1, child2 = [], []
                        for i, cross_point in enumerate(cross_points):
                            if cross_point == 0:
                                child1.append(parent1[i])
                                child2.append(parent2[i])
                            else:
                                child1.append(parent2[i])
                                child2.append(parent1[i])
                        child1 = crossover_calibration(child1)
                        child2 = crossover_calibration(child2)
                        if len(child1) > 0:
                            pop.append(child1)
                        if len(child2) > 0:
                            pop.append(child2)
            # append randomly generated samples
            while len(pop) < self.pop_size:
                sample = self.init_sample()
                pop.append(sample)
            # mutate - generate a new sample
            for i in range(len(pop)):
                if random.random() < self.mutate_rate:
                    pop[i] = self.init_sample()
            return pop

        # early stop
        def has_converged(fitness_history, threshold, generations):
            # check whether best fitness converges
            if len(fitness_history) < generations:
                return False
            return np.all(np.abs(np.diff(fitness_history[-generations:])) < threshold)

        fitness = []
        if self.debugging:
            start = time.time()
            with tqdm(total=self.max_gen, desc='running GA', unit='round') as pbar:
                for _ in range(self.max_gen):
                    population = iterate(population)
                    fitness.append(max([self.fitness_function(individual) for individual in population]))
                    pbar.update(1)
                    if has_converged(fitness, threshold=self.es_threshold, generations=self.es_max_gen):
                        break
            end = time.time()
            print(f'Time for iteration: {end - start} s')
        else:
            for _ in range(self.max_gen):
                population = iterate(population)
                fitness.append(max([self.fitness_function(individual) for individual in population]))
                if has_converged(fitness, threshold=self.es_threshold, generations=self.es_max_gen):
                    break
        # return best solution
        best_fitness = max([self.fitness_function(individual) for individual in population])
        best_solutions = [individual for individual in population if self.fitness_function(individual) == best_fitness]
        return ({self.sample_to_comp[i]: [best_solutions[0][i][0], best_solutions[0][i][1] / self.cpu_granularity,
                                          best_solutions[0][i][2]] for i in range(self.comp_num)}
                if best_solutions else None, best_fitness, fitness)
