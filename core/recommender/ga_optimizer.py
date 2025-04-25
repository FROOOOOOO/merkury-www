import decimal
import math
import numpy as np
import random
from tqdm import tqdm
from core.utils import utils
from core.queue_model import queue_model
import time


class GAOptimizer:
    """
    Simple GA Optimizer to minimize the weighted average latency of components
    """
    def __init__(self, cpu_total: int | float, queue_models: dict, weights: dict,
                 cpu_granularity: int = utils.DEFAULT_CPU_GRANULARITY, cpu_request: dict = utils.DEFAULT_CPU_REQ,
                 brute_force: bool = False, disable_tc: bool = False, default_concurrency: int | None = None,
                 default_queue_length: int = utils.DEFAULT_QUEUE_LENGTH, pop_size: int = utils.GA_POP_SIZE,
                 max_gen: int = utils.GA_MAX_GEN, select_rate: float = utils.GA_SELECT_RATE,
                 cross_rate: float = utils.GA_CROSS_RATE, mutate_rate: float = utils.GA_MUTATE_RATE,
                 es_threshold: float = utils.GA_ES_THRESHOLD, es_max_gen: int = utils.GA_ES_MAX_GEN,
                 debugging: bool = False):
        if not isinstance(cpu_granularity, int) or cpu_granularity <= 0:
            raise ValueError(f'cpu_granularity must be positive integer, received {cpu_granularity} '
                             f'({type(cpu_granularity)}) instead')
        self.cpu_granularity = cpu_granularity  # 1/n core
        if type(cpu_total) not in [int, float] or cpu_total <= 0:
            raise ValueError(f'cpu_total must be positive int or float, received {cpu_total} '
                             f'({type(cpu_total)}) instead')
        self.cpu_total = math.floor(cpu_total * self.cpu_granularity)
        self.debugging = debugging  # for test only
        self.brute_force = brute_force
        self.disable_tc = disable_tc
        # queue models
        self.queue_models = {}
        self.cpu_lb = {}
        self.f_rec = {}
        self.weights = {}
        if not isinstance(queue_models, dict):
            raise ValueError(f'queue_models must be dict, received {type(queue_models)} instead')
        for comp in queue_models:
            if comp in utils.CONTROL_PLANE_COMPONENTS:
                if not isinstance(queue_models[comp], queue_model.QueueModelCustom):
                    raise ValueError(f'values of queue_models must be QueueModelwithTruncation, '
                                     f'received {type(queue_models[comp])} instead')
                if comp not in cpu_request:
                    raise ValueError('keys of queue_models must be in cpu_request')
                if type(cpu_request[comp]) not in [int, float]:
                    raise ValueError(f'values of cpu_request must be int or float, received '
                                     f'{type(cpu_request[comp])} instead')
                self.queue_models[comp] = queue_models[comp]
                cpu_lb_temp = float(self.queue_models[comp].get_cpu_lower_bound())
                qmlb_as_lb = cpu_lb_temp >= cpu_request[comp]
                cpu_lb = math.ceil(max(cpu_lb_temp, cpu_request[comp]) * self.cpu_granularity)
                if qmlb_as_lb and cpu_lb - (cpu_lb_temp * cpu_granularity) < 1e-3:
                    cpu_lb += 1  # ensure cpu_lb is approachable
                self.cpu_lb[comp] = cpu_lb
                if self.brute_force:
                    tc_limits = self.queue_models[comp].get_tc_limits()
                    tc_limits_full = [0]
                    tc_limits_full.extend(tc_limits)
                    tc_limits_full.append(self.queue_models[comp].get_tc_upper_bound() + 1)
                    available_tc = []
                    for i in range(1, len(tc_limits_full), 2):
                        left = math.ceil(tc_limits_full[i])
                        if left == tc_limits_full[i]:
                            left += 1
                        right = math.ceil(tc_limits_full[i + 1])
                        num = list(range(left, right))
                        available_tc.extend(num)
                    self.f_rec[comp] = self.queue_models[comp].get_available_tc()
                else:
                    self.f_rec[comp] = self.queue_models[comp].get_flow_ctrl_rec()
        self.comp_num = len(self.queue_models)
        if self.comp_num <= 0:
            raise ValueError('no component to optimize')
        self.sample_to_comp = [comp for comp in self.queue_models]
        cpu_left = self.cpu_total - sum(self.cpu_lb.values())
        self.cpu_left = cpu_left
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
            if comp in self.queue_models:
                self.weights[comp] = weights[comp]
        self.default_concurrency = default_concurrency
        self.default_queue_length = default_queue_length
        if disable_tc and default_concurrency is None:
            raise ValueError('missing default_concurrency when disable_tc')
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

    def init_c(self) -> list:  # generate [c*] randomly, satisfying ∑c*=C and c*[i]>cpu_lb[i]
        if self.cpu_left < 0:
            return []
        if self.cpu_left == 0:
            return [self.cpu_lb[comp] for comp in self.cpu_lb]
        # generate a sorted list containing 0, rand * (comp_num-1), left
        points = sorted([0] + random.sample(range(1, self.cpu_left), self.comp_num - 1) + [self.cpu_left])
        # calculate diff between 2 neighboring numbers in the list (generated random number)
        numbers = [points[i + 1] - points[i] for i in range(self.comp_num)]
        # add cpu_lb[i] to corresponding random number
        cpu = [numbers[i] + self.cpu_lb[self.sample_to_comp[i]] for i in range(self.comp_num)]
        return cpu

    def init_q(self, c, comp, f: int | None = None) -> int:  # generate q* for single c*
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
        if self.disable_tc:
            sample = [[self.default_concurrency, cpu[i], self.default_queue_length] for i in range(self.comp_num)]
        elif self.brute_force:
            f = self.init_f()
            q = [self.init_q(cpu[i], self.sample_to_comp[i], f[i]) for i in range(self.comp_num)]
            sample = [[f[i], cpu[i], q[i]] for i in range(self.comp_num)]
        else:
            q = [self.init_q(cpu[i], self.sample_to_comp[i]) for i in range(self.comp_num)]
            sample = [[self.f_rec[self.sample_to_comp[i]], cpu[i], q[i]] for i in range(self.comp_num)]
        return sample

    def obj_func(self, sample: list) -> decimal.Decimal | float | int:
        if not isinstance(sample, list) or len(sample) == 0:
            raise ValueError(f'sample must be non-empty list, received {sample} instead')
        for s in sample:
            if len(s) != 3:
                raise ValueError(f'elements of sample must be list with 3 elements, received {s} instead')
        # ∑w_iW_i
        weighted_w = 0
        for i in range(len(sample)):
            f, c, q = sample[i][0], decimal.Decimal(sample[i][1] / self.cpu_granularity), sample[i][2]
            p_r = self.queue_models[self.sample_to_comp[i]].calc_p_reject(flow_ctrl=f, cpu_alloc=c, max_ql=q)
            sl = self.queue_models[self.sample_to_comp[i]].calc_l(flow_ctrl=f, cpu_alloc=c, max_ql=q)
            w = float(sl / (self.queue_models[self.sample_to_comp[i]].get_lambda() * (1 - p_r)))
            # req_num = self.queue_models[self.sample_to_comp[i]].get_req_num()
            # weighted_req_num = req_num * self.weights[self.sample_to_comp[i]]
            # total_req_num += decimal.Decimal(weighted_req_num)
            weighted_w += self.weights[self.sample_to_comp[i]] * w
        return weighted_w

    def fitness_function(self, sample):
        return -self.obj_func(sample)

    def run(self) -> (dict | None, decimal.Decimal | float | int | None, list | None):  # type: ignore
        if self.cpu_left < 0:
            return None, None, None
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
            if self.disable_tc:
                calibrated_sample = [[self.default_concurrency, excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]],
                                      self.default_queue_length] for i in range(self.comp_num)]
            elif self.brute_force:
                q = [self.init_q(excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], self.sample_to_comp[i],
                                 f=sample[i][0]) for i in range(self.comp_num)]
                calibrated_sample = [[sample[i][0], excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], q[i]]
                                     for i in range(self.comp_num)]
            else:
                q = [self.init_q(excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]], self.sample_to_comp[i])
                     for i in range(self.comp_num)]
                calibrated_sample = [[self.f_rec[self.sample_to_comp[i]],
                                      excessive_cpu[i] + self.cpu_lb[self.sample_to_comp[i]],
                                      q[i]] for i in range(self.comp_num)]
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
                        pop.append(child1)
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
            # check whether the best fitness converges
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
