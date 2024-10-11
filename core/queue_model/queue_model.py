import numpy as np
import decimal
import math
import time
from scipy.optimize import root, minimize, OptimizeWarning
from tqdm import tqdm
import logging
from core.utils import utils, queue_model


class QueueModelwithTruncation:  # similar to M/M/c/K
    def __init__(self, concurrency: np.ndarray, cpu_utl: np.ndarray,
                 total_load: decimal.Decimal | float | int, delta_t: int, req_num: int,
                 precision: int = 200, interp_method: str = 'linear', enable_tqdm: bool = False):
        if concurrency.ndim != 1:
            raise ValueError(f'concurrency must be 1d np.ndarray, received {concurrency.ndim}d instead')
        if concurrency.shape != cpu_utl.shape:
            raise ValueError('the shape of concurrency and cpu_utl must be the same')
        uniq_con, uniq_idx = np.unique(concurrency, return_index=True)
        sort_idx = uniq_con.argsort()
        final_idx = uniq_idx[sort_idx]
        self._concurrency = concurrency[final_idx]
        self._cpu_utl = cpu_utl[final_idx]
        self._total_load = decimal.Decimal(total_load)  # CPU load, unit: 10^-3core·s
        self._delta_t = delta_t * 1000  # convert from s to ms
        self._req_num = req_num
        self._lambda = decimal.Decimal(self._req_num / self._delta_t)
        self._mu = decimal.Decimal(self._req_num / self._total_load)
        self.precision = precision
        self.enable_tqdm = enable_tqdm
        self.interp_method = interp_method

        def mapping_extend(x):
            fitting_func = queue_model.interpolate_fitting(self._concurrency, self._cpu_utl, kind=interp_method)
            x = np.array(x)
            mapping_values = np.zeros_like(x, dtype=float)
            # 对于数组中所有超过最大阈值的元素，应用fitting_func(max(self._concurrency))
            indices_over_max = x > max(self._concurrency)
            mapping_values[indices_over_max] = fitting_func(max(self._concurrency))
            # 对于数组中所有其他元素，应用fitting_func(element)
            indices_below_max = ~indices_over_max
            mapping_values[indices_below_max] = fitting_func(x[indices_below_max])
            return mapping_values

        self._utl_con_mapping = mapping_extend
        # cache map for faster computation
        self._cache_p0 = utils.nn_dict()  # cache[f][q][c]=value
        self._cache_pr = utils.nn_dict()
        self._cache_l = utils.nn_dict()
        self._cache_lq = utils.nn_dict()
        self._cache_q0 = utils.nn_dict()  # cache[f][P][c]=value
        # calculate bounds
        self._cpu_lower_bound = decimal.Decimal(self._total_load / self._delta_t)

        def max_cpu_func(x):
            return -self._utl_con_mapping(x)

        bounds = [(np.min(self._concurrency), np.max(self._concurrency))]
        result = minimize(max_cpu_func, x0=bounds[0][0], bounds=bounds)
        max_x = result.x
        if type(max_x) is list:
            max_x = max_x[0]
        self._cpu_upper_bound = self._utl_con_mapping(max_x).max()

        if self.interp_method == 'linear':
            fc_limits = []
            for i in range(len(self._concurrency)):
                if i != len(self._concurrency) - 1:
                    x_1, y_1 = self._concurrency[i], self._cpu_utl[i]
                    x_2, y_2 = self._concurrency[i + 1], self._cpu_utl[i + 1]
                    if self._cpu_lower_bound < min(y_1, y_2) or self._cpu_lower_bound > max(y_1, y_2) or y_1 == y_2:
                        continue
                    else:
                        x = x_1 + (float(self._cpu_lower_bound) - y_1) / (y_2 - y_1) * (x_2 - x_1)
                        if x not in fc_limits:
                            fc_limits.append(x)
            if not fc_limits:
                self._fc_limits = [0]
            else:
                self._fc_limits = [fc_limit for fc_limit in fc_limits if bounds[0][0] <= fc_limit <= bounds[0][1]]
                # self._fc_limits.sort()
        else:
            # TODO: how to avoid errors like "ValueError: A value in x_new is below the interpolation
            #  range's minimum value."
            def diff_func(x):
                return self._utl_con_mapping(x) - float(self._cpu_lower_bound)

            try:
                result = root(diff_func, x0=np.linspace(bounds[0][0] + 1, bounds[0][1] - 1, num=10), method='krylov')
            except (OptimizeWarning, RuntimeError) as e:
                logging.error(f'An error occurred when calculating f* bounds: {e}')
                self._fc_limits = [0]
            else:
                self._fc_limits = [0]
                if result.success:
                    fc_limits = list(set(result.x))
                    self._fc_limits = [fc_limit for fc_limit in fc_limits if bounds[0][0] <= fc_limit <= bounds[0][1]]
                    self._fc_limits.sort()

    def __str__(self):
        return (f'Params:\n'
                f'  Concurrency list = {self._concurrency}\n'
                f'  CPU utilization = {self._cpu_utl}\n'
                f'  Delta t = {self._delta_t}ms\n'
                f'  Number of requests = {self._req_num}\n'
                f'  Total load = {self._total_load:.3f}ms·core\n'
                f'  Precision = {self.precision}\n'
                f'  Interpolation method = {self.interp_method}\n'
                f'Bounds:\n'
                f'  CPU lower bound = {self._cpu_lower_bound}\n'
                f'  CPU upper bound = {self._cpu_upper_bound}\n'
                f'  Flow control limits = {self._fc_limits}')

    def get_req_num(self) -> int:
        return self._req_num

    def get_total_load(self) -> decimal.Decimal:
        return self._total_load

    def get_lambda(self) -> decimal.Decimal:
        return self._lambda

    def get_cpu_lower_bound(self) -> decimal.Decimal:
        return self._cpu_lower_bound

    def get_cpu_upper_bound(self, start: int = -1, end: int = -1, num_x: int = 1000) -> float:
        if start < self._concurrency.min():
            start = self._concurrency.min()
        if end > self._concurrency.max() or end == -1:
            end = self._concurrency.max()
        if start == self._concurrency.min() and end == self._concurrency.max():
            return self._cpu_upper_bound
        if start == end:
            return self._utl_con_mapping(start).max()
        xs = np.linspace(start, end, max(2, num_x))
        ub = np.max(self._utl_con_mapping(xs))
        return ub

    def get_fc_upper_bound(self) -> int:
        return self._concurrency.max()

    def meet_steady_state_condition(self, cpu_alloc: decimal.Decimal | float | int, flow_ctrl: int,
                                    max_ql: int = 1000, accurate: bool = False) -> bool:
        if accurate:
            p_r = self.calc_p_reject(flow_ctrl, cpu_alloc, max_ql)
            rho = self._total_load / decimal.Decimal(self._delta_t * min(cpu_alloc, self._utl_con_mapping(flow_ctrl)))
            return 1 > rho * (1 - p_r)
        else:
            return min(cpu_alloc, self._utl_con_mapping(flow_ctrl)) > self._cpu_lower_bound

    def get_fc_limits(self) -> list:
        return self._fc_limits

    def get_available_fc(self) -> list:
        fc_limits_full = [0]
        fc_limits_full.extend(self._fc_limits)
        fc_limits_full.append(self.get_fc_upper_bound() + 1)
        available_fc = []
        for i in range(1, len(fc_limits_full), 2):
            left = math.ceil(fc_limits_full[i])
            if left == fc_limits_full[i]:
                left += 1
            right = math.ceil(fc_limits_full[i + 1])
            num = list(range(left, right))
            available_fc.extend(num)
        return available_fc

    def _get_next_cpu_alloc(self, cpu_alloc: decimal.Decimal | int) -> decimal.Decimal | int:
        if cpu_alloc - self._cpu_lower_bound < 0.5:
            return cpu_alloc + decimal.Decimal(0.1)
        elif cpu_alloc - self._cpu_lower_bound < 3:
            return cpu_alloc + decimal.Decimal(0.5)
        else:
            return math.ceil(cpu_alloc) + 1

    def _get_next_flow_ctrl(self, flow_ctrl: int) -> int:
        if flow_ctrl <= self._fc_limits[0]:
            if math.ceil(self._fc_limits[0]) == math.floor(self._fc_limits[0]):
                return int(self._fc_limits[0] + 1)
            else:
                return math.ceil(self._fc_limits[0])
        small_step_intervals = []
        for i, limit in enumerate(self._fc_limits):
            if i % 2 == 0:
                if math.ceil(limit) == math.floor(limit):
                    small_step_intervals.append((int(limit + 1), int(limit + 5)))
                else:
                    small_step_intervals.append((math.ceil(limit), math.ceil(limit) + 4))
            else:
                if math.ceil(limit) == math.floor(limit):
                    small_step_intervals.append((int(limit - 5), int(limit + -1)))
                else:
                    small_step_intervals.append((math.floor(limit) - 4, math.floor(limit)))
        small_step = False
        next_interval_index = -1
        for i, interval in enumerate(small_step_intervals):
            if interval[0] <= flow_ctrl < interval[1]:
                small_step = True
                break
            if interval[0] > flow_ctrl:
                next_interval_index = i
        if small_step or flow_ctrl < self._fc_limits[0]:
            return flow_ctrl + 1
        else:
            if next_interval_index != -1:
                return min(flow_ctrl + 10, small_step_intervals[next_interval_index][0])
            return flow_ctrl + 10

    def _a_n(self, n: int, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, temp_n: int = 0,
             temp_res: decimal.Decimal | int = 0) -> decimal.Decimal | int:
        if n <= 0:
            return -1
        if temp_n > 0:
            prod = temp_res
        else:
            prod = 1
        for i in range(temp_n, n):
            if i + 1 <= flow_ctrl:
                max_occ = self._utl_con_mapping(i + 1)
            else:
                max_occ = self._utl_con_mapping(flow_ctrl)
            max_occ = decimal.Decimal(max_occ.max())
            if cpu_alloc <= max_occ:
                prod *= self._lambda / (decimal.Decimal(cpu_alloc) * self._mu)
            else:
                prod *= self._lambda / (max_occ * self._mu)
        return prod

    def _sum_a_n(self, n: int, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, temp_n: int = 0,
                 temp_res: decimal.Decimal | int = 0) -> decimal.Decimal | int:
        if n <= 0:
            return -1
        if temp_n > 0:
            sum = temp_res
        else:
            sum = 0
        t_n = 0
        t_res = 0
        if self.enable_tqdm:
            with tqdm(total=n - temp_n, desc=f'Computing ∑a_n (n={n}, start from {temp_n})', unit='addition') as pbar:
                for i in range(temp_n, n):
                    an = self._a_n(i + 1, flow_ctrl, cpu_alloc, temp_n=t_n, temp_res=t_res)
                    sum += an
                    t_n = i + 1
                    t_res = an
                    pbar.update(1)
        else:
            for i in range(temp_n, n):
                an = self._a_n(i + 1, flow_ctrl, cpu_alloc, temp_n=t_n, temp_res=t_res)
                sum += an
                t_n = i + 1
                t_res = an
        return sum

    def calc_p_0(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, max_ql: int = 1000) \
            -> decimal.Decimal:
        if cpu_alloc in self._cache_p0[flow_ctrl][max_ql].keys():
            return self._cache_p0[flow_ctrl][max_ql][cpu_alloc]
        sum = self._sum_a_n(n=flow_ctrl + max_ql, flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc)
        p_0 = decimal.Decimal(1 / (1 + sum))
        self._cache_p0[flow_ctrl][max_ql][cpu_alloc] = p_0
        return p_0

    def _p_n(self, n: int, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, max_ql: int = 1000,
             temp_n: int = 0, temp_res: decimal.Decimal | int = 0) -> decimal.Decimal | int:
        if n < 0:  # invalid n
            return -1
        p = self.calc_p_0(flow_ctrl, cpu_alloc, max_ql)
        if temp_n > 0:
            p = temp_res
        for i in range(temp_n, n):
            p *= self._lambda / self._mu
            if i + 1 <= flow_ctrl:
                max_utl = self._utl_con_mapping(i + 1)
            else:
                max_utl = self._utl_con_mapping(flow_ctrl)
            max_utl = decimal.Decimal(max_utl.max())
            if cpu_alloc <= max_utl:
                p /= decimal.Decimal(cpu_alloc)
            else:
                p /= max_utl
        return p

    def calc_p_reject(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int,
                      max_ql: int = 1000) -> decimal.Decimal | int:
        if cpu_alloc in self._cache_pr[flow_ctrl][max_ql].keys():
            return self._cache_pr[flow_ctrl][max_ql][cpu_alloc]
        p_r = self._p_n(n=flow_ctrl + max_ql, flow_ctrl=flow_ctrl, max_ql=max_ql, cpu_alloc=cpu_alloc)
        self._cache_pr[flow_ctrl][max_ql][cpu_alloc] = p_r
        return p_r

    def _sum_n_p_n(self, n: int, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int,
                   max_ql: int = 1000, temp_n: int = 0, temp_res: decimal.Decimal | int = 0) -> decimal.Decimal | int:
        if n < 0:
            return -1
        if temp_n > 0:
            sum = temp_res
        else:
            sum = 0
        t_n = 0
        t_res = 0
        if self.enable_tqdm:
            with tqdm(total=n - temp_n, desc=f'Computing ∑n*p_n (n={n}, start from {temp_n})', unit='addition') as pbar:
                for i in range(temp_n, n):
                    pn = self._p_n(i + 1, flow_ctrl, cpu_alloc, max_ql=max_ql, temp_n=t_n, temp_res=t_res)
                    sum += (i + 1) * pn
                    t_n = i + 1
                    t_res = pn
                    pbar.update(1)
        else:
            for i in range(temp_n, n):
                pn = self._p_n(i + 1, flow_ctrl, cpu_alloc, max_ql=max_ql, temp_n=t_n, temp_res=t_res)
                sum += (i + 1) * pn
                t_n = i + 1
                t_res = pn
        return sum

    def calc_l(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, max_ql: int = 1000) \
            -> decimal.Decimal | int:
        if cpu_alloc in self._cache_l[flow_ctrl][max_ql].keys():
            return self._cache_l[flow_ctrl][max_ql][cpu_alloc]
        sl = self._sum_n_p_n(n=flow_ctrl + max_ql, flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc, max_ql=max_ql)
        self._cache_l[flow_ctrl][max_ql][cpu_alloc] = sl
        return sl

    def calc_load_left(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int) -> decimal.Decimal | int:
        if self.meet_steady_state_condition(flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc):
            return 0
        return self._total_load - decimal.Decimal(self._delta_t * min(cpu_alloc, self._utl_con_mapping(flow_ctrl)))

    def _sum_n_minus_fc_p_n(self, n: int, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, max_ql: int = 1000,
                            temp_n: int = 0, temp_res: decimal.Decimal | int = 0) -> decimal.Decimal | int:
        if n < flow_ctrl:
            return -1
        if temp_n > 0:
            sum = temp_res
        else:
            sum = 0
        t_n = 0
        t_res = 0
        if self.enable_tqdm:
            with tqdm(total=n - temp_n, desc=f'Computing ∑(n-f*)*p_n (n={n}, start from {temp_n})',
                      unit='addition') as pbar:
                for i in range(temp_n, n):
                    if i + 1 <= flow_ctrl:
                        continue
                    pn = self._p_n(i + 1, flow_ctrl, cpu_alloc, max_ql=max_ql, temp_n=t_n, temp_res=t_res)
                    sum += (i + 1 - flow_ctrl) * pn
                    t_n = i + 1
                    t_res = pn
                    pbar.update(1)
        else:
            for i in range(temp_n, n):
                if i + 1 <= flow_ctrl:
                    continue
                pn = self._p_n(i + 1, flow_ctrl, cpu_alloc, max_ql=max_ql, temp_n=t_n, temp_res=t_res)
                sum += (i + 1 - flow_ctrl) * pn
                t_n = i + 1
                t_res = pn
        return sum

    def calc_l_q(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int, max_ql: int = 1000) \
            -> decimal.Decimal | int:
        if max_ql == 0:
            return 0
        if cpu_alloc in self._cache_lq[flow_ctrl][max_ql].keys():
            return self._cache_lq[flow_ctrl][max_ql][cpu_alloc]
        l_q = self._sum_n_minus_fc_p_n(n=flow_ctrl + max_ql, flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc,
                                       max_ql=max_ql)
        self._cache_lq[flow_ctrl][max_ql][cpu_alloc] = l_q
        return l_q

    def get_flow_ctrl_rec(self, cpu_alloc: decimal.Decimal | float | int = 0) -> int:  # f* = min(argmax_x(f(x)))
        f_min = self._concurrency.min()
        f_max = self._concurrency.max()
        x = np.linspace(f_min, f_max, f_max - f_min + 1)
        y = self._utl_con_mapping(x)
        if float(cpu_alloc) > np.min(self._cpu_utl):
            y[y > cpu_alloc] = cpu_alloc
        return queue_model.get_concurrency_rec(x, y)

    def get_ql_rec(self, flow_ctrl: int, cpu_alloc: decimal.Decimal | float | int,
                   prob: float = utils.REJECT_PROB) -> int:  # q* = argmin_q*(p_reject(f*,c*,q*)<=P)
        if cpu_alloc in self._cache_q0[flow_ctrl][prob].keys():
            return self._cache_q0[flow_ctrl][prob][cpu_alloc]
        rho = self._total_load / decimal.Decimal(self._delta_t * min(cpu_alloc, self._utl_con_mapping(flow_ctrl)))
        if rho >= 1:
            ql_rec = 0
        else:
            af = self._a_n(n=flow_ctrl, flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc)
            sum_af = self._sum_a_n(n=flow_ctrl, flow_ctrl=flow_ctrl, cpu_alloc=cpu_alloc)
            if af / (1 + sum_af) <= decimal.Decimal(prob):
                ql_rec = 0
            else:
                q = decimal.Decimal(prob) * ((1 - rho) * (1 + sum_af) + rho * af) / (
                        af * (1 + decimal.Decimal(prob) * rho - rho))
                ql_rec = math.floor(math.log(q, rho))
        self._cache_q0[flow_ctrl][prob][cpu_alloc] = max(1, ql_rec)
        return max(1, ql_rec)

    def run(self, max_ql=1000, save_to_file: str | None = None, dry_run: bool = False) -> None:

        def write_to_file(msg='', append=True):
            if save_to_file is None:
                print(msg)
            else:
                if append:
                    with open(save_to_file, 'a') as file:
                        file.write(msg + '\n')
                else:
                    with open(save_to_file, 'w') as file:
                        file.write(msg + '\n')

        if max_ql < 0:
            max_ql = 0
        # write_to_file(f'q* = {max_ql}', append=False)
        start = time.time()
        write_to_file(str(self))
        point_num = 0
        total_n = {'p_0': 0, 'L': 0, 'L_q': 0}
        for ql in np.linspace(0, max_ql, 21):
            if type(ql) is not int:
                ql = int(ql)
            cpu_alloc = self.get_cpu_lower_bound() + decimal.Decimal(0.1)
            if math.floor(self._fc_limits[0]) == math.ceil(self._fc_limits[0]):
                flow_control_lb = self._fc_limits[0] + 1
            else:
                flow_control_lb = math.ceil(self._fc_limits[0])
            while cpu_alloc <= math.ceil(self._cpu_upper_bound):
                flow_control = flow_control_lb
                while flow_control <= self.get_fc_upper_bound():
                    if type(cpu_alloc) is decimal.Decimal:
                        cpu_alloc_display = cpu_alloc.quantize(decimal.Decimal('0.000'))
                    else:
                        cpu_alloc_display = decimal.Decimal(cpu_alloc).quantize(decimal.Decimal('0.000'))
                    write_to_file(f'----------\nq* = {ql}\nc* = {cpu_alloc_display}\nf* = {flow_control}')
                    if not self.meet_steady_state_condition(cpu_alloc, flow_control):
                        write_to_file('overload')
                        flow_control = self._get_next_flow_ctrl(flow_control)
                        continue
                    if dry_run:
                        point_num += 1
                    else:
                        p_0 = self.calc_p_0(flow_ctrl=flow_control, cpu_alloc=cpu_alloc, max_ql=ql)
                        write_to_file(f'p_0(n={flow_control + ql}) ≈ {p_0}')
                        p_reject = self.calc_p_reject(flow_ctrl=flow_control, cpu_alloc=cpu_alloc, max_ql=ql)
                        write_to_file(f'p_reject(n={flow_control + ql}) ≈ {p_reject}')
                        l = self.calc_l(flow_ctrl=flow_control, cpu_alloc=cpu_alloc, max_ql=ql)
                        write_to_file(f'L(n={flow_control + ql}) ≈ {l}\n'
                                      f'W(n={flow_control + ql}) ≈ {l / (self._lambda * (1 - p_reject))}')
                        l_q = self.calc_l_q(flow_ctrl=flow_control, cpu_alloc=cpu_alloc, max_ql=ql)
                        write_to_file(f'L_q(n={flow_control + ql}) ≈ {l_q}\n'
                                      f'W_q(n={flow_control + ql}) ≈ {l_q / (self._lambda * (1 - p_reject))}')
                        total_n['p_0'] += flow_control + ql
                        total_n['L'] += flow_control + ql
                        total_n['L_q'] += ql
                        point_num += 1
                    flow_control = self._get_next_flow_ctrl(flow_control)
                cpu_alloc = self._get_next_cpu_alloc(cpu_alloc)
        end = time.time()
        execution_time = end - start
        write_to_file(f'----------------------------------------\n'
                      f'Summary:\n'
                      f'  Execution time: {execution_time}s\n'
                      f'  Data point number: {point_num}\n'
                      f'  Total iterations of:\n'
                      f'    p_0: {total_n["p_0"]}\n'
                      f'    L: {total_n["L"]}\n'
                      f'    L_q: {total_n["L_q"]}')
