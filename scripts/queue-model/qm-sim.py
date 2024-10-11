import argparse
import numpy as np
import decimal
import os
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.queue_model import queue_model

if __name__ == '__main__':
    # discuss monotonicity of L with f* and/or c*
    con = np.array([0, 3, 6, 9, 15, 30, 60, 90, 150, 300, 600, 900])
    cpu = np.array([0 for _ in range(len(con))])
    parser = argparse.ArgumentParser(description='Simulate metrics for self-designed queue models.')
    parser.add_argument('--mapping', '-m', type=int, default=0,
                        help='choose mapping: 0-increase-no-change, 1-increasing, 2-varying')
    parser.add_argument('--dry_run', '-d', action='store_true',
                        help='if set, simulation will only list (c*, f*) points to calculate instead of '
                             'really calculating them')
    parser.add_argument('--tqdm', action='store_true', help='if set, display tqdm info')
    parser.add_argument('--req_num', '-r', type=int, default=1000,
                        help='number of requests within a time window')
    parser.add_argument('--delta_t', '-t', type=int, default=1, help='time window (s)')
    parser.add_argument('--total_load', '-l', type=float, default=10000.0, help='total load (core·ms)')
    parser.add_argument('--precision', '-p', type=int, default=200,
                        help='precision of decimal computation')
    # parser.add_argument('--error', '-e', type=float, default=1e-10, help='error tolerance')
    parser.add_argument('--interp_method', '-i', type=str, default='linear',
                        help='cpu-concurrency mapping interpolation method: linear, quadratic or cubic')
    parser.add_argument('--max_queue_length', '-q', type=int, default=1000,
                        help='max queue length (default: 1000)')
    args = parser.parse_args()
    decimal.getcontext().prec = args.precision
    total_load = decimal.Decimal(args.total_load)
    file_path = f'{python_path}/output/queue-model-truncation/'
    assert args.mapping in [0, 1, 2], 'invalid mapping'
    if args.mapping == 0:
        cpu = np.array([0.28, 3.12, 4.90, 6.57, 9.46, 14.66, 19.05, 21.29, 23, 23, 23, 23])
        file_path += 'increase-no-change/'
    if args.mapping == 1:
        cpu = np.array([0.28, 3.12, 4.90, 6.57, 9.46, 14.66, 19.05, 21.29, 22.86, 23.08, 23.12, 23.29])
        file_path += 'increasing/'
    if args.mapping == 2:
        cpu = np.array([0.28, 2.96, 4.74, 5.95, 8.63, 12.53, 15.16, 16.97, 20.70, 20.61, 17.31, 17.59])
        file_path += 'varying/'
    # queue_model = queue_model.QueueModel(con, cpu, total_load, args.delta_t, args.req_num, args.precision, args.error,
    #                                      args.interp_method, tqdm=args.tqdm)
    queue_model = queue_model.QueueModelwithTruncation(con, cpu, total_load, args.delta_t, args.req_num,
                                                       args.precision, args.interp_method, enable_tqdm=args.tqdm)
    file_name = f'{args.delta_t}-{args.req_num}-{int(total_load)}.txt'
    queue_model.run(max_ql=args.max_queue_length, save_to_file=file_path + file_name)
