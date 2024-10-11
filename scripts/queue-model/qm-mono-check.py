import argparse
import os
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.utils import queue_model


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Check monotonicity of queue models based on files generated through simulation.')
    parser.add_argument('--file', '-f', type=str, required=True, help='File to check')
    parser.add_argument('--ftype', '-t', type=str, required=True, help='File type, 2d or 3d')
    # parser.add_argument('--ind_var', '-v', type=str, default='',
    #                     help='Independent variable to check: f (f*), c (c*) or None (all, default)')
    parser.add_argument('--ignore', '-i', action='store_true', help='if set, ignore violations')
    args = parser.parse_args()
    mono, violations = queue_model.check_monotonicity(file=args.file, ftype=args.ftype)
    for metric in mono:
        for var in mono[metric]:
            value = mono[metric][var]
            if value not in [-1, 0, 1]:
                print(f'{metric} neither increases nor decreases monotonically with {var} (?)')
            elif value == 0:
                print(f'{metric} does not change with {var} (-)')
            elif value == 1:
                print(f'{metric} increases monotonically with {var} (↑)')
            else:
                print(f'{metric} decreases monotonically with {var} (↓)')
    if not args.ignore:
        for v in violations:
            print(v)
    # mono_c_w, mono_c_w_q, mono_c_w_e, mono_f_w, mono_f_w_q, mono_f_w_e, violations = (
    #     queue_model.check_monotonicity(args.file))
    # if args.ind_var == '' or args.ind_var == 'c':
    #     if mono_c_w:
    #         print('W decreases monotonically with c*')
    #     if mono_c_w_q:
    #         print('W_q decreases monotonically with c*')
    #     if mono_c_w_e:
    #         print('W_e decreases monotonically with c*')
    # if args.ind_var == '' or args.ind_var == 'f':
    #     if mono_f_w:
    #         print('W decreases monotonically with f*')
    #     if mono_f_w_q:
    #         print('W_q decreases monotonically with f*')
    #     if mono_f_w_e:
    #         print('W_e increases monotonically with f*')
    # if not args.ignore and len(violations) > 0:
    #     print('Violations:')
    #     for v in violations:
    #         print(v)
