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
