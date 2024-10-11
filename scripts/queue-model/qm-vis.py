import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))  # root dir of repo
sys.path.insert(0, python_path)
from core.utils import queue_model

if __name__ == '__main__':
    plt.rcParams['font.family'] = 'Arial'
    sns.set_theme(font='Arial', style='ticks')
    parser = argparse.ArgumentParser(description='Script to visualise QM data.')
    parser.add_argument('--file_3d', '-t', action='store_true',
                        help='if set, file type is interpreted as 3d (c, f, and q vary)')
    parser.add_argument('--file_name', '-f', type=str,
                        default=f'{python_path}/output/queue-model-truncation/increase-no-change/1000-1000-1000.txt',
                        help='data file to visualize')
    parser.add_argument('--plot_type', '-p', type=str, default='3d',
                        help='plot type: 3d or 2d. DEFAULT: 3d')
    parser.add_argument('--z_data', '-z', type=str, default='w',
                        help='z(y) data for 3d(2d) plot: w, l, w_q, l_q, w_e, l_e or p_r. DEFAULT: w')
    parser.add_argument('--x_var', '-x', type=str, default='c',
                        help='x data for 3d(2d) plot: c, f or q. DEFAULT: c')
    parser.add_argument('--y_var', '-y', type=str, default='f',
                        help='y data for 3d(2d) plot: c, f or q. DEFAULT: f')
    parser.add_argument('--fix_value', '-v', type=float, default=0.0,
                        help='fix value for 3d(2d) plot: if x and y are c and f, fix value is for q. DEFAULT: 0.0')
    parser.add_argument('--interp', '-i', action='store_true',
                        help='if set, 3d plot uses linear interpolation')
    parser.add_argument('--samples', '-s', type=int, default=1000,
                        help='interpolations samples for c* and f*. DEFAULT: 1000')
    parser.add_argument('--dpi', type=int, default=150, help='dpi for output png. DEFAULT: 150')
    args = parser.parse_args()
    if args.file_3d:
        queue_model.qm_data_vis_3d(args.file_name, plot_type=args.plot_type, x_var=args.x_var, y_var=args.y_var,
                                   fix_value=args.fix_value, z_metric=args.z_data, interp=args.interp,
                                   samples=args.samples, dpi=args.dpi)
    else:
        queue_model.qm_data_vis(args.file_name, plot_type=args.plot_type, z_data=args.z_data, interp=args.interp,
                                samples=args.samples, dpi=args.dpi)
    plt.show()
