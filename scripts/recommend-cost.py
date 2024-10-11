import argparse
import numpy as np


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate recommendation and update cost based on log file.')
    parser.add_argument('-f', '--file', required=True, type=str, help='log file path')
    args = parser.parse_args()
    pids = []
    cpu_avg, cpu_p99, mem_avg, mem_p99 = [], [], [], []
    recommend_duration = []
    update_duration = []
    with open(args.file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            str_list = line.split(' ')
            if len(str_list) > 4:
                if str_list[-1] == 's.' and str_list[-3] == 'using':
                    duration = float(str_list[-2])
                    if str_list[-4].endswith('),'):
                        recommend_duration.append(duration)
                    elif str_list[-4] == 'updating,':
                        update_duration.append(duration)
                if len(str_list) > 6:
                    if str_list[5] == 'monitor_cost():' and str_list[-1] == 'MB.':
                        pids.append(str_list[7][:-1])
                        cpu_avg.append(float(str_list[11][:-1]))
                        mem_avg.append(float(str_list[15]))
                        cpu_p99.append(float(str_list[-6][:-1]))
                        mem_p99.append(float(str_list[-2]))
    if recommend_duration:
        avg_rec_time = np.mean(recommend_duration)
        mid_rec_time = np.median(recommend_duration)
        p99_rec_time = np.percentile(recommend_duration, 99)
        print(f'Recommendation times: {len(recommend_duration)}')
        print(f'Average recommendation duration: {avg_rec_time:.2f} s')
        print(f'Median recommendation duration: {mid_rec_time:.2f} s')
        print(f'P99 recommendation duration: {p99_rec_time:.2f} s')
    else:
        print('No recommendations, cost computation canceled.')
    if update_duration:
        avg_update_time = np.mean(update_duration)
        mid_update_time = np.median(update_duration)
        p99_update_time = np.percentile(update_duration, 99)
        print(f'Update times: {len(update_duration)}')
        print(f'Average update duration: {avg_update_time:.2f} s')
        print(f'Median update duration: {mid_update_time:.2f} s')
        print(f'P99 update duration: {p99_update_time:.2f} s')
    else:
        print('No updates, cost computation canceled.')
    for i, pid in enumerate(pids):
        print(f'PID: {pid}; average CPU usage: {cpu_avg[i]}; P99 CPU usage: {cpu_p99[i]}; '
              f'average memory usage: {mem_avg[i]} MB; P99 memory usage: {mem_p99[i]} MB.')
    if len(pids) == 0:
        print('No data for resource cost.')
