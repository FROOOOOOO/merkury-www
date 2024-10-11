import math
import pandas as pd
from statsmodels.tsa.api import VARMAX, ARIMA
from pmdarima.arima import auto_arima
from statsmodels.tsa.stattools import adfuller

# constants
AVAILABLE_MODELS = ['latest', 'average', 'power_decay', 'exponential_decay', 'ARIMA', 'VARMA']


def exp_decay(time_passed: int = 0, factor: float = 1.0) -> float:
    """exponential decay function: f(t)=exp(-factor*t)

    Args:
        time_passed (int, optional): Time passed (unit: s). Defaults to 0.
        factor (float, optional): Hyperparameter of f(t). Defaults to 1.0.

    Returns:
        float: Decay factor, <1.
    """
    return math.exp(-factor * time_passed)


def power_decay(time_passed: int = 0, factor: float = 1.0, power: int = 1) -> float:
    """power decay function: f(t) = (1+factor*t)^(-power)

    Args:
        time_passed (int, optional): Time passed (unit: s). Defaults to 0.
        factor (float, optional): Hyperparameter of f(t). Defaults to 1.0.
        power (int, optional): Hyperparameter of f(t). Defaults to 1.

    Returns:
        float: Decay factor, <1.
    """
    return math.pow(1 + factor * time_passed, -power)


class Predictor:
    """simple predictor applying basic time series forcasting models to predict upcoming data (CPU, memory, etc.)"""

    def __init__(self, data: dict[str, list[int | float]], start_timestamps: list[int],
                 time_windows: list[int], model: str = 'latest'):
        if model not in AVAILABLE_MODELS:
            raise ValueError(f'model must be one of {AVAILABLE_MODELS}, received {model} instead')
        self.model = model
        if not isinstance(data, dict):
            raise ValueError(f'data must be a dict, received {data} ({type(data)}) instead')
        if not isinstance(start_timestamps, list):
            raise ValueError(f'start_timestamps must be a list, received {start_timestamps} '
                             f'({type(start_timestamps)}) instead')
        if not isinstance(time_windows, list):
            raise ValueError(f'time_windows must be a list, received {time_windows} '
                             f'({type(time_windows)}) instead')
        for data_column in data.values():
            if not 0 < len(data_column) == len(start_timestamps) == len(time_windows):
                raise ValueError(f'data, start timestamps and time windows must be non-empty and aligned, '
                                 f'#data={len(data_column)}, #timestamps={len(start_timestamps)}, '
                                 f'#timewindows={len(time_windows)}')
        # make the timestamp interval between two data = time window
        for i in range(len(start_timestamps)):
            if i != len(start_timestamps) - 1:
                interval = start_timestamps[i + 1] - start_timestamps[i]
                if interval <= 0:
                    raise ValueError('interval between start timestamps must > 0')
                for k in data.keys():
                    data[k][i] *= interval / time_windows[i]
                time_windows[i] = interval
        self.data = data
        self.start_timestamps = start_timestamps  # unit: s
        self.time_windows = time_windows  # unit: s

    def predict(self) -> (dict[str, float | int] | None, int | None):
        """predict upcoming data, return (data, time window(unit: s))"""
        if self.model == 'latest':
            return {k: self.data[k][-1] for k in self.data.keys()}, self.time_windows[-1]
        if self.model == 'average':
            return self._predict_average()
        if self.model == 'power_decay':
            return self._predict_decay(decay_func=power_decay)
        if self.model == 'exponential_decay':
            return self._predict_decay(decay_func=exp_decay)
        if self.model == 'ARIMA':
            if len(self.time_windows) >= 10:
                return self._predict_ARIMA()
            else:
                return self._predict_decay(decay_func=power_decay)
        if self.model == 'VARMA':
            if len(self.time_windows) >= 10:
                return self._predict_VARMA()
            else:
                return self._predict_decay(decay_func=power_decay)
        return None, None

    def _predict_average(self) -> (dict[str, float | int], int):
        total_time_window = sum(self.time_windows)
        prediction = {}
        for k in self.data.keys():
            data_total = sum([self.data[k][i] * self.time_windows[i] for i in range(len(self.time_windows))])
            prediction[k] = data_total / total_time_window
        return prediction, int(total_time_window / len(self.time_windows))

    def _predict_decay(self, decay_func: callable) -> (float | int, int):
        decay_weights = [decay_func(time_passed=self.start_timestamps[-1] - self.time_windows[i])
                         for i in range(len(self.time_windows))]
        decay_windows = [self.time_windows[i] * decay_weights[i] for i in range(len(decay_weights))]
        prediction = {}
        for k in self.data.keys():
            data_total = sum([self.data[k][i] * decay_windows[i] for i in range(len(decay_weights))])
            prediction[k] = data_total / sum(decay_windows)
        return prediction, int(sum(decay_windows) / sum(decay_weights))

    def _predict_ARIMA(self) -> (dict[float | int], int):
        prediction = {}
        # time window
        time_window = int((self.start_timestamps[-1] - self.start_timestamps[0]) / len(self.start_timestamps))
        for k in self.data.keys():
            # preprocess data to equal the timestamp interval
            ts_dt = pd.to_datetime(self.start_timestamps, unit='s')
            df = pd.DataFrame(data=self.data[k], index=ts_dt, columns=['Data'])
            resample_interval = time_window
            df_resampled = df.resample(f'{resample_interval}S').interpolate()
            data_series = df_resampled['Data']
            print(data_series)  # debug
            # differentiate until stable
            d = 0
            original_series = data_series.copy()
            while True:
                result = adfuller(data_series)
                if result[1] > 0.05:
                    data_series = data_series.diff().dropna()
                    d += 1
                else:
                    break
            # determine p and q using auto_arima
            pre_model = auto_arima(data_series, d=0, seasonal=False, trace=False)
            p, _, q = pre_model.order
            # train (p, 0, q) ARIMA model and predict
            model = ARIMA(data_series, order=(p, 0, q))
            model_fit = model.fit()
            forecast = model_fit.forecast(steps=1)
            # restore original data
            forecast_value = forecast[0]
            original_forecast = forecast_value + original_series[-d:]
            prediction[k] = original_forecast
        return prediction, time_window

    def _predict_VARMA(self) -> (dict[float | int], int):
        prediction = {}
        time_window = int((self.start_timestamps[-1] - self.start_timestamps[0]) / len(self.start_timestamps))
        # differentiate until stable
        diff_dict, diff_data = {}, {}
        for k in self.data.keys():
            diff_dict[k] = 0
            diff_data[k] = pd.Series(self.data[k])
            while True:
                result = adfuller(diff_data[k])
                if result[1] > 0.05:
                    diff_data[k] = diff_data[k].diff().dropna()
                    diff_dict[k] += 1
                else:
                    break
        # order of difference should be the same
        d = max(diff_dict.values())
        for k in self.data.keys():
            for _ in range(diff_dict[k], d):
                diff_data[k] = diff_data[k].diff().dropna()
        # construct dataframe
        df = pd.DataFrame(diff_data)
        datetime_index = pd.to_datetime(self.start_timestamps, unit='s')
        df.set_index(datetime_index[d:], inplace=True)
        # VARMA model
        model = VARMAX(df, freq='S', order=(1, 0))  # order: hyper-parameter
        model_fit = model.fit(disp=False)
        forecast = model_fit.forecast(steps=1)
        # restore original data
        forecast_dict = forecast.to_dict('list')
        for k in self.data.keys():
            prediction[k] = forecast_dict[k][0] + self.data[k][-d:]
        return prediction, time_window
