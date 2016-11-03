#!/usr/bin/env python
# encoding: utf-8

"""
@author: Jackling Gu
@file: data_transform.py
@time: 16-10-12 16:31
"""
from scipy import signal
import numpy as np

round_f = lambda x:round(x,2)

class DataTransformer(object):
    def __init__(self, data):
        self.data = data
        assert type(self.data) == list

    def run(self, trans_type, params={}):
        self.params = params
        assert type(self.params) == dict
        res = None
        try:
            res = getattr(self, '_{}'.format(trans_type))()
        except AttributeError:
            raise Exception("unsupported function : {}".format(trans_type))

        return res

    def _median_filter(self):
        return map(round_f,list(signal.medfilt(self.data)))

    def _moving_average_filter(self):
        return round_f(np.mean(self.data))

    def _threshold_filter(self):
        min_v = self.params.get('min_value', None)
        max_v = self.params.get('max_value', None)
        tmp_data = np.array(self.data)
        if max_v != None:
            tmp_data = np.clip(tmp_data, a_min=min(tmp_data), a_max=int(max_v))
        if min_v != None:
            tmp_data = np.clip(tmp_data, a_min=int(min_v), a_max=max(tmp_data))
        return map(round_f,tmp_data)

    def _normalization(self):
        # normalize to [0,1]
        tmp_min = min(self.data)
        tmp_max = max(self.data)
        tmp_range = tmp_max - tmp_min
        tmp_data = [x - tmp_min for x in self.data]
        if tmp_range == 0:
            return map(round_f,tmp_data)  # all zero
        else:
            return map(round_f,[x * 1.0 / tmp_range for x in tmp_data])

    def _simple_features(self):
        # return [min,max,avg,25%,75%,median]
        tmp_data = sorted(self.data)
        data_len = len(tmp_data)
        idx_25 = data_len / 4
        idx_75 = idx_25 * 3
        return map(round_f,[min(tmp_data), max(tmp_data), np.mean(tmp_data), tmp_data[idx_25], tmp_data[idx_75],
                np.median(tmp_data)])
