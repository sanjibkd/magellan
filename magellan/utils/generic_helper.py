# coding=utf-8
import logging
import os
import pickle


import cloud
import numpy as np
import pandas as pd


from magellan.utils import install_path
from magellan.io.parsers import read_csv

logger = logging.getLogger(__name__)


def get_install_path():
    path_list = install_path.split(os.sep)
    return os.sep.join(path_list[0:len(path_list)-1])


def remove_non_ascii(s):
    s = ''.join(i for i in s if ord(i) < 128)
    s = str(s)
    return str.strip(s)


# find list difference
def list_diff(a_list, b_list):
    b_set = set(b_list)
    return [a for a in a_list if a not in b_set]


def load_dataset(file_name, key=None):
    p = get_install_path()
    p = os.sep.join([p, 'datasets', file_name+'.csv'])
    if file_name is 'table_A' or 'table_B':
        if key is None:
            key = 'ID'
    table = read_csv(p, key=key)
    return table


# remove rows with NaN in a particular attribute
def rem_nan(table, attr):
    l = table.index.values[np.where(table[attr].notnull())[0]]
    return table.ix[l]

