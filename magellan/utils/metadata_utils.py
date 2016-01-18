import numpy as np
import pandas as pd
import logging
from magellan.core.catalog import get_all_properties
from magellan.utils.helperfunctions import is_key_attribute

logger = logging.getLogger(__name__)


def get_reqd_metdata_from_catalog(df, reqd_metadata):
    if isinstance(reqd_metadata, list) == False:
        reqd_metadata = [reqd_metadata]

    metadata = {}
    d = get_all_properties(df)
    for m in reqd_metadata:
        if m in d:
            metadata[m] = d[m]
    return metadata


def update_reqd_metadata_with_kwargs(metadata, kwargs_dict, reqd_metadata):
    if isinstance(reqd_metadata, list) == False:
        reqd_metadata = [reqd_metadata]

    for m in reqd_metadata:
        if m in kwargs_dict:
            metadata[m] = kwargs_dict[m]
    return metadata


def get_diff_with_reqd_metadata(metadata, reqd_metadata):
    k = metadata.keys()
    if isinstance(reqd_metadata, list) == False:
        reqd_metadata = [reqd_metadata]
    d = set(reqd_metadata).difference(k)
    return d


def is_all_reqd_metadata_present(metadata, reqd_metadata):
    d = get_diff_with_reqd_metadata(metadata, reqd_metadata)
    if len(d) == 0:
        return True
    else:
        return False


def check_fk_constraint(df, fk, df_base, key):
    t = df_base[df_base[key].isin(pd.unique(df[fk]))]
    return is_key_attribute(t, key)


def does_contain_rows(df):
    return len(df) > 0


def is_attr_unique(df, key):
    uniq_flag = len(np.unique(df[key])) == len(df)
    if not uniq_flag:
        return False
    else:
        return True


def does_contain_missing_vals(df, key):
    nan_flag = sum(df[key].isnull()) == 0
    if not nan_flag:
        return False
    else:
        return True
