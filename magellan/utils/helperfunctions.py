import logging
import numpy as np
import os
from magellan.utils import install_path

logger = logging.getLogger(__name__)


def are_all_attributes_present(df, col_names, verbose=False):
    df_columns_names = list(df.columns)
    for c in col_names:
        if c not in df_columns_names:
            if verbose:
                logger.warning('Column name (' +c+ ') is not present in dataframe')
            return False
    return True


def is_key_attribute(df, key, verbose=False):
    # check if the length is > 0
    if len(df) > 0:
        # check for uniqueness
        uniq_flag = len(np.unique(df[key])) == len(df)
        if not uniq_flag:
            if verbose:
                logger.warning('Attribute ' + key + ' does not contain unique values')
            return False

        # check if there are missing or null values
        nan_flag = sum(df[key].isnull()) == 0
        if not nan_flag:
            if verbose:
                logger.warning('Attribute ' + key + ' contains missing values')
            return False
        return uniq_flag and nan_flag
    else:
        return True


def get_install_path():
    plist = install_path.split(os.sep)
    return os.sep.join(plist[0:len(plist)-1])

