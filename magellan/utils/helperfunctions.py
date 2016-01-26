import logging
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


def get_install_path():
    plist = install_path.split(os.sep)
    return os.sep.join(plist[0:len(plist)-1])


def add_key_column(table, key):
    table.insert(0, key, range(0, len(table)))
    return table


def get_name_for_key(columns):
    k = '_id'
    i = 0
    # try attribute name of the form "_id", "_id0", "_id1", ... and
    # return the first available name
    while True:
        if k not in columns:
            break
        else:
            k = '_id' + str(i)
        i += 1
    return k