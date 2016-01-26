import logging

import numpy as np
import pandas as pd

import magellan.utils.metadata_utils


logger = logging.getLogger(__name__)


class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.
    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.
    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.
    Limitations: The decorated class cannot be inherited from.
    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.
        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


@Singleton
class Catalog(object):

    def __init__(self):
        self.properties_catalog = {}

    def init_properties(self, df):
        df_id = id(df)
        self.properties_catalog[df_id] = {}
        return True

    def get_property(self, df, name):
        df_id = id(df)
        d = self.properties_catalog[df_id]
        return d[name]

    def set_property(self, df, name, value):
        df_id = id(df)
        d = self.properties_catalog[df_id]
        d[name] = value
        self.properties_catalog[df_id] = d
        return True

    def get_all_properties(self, df):
        df_id = id(df)
        d = self.properties_catalog[df_id]
        return d

    def del_property(self, df, name):
        df_id = id(df)
        d = self.properties_catalog[df_id]
        del d[name]
        self.properties_catalog[df_id] = d
        return True

    def del_all_properties(self, df):
        df_id = id(df)
        del self.properties_catalog[df_id]
        return True

    def get_catalog(self):
        return self.properties_catalog

    def del_catalog(self):
        self.properties_catalog = {}
        return True

    def get_catalog_len(self):
        return len(self.properties_catalog)

    def is_catalog_empty(self):
        return self.properties_catalog > 0

    def is_dfinfo_present(self, df):
        return id(df) in self.properties_catalog

    def is_metadata_present_for_df(self, df, name):
        df_id = id(df)
        d = self.properties_catalog[df_id]
        return name in d


def get_property(df, name):
    """
    Get property for a dataframe

    Parameters
    ----------
    df : pandas DataFrame
        Data frame object

    name : str
        Property name

    Returns
    -------
    result : object
        Property value for the given name

    """

    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    if catalog.is_metadata_present_for_df(df, name) == False:
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.get_property(df, name)


def set_property(df, name, value):
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        catalog.init_properties(df)

    catalog.set_property(df, name, value)


def get_all_properties(df):
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.get_all_properties(df)


def del_property(df, name):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    if catalog.is_metadata_present_for_df(df, name) == False:
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.del_property(df, name)


def del_all_properties(df):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.del_all_properties(df)


def get_catalog():
    catalog = Catalog.Instance()
    return catalog.get_catalog()


def del_catalog():
    catalog = Catalog.Instance()
    return catalog.del_catalog()


def is_catalog_empty():
    catalog = Catalog.Instance()
    return catalog.is_catalog_empty()


def is_dfinfo_present(df):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    return catalog.is_dfinfo_present(df)


def is_property_present_for_df(df, name):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) is False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.is_metadata_present_for_df(df, name)


def get_catalog_len():
    catalog = Catalog.Instance()
    return catalog.get_catalog_len()


def set_properties(df, prop_dict, replace=True):
    catalog = Catalog.Instance()
    if catalog.is_dfinfo_present(df) == True:
        if replace is False:
            logger.warning('Properties already exists for df (' +id(df) +' ). Not replacing it')
            return False

    catalog.del_all_properties(df)
    for k, v in prop_dict.iteritems():
        catalog.set_property(df, k, v)
    return True


def copy_properties(src, tar, replace=True):
    # copy catalog information from src to tar
    catalog = Catalog.Instance()
    metadata = catalog.get_all_properties(src)
    return set_properties(tar, metadata, replace)

# key related methods
def get_key(df):
    return get_property(df, 'key')


def set_key(df, key):
    if magellan.core.catalog.is_key_attribute(df, key) is False:
        logger.warning('Attribute ('+key+') does not qualify to be a key')
        return False
    else:
        return set_property(df, 'key', key)


def get_reqd_metadata_from_catalog(df, reqd_metadata):
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


