import logging

import numpy as np
import pandas as pd

# import magellan.utils.metadata


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
    """
    Class to store and retrieve catalog information
    """

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

    Args:
        df (pandas dataframe): Dataframe for which the property should be retrieved
        name (str): Name of the property that should be retrieved

    Returns:
        Property value (pandas object) for the given property name

    Raises:
        AttributeError: If the input dataframe in null
        KeyError: If the dataframe is not present in the catalog, or the requested property is not
            present in the catalog

    """

    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        raise KeyError('Dataframe information is not present in the catalog')

    if not catalog.is_metadata_present_for_df(df, name):
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.get_property(df, name)


def set_property(df, name, value):
    """
    Set property for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the property has to be set
        name (str): Property name
        value (pandas object): Property value

    Returns:
        status (bool). Returns True if the property was set successfully

    Raises:
        AttributeError: If the input dataframe is null

    """
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        catalog.init_properties(df)

    catalog.set_property(df, name, value)


def get_all_properties(df):
    """
    Get all the properties for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the properties must be retrieved

    Returns:
        Property dictionary (dict). The keys are property names (str) and the values are property values (pandas object)

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the information about the input dataframe is not present in the catalog

    """
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.get_all_properties(df)


def del_property(df, name):
    """
    Delete a property from the catalog

    Args:
        df (pandas dataframe): Input dataframe for which a property must be deleted
        name (str): Property name

    Returns:
        status (bool). Returns True if the deletion was successful

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the Dataframe info. is not present or the given property is not present for that dataframe in the
            catalog
    """
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        raise KeyError('Dataframe information is not present in the catalog')

    if not catalog.is_metadata_present_for_df(df, name):
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.del_property(df, name)


def del_all_properties(df):
    """
    Delete all properties for a dataframe

    Args:
        df (pandas dataframe): Input dataframe for which all the properties must be deleted.

    Returns:
        status (bool). Returns True if the deletion was successful

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the dataframe information is not present in the catalog
    """
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.del_all_properties(df)


def get_catalog():
    """
    Get Catalog information.


    Returns:
        Catalog information in a dictionary format.

    """
    catalog = Catalog.Instance()
    return catalog.get_catalog()


def del_catalog():
    """
    Delete catalog information

    Returns:
        status (bool). Returns True if the deletion was successful.
    """
    catalog = Catalog.Instance()
    return catalog.del_catalog()


def is_catalog_empty():
    """
    Check if the catalog is empty

    Returns:
        result (bool). Returns True if the catalog is empty, else returns False.

    """
    catalog = Catalog.Instance()
    return catalog.is_catalog_empty()


def is_dfinfo_present(df):
    """
    Check if the dataframe information is present in the catalog

    Args:
        df (pandas dataframe): Input dataframe

    Returns:
        result (bool). Returns True if the dataframe information is present in the catalog, else returns False

    Raises:
        AttributeError: If the input dataframe is null

    """
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    return catalog.is_dfinfo_present(df)


def is_property_present_for_df(df, name):
    """
    Check if the property is present for the dataframe

    Args:
        df (pandas dataframe): Input dataframe
        name (str): Property name

    Returns:
        result (bool). Returns True if the property is present for the input dataframe

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the dataframe is not present in the catalog

    """
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) is False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.is_metadata_present_for_df(df, name)


def get_catalog_len():
    """
    Get the number of entries in the catalog

    Returns:
        length (int) of the catalog

    """
    catalog = Catalog.Instance()
    return catalog.get_catalog_len()


def set_properties(df, prop_dict, replace=True):
    """
    Set properties for a dataframe in the catalog
    Args:
        df (pandas dataframe): Input dataframe
        prop_dict (dict): Property dictionary with keys as property names and values as python objects
        replace (bool): Flag to indicate whether the input properties can replace the properties in the catalog

    Returns:
        status (bool). Returns True if the setting of properties was successful

    Note:
        The function is intended to set all the properties in the catalog with the given property dictionary.
          The replace flag is just a check where the properties will be not be disturbed if they exist already in the
          catalog

    """
    catalog = Catalog.Instance()
    if catalog.is_dfinfo_present(df):
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
    if is_key_attribute(df, key) is False:
        logger.warning('Attribute ('+key+') does not qualify to be a key')
        return False
    else:
        return set_property(df, 'key', key)


def get_reqd_metadata_from_catalog(df, reqd_metadata):
    if not isinstance(reqd_metadata, list):
        reqd_metadata = [reqd_metadata]

    metadata = {}
    d = get_all_properties(df)
    for m in reqd_metadata:
        if m in d:
            metadata[m] = d[m]
    return metadata


def update_reqd_metadata_with_kwargs(metadata, kwargs_dict, reqd_metadata):
    if not isinstance(reqd_metadata, list):
        reqd_metadata = [reqd_metadata]

    for m in reqd_metadata:
        if m in kwargs_dict:
            metadata[m] = kwargs_dict[m]
    return metadata


def get_diff_with_reqd_metadata(metadata, reqd_metadata):
    k = metadata.keys()
    if not isinstance(reqd_metadata, list):
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


