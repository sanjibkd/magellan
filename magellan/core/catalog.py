import logging
import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig()

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
        self.metadata_catalog = {}


    def init_metadata(self, df):
        df_id = id(df)
        self.metadata_catalog[df_id] = {}
        return True

    def get_metadata(self, df, name):
        df_id = id(df)
        d = self.metadata_catalog[df_id]
        return d[name]


    def set_metadata(self, df, name, value):
        df_id = id(df)
        d = self.metadata_catalog[df_id]
        d[name] = value
        self.metadata_catalog[df_id] = d
        return True

    def get_all_metadata(self, df):
        df_id = id(df)
        d = self.metadata_catalog[df_id]
        return d

    def del_metadata(self, df, name):
        df_id = id(df)
        d = self.metadata_catalog[df_id]
        del d[name]
        self.metadata_catalog[df_id] = d
        return True

    def del_all_metadata(self, df):
        df_id = id(df)
        del self.metadata_catalog[df_id]
        return True


    def get_catalog(self):
        return self.metadata_catalog

    def del_catalog(self):
        self.metadata_catalog = {}
        return True

    def get_catalog_len(self):
        return len(self.metadata_catalog)

    def is_catalog_empty(self):
        return self.metadata_catalog > 0


    def is_dfinfo_present(self, df):
        return id(df) in self.metadata_catalog

    def is_metadata_present_for_df(self, df, name):
        df_id = id(df)
        d = self.metadata_catalog[df_id]
        return name in d




def get_metadata(df, name):
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    if catalog.is_metadata_present_for_df(df, name) == False:
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.get_metadata(df, name)

def set_metadata(df, name, value):
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        catalog.init_metadata(df)

    catalog.set_metadata(df, name, value)

def get_all_metadata(df):
    catalog = Catalog.Instance()

    if df is None:
        raise AttributeError('Input dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.get_all_metadata(df)


def del_metadata(df, name):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    if catalog.is_metadata_present_for_df(df, name) == False:
        raise KeyError('Requested metadata (' + name + ') for the given dataframe is not present in the catalog')

    return catalog.del_metadata(df, name)

def del_all_metadata(df):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.del_all_metadata(df)

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

def is_metadata_present_for_df(df, name):
    catalog = Catalog.Instance()
    if df is None:
        raise AttributeError('Input Dataframe cannot be null')

    if catalog.is_dfinfo_present(df) == False:
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.is_metadata_present_for_df(df, name)

def get_catalog_len():
    catalog = Catalog.Instance()
    return catalog.get_catalog_len()

# key related methods
def get_key(df):
    return get_metadata(df, 'key')


def set_key(df, key):
    if is_key_attribute(df, key) == False:
        logger.warning('Attribute ('+key+') does not qualify to be a key')
        return False
    else:
        return set_metadata(df, 'key', key)




def is_key_attribute(df, key):
    # check if the length is > 0
    if len(df) > 0:
        # check for uniqueness
        uniq_flag = len(np.unique(df[key])) == len(df)
        if not uniq_flag:
            logger.warning('Attribute contains duplicate values')

        # check if there are missing or null values
        nan_flag = sum(df[key].isnull()) == 0
        if not nan_flag:
            logger.warning('Attribute contains missing values')
        return uniq_flag and nan_flag
    else:
        return True
