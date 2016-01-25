import logging
import magellan.utils.helperfunctions as helper
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


# key related methods
def get_key(df):
    return get_property(df, 'key')


def set_key(df, key):
    if magellan.utils.metadata_utils.is_key_attribute(df, key) is False:
        logger.warning('Attribute ('+key+') does not qualify to be a key')
        return False
    else:
        return set_property(df, 'key', key)
