import cloud
import os
import pickle

from collections import OrderedDict
from magellan import Catalog
from magellan.io.parsers import is_metadata_file_present


def save_object(obj, file_path):
    """
      Save magellan objects

      Parameters
      ----------
      obj : Python objects.
        It can be magellan objects such as rule-based blocker, feature table, rule-based matcher,
          match trigger

      file_path : str
        File path to store object

      Returns
      -------
      status : bool
        returns True if the command executes successfully.
    """
    with open(file_path, 'w') as f:
        cloud.serialization.cloudpickle.dump(obj, f)
    return True


def load_object(file_path):
    """
    Load magellan objects

    Parameters
    ----------
    file_path : str
        file path to load object from
    Returns
    -------
    result : Python object
        typically magellan objects such as rule-based blocker, feature table, rule-based matcher,
        match_trigger
    """
    with open(file_path, 'r') as f:
        result = pickle.load(f)
    return result


def save_table_metadata(df, file_path):
    """
    Pickle data frame along with metadata

    Parameters
    ----------
    df : pandas DataFrame

    file_path : str
        file path where the data frame must be stored

    Returns
    -------
    status : bool
        returns True if the command executes successfully.
    """

    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + '.metadata'
    catalog = Catalog.Instance()

    metadata_dict = OrderedDict()

    # get all the properties for the input data frame
    if catalog.is_dfinfo_present(df) is True:
        d = catalog.get_all_properties(df)

    # write properties to disk
    if len(d) > 0:
        for k, v in d.iteritems():
            if isinstance(v, basestring) is True:
                metadata_dict[k] = v

    with open(file_path, 'w') as f:
        cloud.serialization.cloudpickle.dump(df, f)

    # write metadata contents
    with open(metadata_filename, 'w') as f:
        cloud.serialization.cloudpickle.dump(metadata_dict, f)

    return True


def load_table_metadata(file_path):
    """
    load table from file

    Parameters
    ----------
    file_path : str
        File path to load file from

    Returns
    -------
    loaded_dataframe : pandas DataFrame
         returns the data frame loaded from file

    """
    # load data frame from file path
    df = pickle.load(file_path)

    # load metadata from file path
    if is_metadata_file_present(file_path):
        file_name, file_ext = os.path.splitext(file_path)
        metadata_filename = file_name + '.metadata'
        catalog = Catalog.Instance()
        metadata_dict = pickle.load(metadata_filename)
        # update metadata in the catalog
        for key, value in metadata_dict.iteritems():
            if key is 'key':
                catalog.set_key(df, key)
            else:
                catalog.set_property(df, key, value)
    return df
