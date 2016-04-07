import pandas as pd
import logging
import os
from collections import OrderedDict

import magellan.core.catalog as catalog

logger = logging.getLogger(__name__)


def read_csv_metadata(file_path, **kwargs):
    """
    Read contents from the given file name along with metadata.
    The file name is typically with .csv extension. Metadata is
    expected to be  with the same file name but with .metadata
    extension.

    Args:
    file_path (str): csv file path

    kwargs (dict): key value arguments to pandas read_csv

    Returns:
        result (pandas dataframe)

    Examples:
    >>> import magellan as mg
    >>> A = mg.read_csv_metadata('A.csv')
    # if A.metadata_ is present along with A.csv, the metadata information
    # will be updated for A
    >>> A.get_key()
    """
    # update metadata from file (if present)
    if is_metadata_file_present(file_path):
        file_name, file_ext = os.path.splitext(file_path)
        file_name += '.metadata'
        metadata, num_lines = get_metadata_from_file(file_name)
    else:
        metadata = {}

    # update metadata from kwargs
    metadata, kwargs = update_metadata_for_read_cmd(metadata, **kwargs)
    check_metadata_for_read_cmd(metadata)
    df = pd.read_csv(file_path, **kwargs)
    key = metadata.pop('key', None)
    if key is not None:
        catalog.set_key(df, key)
    for k, v in metadata.iteritems():
        catalog.set_property(df, k, v)
    return df


def to_csv_metadata(df, file_path, **kwargs):
    """
    Write csv file along with metadata

    Parameters
    ----------
    df : pandas DataFrame
        Data frame to written to disk
    file_path : str
        File path where df contents to be written.
        Metadata is written with the same file name
        with .metadata extension

    kwargs : dict
        Key value arguments

    Returns
    -------
    status : bool
        Returns True if the file was written successfully

    Examples
    --------
    >>> import magellan as mg
    >>> A = mg.read_csv_metadata('A.csv')
    >>> mg.to_csv_metadata(A, 'updated.csv')

    """

    index = kwargs.pop('index', None)
    if index is None:
        kwargs['index'] = False

    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + '.metadata'

    # write metadata
    write_metadata(df, metadata_filename)

    # write dataftame
    df.to_csv(file_path, **kwargs)


def write_metadata(df, file_path):
    """
    Write metadata to disk

    Parameters
    ----------
    df : pandas DataFrame

    file_path : str
        File path where the metadata should be written

    """
    metadata_dict = OrderedDict()

    # get all the properties for the input data frame
    if catalog.is_dfinfo_present(df) is True:
        d = catalog.get_all_properties(df)

    # write properties to disk
    if len(d) > 0:
        for k, v in d.iteritems():
            if isinstance(v, basestring) is False:
                metadata_dict[k] = 'POINTER'
            else:
                metadata_dict[k] = v

        with open(file_path, 'w') as f:
            for k, v in d.iteritems():
                f.write('#%s=%s\n' % (k, v))


def is_metadata_file_present(file_path):
    file_name, file_ext = os.path.splitext(file_path)
    file_name += '.metadata'
    return os.path.isfile(file_name)


def get_metadata_from_file(file_path):
    metadata = dict()
    # get the number of lines from the file
    num_lines = sum(1 for line in open(file_path))
    if num_lines > 0:
        # read contents from file
        with open(file_path) as f:
            for i in range(num_lines):
                line = next(f)
                if line.startswith('#'):
                    line = line.lstrip('#')
                    tokens = line.split('=')
                    assert len(tokens) is 2, "Error in file, the num tokens is not 2"
                    key = tokens[0].strip()
                    value = tokens[1].strip()
                    if value is not "POINTER":
                        metadata[key] = value
    return metadata, num_lines


def update_metadata_for_read_cmd(metadata, **kwargs):
    # first update from the key-value arguments
    for k in metadata.keys():
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logger.warning('%s key had a value in file but input arg is set to None')
                v = metadata.pop(k)  # remove the key-value pair

    # Add the properties from key-value arguments
    table_props = ['key', 'ltable', 'rtable', 'fk_ltable', 'fk_rtable']
    for k in table_props:
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logging.getLogger(__name__).warning('%s key had a value in file but input arg is set to None')
                v = metadata.pop(k)  # remove the key-value pair
    return metadata, kwargs


def check_metadata_for_read_cmd(metadata):
    table_props = ['ltable', 'rtable', 'fk_ltable', 'fk_rtable']
    v = set(table_props)
    k = set(metadata.keys())
    i = v.intersection(k)
    if len(i) > 0:
        if len(i) is not len(table_props):
            raise AssertionError('Dataframe requires all valid ltable, rtable, fk_ltable, '
                                 'fk_rtable parameters set')

        if isinstance(metadata['ltable'], pd.DataFrame) is False:
            raise AssertionError('The parameter ltable must be set to valid DataFrame')

        if isinstance(metadata['rtable'], pd.DataFrame) is False:
            raise AssertionError('The parameter rtable must be set to valid MTable')

    return True
