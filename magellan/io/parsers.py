
import pandas as pd
import logging
import os
from collections import OrderedDict

import magellan.core.catalog as catalog

logger = logging.getLogger(__name__)

def read_csv(filepath, **kwargs):
    if is_metadata_file_present(filepath) == True:
        filename, fileextension = os.path.splitext(filepath)
        filename = filename +'.metadata'
        metadata, numlines = get_metadata_from_file(filename)
    else:
        metadata = {}
    metadata, kwargs = update_metadata_for_read_cmd(metadata, **kwargs)
    check_metadata_for_read_cmd(metadata)
    df = pd.read_csv(filepath, **kwargs)
    key = metadata.pop('key', None)
    if key is not None:
        catalog.set_key(df, key)
    for k, v in metadata.iteritems():
        catalog.set_property(df, k, v)
    return df


def to_csv(df, filepath, **kwargs):
    index = kwargs.pop('index', None)
    if index == None:
        kwargs['index'] = False



    filename, fileextention = os.path.splitext(filepath)
    metadata_filename = filename +'.metadata'

    # write metadata
    write_metadata(df, metadata_filename)

    # write dataftame
    df.to_csv(filepath, **kwargs)

def write_metadata(df, filepath):
    metadata_dict = OrderedDict()
    if catalog.is_dfinfo_present(df) == True:
        d = catalog.get_all_properties(df)
    if len(d) > 0:
        for k, v in d.iteritems():
            if isinstance(v, basestring) is False:
                metadata_dict[k] = 'POINTER'
            else:
                metadata_dict[k] = v

        with open(filepath, 'w') as f:
            for k, v in d.iteritems():
                f.write('#%s=%s\n' %(k, v))




def is_metadata_file_present(filepath):
    filename, fileextension = os.path.splitext(filepath)
    filename = filename +'.metadata'
    return os.path.isfile(filename)


def get_metadata_from_file(filepath):
    metadata = dict()
    num_lines = sum(1 for line in open(filepath))

    if num_lines > 0:
        with open(filepath) as f:
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
                else:
                    stop_flag = True

    return metadata, num_lines

def update_metadata_for_read_cmd(metadata, **kwargs):
    # first update
    for k in metadata.keys():
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logger.warning('%s key had a value in file but input arg is set to None')
                v = metadata.pop(k) # remove the key-value pair

    # Now add
    # following are the list of properties that the user can provide
    mtable_props = ['key', 'ltable', 'rtable', 'foreign_key_ltable', 'foreign_key_rtable']
    for k in mtable_props:
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logging.getLogger(__name__).warning('%s key had a value in file but input arg is set to None')
                v = metadata.pop(k) # remove the key-value pair
    return metadata, kwargs



def check_metadata_for_read_cmd(metadata):
    vtable_props = ['ltable', 'rtable', 'foreign_key_ltable', 'foreign_key_rtable']
    v = set(vtable_props)
    k = set(metadata.keys())
    i = v.intersection(k)
    if len(i) > 0 :
        if len(i) is not len(vtable_props):
            raise AssertionError('Dataframe equires all valid ltable, rtable, foreign_key_ltable, '
                                  'foreign_key_rtable parameters set')

        if isinstance(metadata['ltable'], pd.DataFrame) == False:
            raise AssertionError('The parameter ltable must be set to valid DataFrame')
        if isinstance(metadata['rtable'], pd.DataFrame) == False:
            raise AssertionError('The parameter rtable must be set to valid MTable')
    return True



