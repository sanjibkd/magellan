import logging
import logging.config


from collections import OrderedDict
import pandas as pd
import pyprind


import magellan.utils.helperfunctions as helper
import magellan.core.catalog as cg
from magellan.blocker.blocker import Blocker


logging.config.fileConfig(helper.get_install_path()+'/configs/logging.ini')
logger = logging.getLogger(__name__)


class BlackBoxBlocker(Blocker):
    def __init__(self, *args, **kwargs):
        super(Blocker, self).__init__(*args, **kwargs)
        self.black_box_function = None

    def set_black_box_function(self, function):
        self.black_box_function = function

    def block_tables(self, ltable, rtable, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, show_progress=True):

        # validate the presence of black box function
        if self.black_box_function is None:
            raise AssertionError('Black box function is not set')

        # validate output attributes
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # metadata related stuff..

        # required metadata: keys for the input tables
        helper.log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # get metadata
        l_key, r_key = cg.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # validate metadata.
        cg.validate_metadata_for_table(ltable, l_key, 'left', logger, verbose)
        cg.validate_metadata_for_table(rtable, r_key, 'right', logger, verbose)

        # do blocking
        if show_progress:
            bar = pyprind.ProgBar(len(ltable)*len(rtable))

        # #keep track of the list that survives blocking
        block_list = []

        # #set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # #create look up index for faster processing.
        l_dict={}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict={}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # #get the position of the id attribute in the tables.
        l_id_pos = list(ltable.columns).index(l_key)
        r_id_pos = list(rtable.columns).index(r_key)

        # #iterate through the tuples and apply the black box function
        for l_t in ltable.itertuples(index=False):
            for r_t in rtable.itertuples(index=False):
                if show_progress:
                    bar.update()

                l_tuple = l_dict[l_t[l_id_pos]]
                r_tuple = r_dict[r_t[r_id_pos]]

                res = self.black_box_function(l_tuple, r_tuple)

                if not res is True: # "not" because, we want to include only tuple pairs that SURVIVE the blocking fn.
                    d = OrderedDict()

                    # #add ltable and rtable ids
                    ltable_id = l_output_prefix+l_key
                    rtable_id = r_output_prefix+r_key

                    d[ltable_id] = l_tuple[l_key]
                    d[rtable_id] = r_tuple[r_key]

                    # #add left table attributes
                    if l_output_attrs:
                        l_out = l_tuple[l_output_attrs]
                        l_out.index = l_output_prefix + l_out.index
                        d.update(l_out)

                    # #add right table attributes
                    if r_output_attrs:
                        r_out = r_tuple(r_output_attrs)
                        r_out.index = r_output_prefix + r_out.index
                        d.update(r_out)

                    # #add the ordered dict to block_list
                    block_list.append(d)

        # Construct the output table
        candset = pd.DataFrame(block_list)
        l_output_attrs = self.process_output_attrs(ltable, l_key, l_output_attrs, 'left')
        r_output_attrs = self.process_output_attrs(rtable, r_key, r_output_attrs, 'right')

        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs, r_output_attrs,
                                               l_output_prefix, r_output_prefix)
        if len(candset) > 0:
            candset = candset[retain_cols]
        else:
            candset = pd.DataFrame(columns=retain_cols)

        # update metadata in the catalog.
        key = helper.get_name_for_key(candset.columns)
        candset = helper.add_key_column(candset, key)
        cg.set_candset_properties(candset, key, l_output_prefix+l_key, r_output_prefix+r_key, ltable, rtable)

        # return the candidate set
        return candset


    def block_candset(self, candset, verbose=True, show_progress=True):

        # validate the presence of black box function
        if self.black_box_function is None:
            raise AssertionError('Black box function is not set')

        # required metadata: key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key
        helper.log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                                'ltable, rtable, ltable key, rtable key', verbose)

        # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cg.get_metadata_for_candset(candset, logger, verbose)


        # validate metadata
        cg.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)

        # do blocking
        if show_progress:
            bar = pyprind.ProgBar(len(candset))

        # #set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # #create a lookup table for a quick access of rows
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r


        # #keep track of valid ids
        valid = []
        l_id_pos = list(candset.columns).index(l_key)
        r_id_pos = list(candset.columns).index(r_key)

        for row in candset.itertuples(index=False):
            if show_progress:
                bar.update()

            l_tuple = l_dict[row[l_id_pos]]
            r_tuple = r_dict[row[r_id_pos]]

            res = self.black_box_function(l_tuple, r_tuple)

            if not res is True:
                valid.append(True)
            else:
                valid.append(False)

        # construct the output table
        if len(candset) > 0:
            out_table = candset[valid]
        else:
            out_table = pd.DataFrame(columns=candset.columns)

        # update the catalog
        cg.set_candset_properties(out_table, key, fk_ltable, fk_rtable, ltable, rtable)

        # return the output table
        return out_table

    def block_tuples(self, ltuple, rtuple):
        # validate the presence of black box function
        if self.black_box_function is None:
            raise AssertionError('Black box function is not set')

        return self.black_box_function(ltuple, rtuple)




    # utility functions.
