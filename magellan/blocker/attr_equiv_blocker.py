import logging
import pandas as pd
import pyprind
import numpy as np

import magellan.utils.helperfunctions
from magellan.blocker.blocker import Blocker
import magellan.core.catalog as cg
# import magellan.utils.metadata as utils
import magellan.utils.helperfunctions as helper

logger = logging.getLogger(__name__)


class AttrEquivalenceBlocker(Blocker):
    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True):

        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # ----------------------------------- metadata related stuff----------------------------------------------

        # required metadata: keys for the input tables.

        helper.log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        helper.log_info(logger, 'Getting metadata from the catalog', verbose)
        l_key = cg.get_key(ltable, 'key')
        r_key = cg.get_key(rtable, 'key')

        cg.validate_metadata_for_table(ltable, l_key, 'left', logger, verbose)
        cg.validate_metadata_for_table(rtable, r_key, 'right', logger, verbose)

        # ----------------------------------- metadata related stuff----------------------------------------------

        # remove nans : should be modified based on the policy for handling missing values
        l_df, r_df = self.rem_nan(ltable, l_block_attr), self.rem_nan(rtable, r_block_attr)

        # do blocking
        candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr, suffixes=('_ltable', '_rtable'))

        # construct output table
        retain_cols, final_cols = self.output_columns(l_key, r_key, list(candset.columns),
                                                      l_output_attrs, r_output_attrs)

        candset = candset[retain_cols]

        # Update catalog for the candidate set
        key = helper.get_name_for_key(candset.columns)
        candset = helper.add_key_column(candset, key)
        cg.set_candset_properties(candset, key, l_output_prefix + l_key, r_output_prefix + r_key, ltable, rtable)

        # return the candidate set
        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr, verbose=True, show_progress=True):

        # required metadata: key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key
        helper.log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                                'ltable, rtable, ltable key, rtable key', verbose)

        # get metadata
        helper.log_info(logger, 'Getting metadata from the catalog', verbose)
        key = cg.get_key(candset)
        fk_ltable = cg.get_fk_ltable(candset)
        fk_rtable = cg.get_fk_rtable(candset)
        ltable = cg.get_property(candset, 'ltable')
        rtable = cg.get_property(candset, 'rtable')
        l_key = cg.get_key(ltable)
        r_key = cg.get_key(rtable)

        # validate metadata
        cg.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)



        # validate block attrs
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)

        # do blocking
        if show_progress:
            bar = pyprind.ProgBar(len(candset))

        # #keep track of valid ids
        valid = []

        # #set index for convenience
        l_df = ltable.set_index(l_key, inplace=False)
        r_df = rtable.set_index(r_key, inplace=False)

        for idx, row in candset.iterrows(): # think about converting this to itertuples
            if show_progress:
                bar.update()

            # #get the value of block attributes
            l_val = l_df.ix[row[l_key], l_block_attr]
            r_val = r_df.ix[row[r_key], r_block_attr]

            if l_val != np.NaN and r_val != np.NaN:
                if l_val == r_val:
                    valid.append(True)
                else:
                    valid.append(False)
            else:
                valid.append(False)

        # construct output table
        if len(candset) > 0:
            out_table = candset[valid]
        else:
            out_table = pd.DataFrame(columns=candset.columns)

        cg.set_candset_properties(out_table, key, fk_ltable, fk_ltable, ltable, rtable)

        # return the output table
        return out_table

    def block_tuples(self, ltuple, rtuple, l_block_attr, r_block_attr):
        return ltuple[l_block_attr] != rtuple[r_block_attr]








    # -----------------------------------------------------
    # utility functions -- this function seems to be specific to attribute equivalence blocking

    # validate the blocking attrs
    def validate_block_attrs(self, ltable, rtable, l_block_attr, r_block_attr):
        if not isinstance(l_block_attr, list):
            l_block_attr = [l_block_attr]
        assert set(l_block_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_block_attr, list):
            r_block_attr = [r_block_attr]
        assert set(r_block_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'

    def output_columns(self, l_key, r_key, col_names, l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix):

        ret_cols = []
        fin_cols = []

        # retain id columns from merge
        ret_l_id = [self.retain_names(x, col_names, '_ltable') for x in [l_key]]
        ret_r_id = [self.retain_names(x, col_names, '_rtable') for x in [r_key]]
        ret_cols.extend(ret_l_id)
        ret_cols.extend(ret_r_id)

        # retain output attrs from merge
        if l_output_attrs:
            l_output_attrs = [x for x in l_output_attrs if x not in [l_key]]
            ret_l_col = [self.retain_names(x, col_names, '_ltable') for x in l_output_attrs]
            ret_cols.extend(ret_l_col)
        if r_output_attrs:
            l_output_attrs = [x for x in r_output_attrs if x not in [r_key]]
            ret_r_col = [self.retain_names(x, col_names, '_rtable') for x in r_output_attrs]
            ret_cols.extend(ret_r_col)

        # final columns in the output
        fin_l_id = [self.final_names(x, l_output_prefix) for x in [l_key]]
        fin_r_id = [self.final_names(x, r_output_prefix) for x in [r_key]]
        fin_cols.extend(fin_l_id)
        fin_cols.extend(fin_r_id)

        # final output attrs from merge
        if l_output_attrs:
            fin_l_col = [self.final_names(x, l_output_prefix) for x in l_output_attrs]
            fin_cols.extend(fin_l_col)
        if r_output_attrs:
            fin_r_col = [self.final_names(x, r_output_prefix) for x in r_output_attrs]
            fin_cols.extend(fin_r_col)

        return ret_cols, fin_cols

    def retain_names(self, x, col_names, suffix):
        if x in col_names:
            return x
        else:
            return str(x) + suffix

    def final_names(self, x, prefix):
        return prefix + str(x)
