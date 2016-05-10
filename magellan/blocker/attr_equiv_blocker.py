import logging


import pandas as pd
import numpy as np

from magellan.blocker.blocker import Blocker
import magellan.core.catalog_manager as cm
from magellan.utils.catalog_helper import log_info
from magellan.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)

class AttrEquivalenceBlocker(Blocker):
    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True):

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # get and validate required metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm.validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # do blocking

        # # remove nans: should be modified based on missing data policy
        l_df, r_df = rem_nan(ltable, l_block_attr), rem_nan(rtable, r_block_attr)

        # # use pandas merge to do equi join
        candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr, suffixes=('_ltable', '_rtable'))

        # construct output table
        retain_cols, final_cols = self.output_columns(l_key, r_key, list(candset.columns),
                                                      l_output_attrs, r_output_attrs,
                                                      l_output_prefix, r_output_prefix)
        candset = candset[retain_cols]
        candset.columns = final_cols

        # update catalog




        pass



















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