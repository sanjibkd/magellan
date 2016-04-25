import pandas as pd
import pyprind
import numpy as np

import magellan.utils.helperfunctions
from magellan.blocker.blocker import Blocker
import magellan.core.catalog as cg
# import magellan.utils.metadata as utils
import magellan.utils.helperfunctions as helper


class AttrEquivalenceBlocker(Blocker):
    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_id_attr=None, r_id_attr=None,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     update_catalog=False):

        # process the metadata information.
        l_metadata = self.process_table_metadata(ltable, l_id_attr, 'ltable', True, True)
        r_metadata = self.process_table_metadata(rtable, r_id_attr, 'rtable', True)

        # @todo print the metadata that is getting used
        status = self.check_attrs_present(ltable, l_block_attr)
        assert status is True, 'Left block attribute is not present in the left table'

        status = self.check_attrs_present(rtable, r_block_attr)
        assert status is True, 'Right block attribute is not present in the right table'

        # process the output attributes
        l_output_attrs = self.process_output_attrs(ltable, l_output_attrs, 'left')
        r_output_attrs = self.process_output_attrs(rtable, r_output_attrs, 'right')




        # rem nans @todo this should be modified based on missing data handling policy
        l_df = self.rem_nan(ltable, l_block_attr)
        r_df = self.rem_nan(rtable, r_block_attr)

        # do the blocking
        candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr,
                           suffixes=('_ltable', '_rtable'))

        # get output columns
        retain_cols, final_cols = self.output_columns(cg.get_key(ltable),
                                                      cg.get_key(rtable), list(candset.columns),
                                                      l_output_attrs, r_output_attrs,
                                                      l_output_prefix, r_output_prefix)

        # project and rename columns
        candset = candset[retain_cols]
        candset.columns = final_cols
        key = helper.get_name_for_key(candset.columns)
        candset = helper.add_key_column(candset, key)

        # set properties
        cg.set_key(candset, key)
        cg.set_property(candset, 'ltable', ltable)
        cg.set_property(candset, 'rtable', rtable)
        cg.set_property(candset, 'fk_ltable', l_output_prefix + l_metadata['key'])
        cg.set_property(candset, 'fk_rtable', r_output_prefix + r_metadata['key'])

        # update catalog
        if update_catalog:
            if l_id_attr is not None:
                cg.set_key(l_id_attr)
            if r_id_attr is not None:
                cg.set_key(r_id_attr)

        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr,
                      id_attr=None,
                      l_fk_attr=None, r_fk_attr=None,
                      ltable=None, rtable=None,
                      l_id_attr=None, r_id_attr=None):

        # get metadata
        metadata = self.process_candset_metadata(candset, id_attr, l_fk_attr, ltable, r_fk_attr, rtable)

        l_fk_attr, ltable = metadata['fk_ltable'], metadata['ltable']
        r_fk_attr, rtable = metadata['fk_rtable'], metadata['rtable']
        l_key, r_key = cg.get_key(ltable), cg.get_key(rtable)

        status = self.check_attrs_present(ltable, l_block_attr)
        assert status is True, 'Left block attribute is not present in left table'

        status = self.check_attrs_present(rtable, r_block_attr)
        assert status is True, 'Right block attribute is not present in right table'

        l_df, r_df = ltable.copy(), rtable.copy()

        # # set index for convenience
        l_df.set_index(l_key, inplace=True)
        r_df.set_index(r_key, inplace=True)

        bar = pyprind.ProgBar(len(candset))

        # create look up table for quick access of rows
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r
        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        valid = []
        column_names = list(candset.columns)
        lid_idx = column_names.index(l_fk_attr)
        rid_idx = column_names.index(r_fk_attr)

        for row in candset.itertuples(index=False):
            bar.update()
            l_row = l_dict[row[lid_idx]]
            r_row = r_dict[row[rid_idx]]
            l_val = l_row[l_block_attr]
            r_val = r_row[r_block_attr]
            if l_val != np.NaN and r_val != np.NaN:
                if l_val == r_val:
                    valid.append(True)
                else:
                    valid.append(False)
            else:
                valid.append(False)

        if len(candset) > 0:
            out_table = candset[valid]
            cg.set_key(out_table, metadata['key'])
        else:
            out_table = pd.DataFrame(columns=candset.columns)
            cg.set_key(out_table, metadata['key'])

        cg.set_property(out_table, 'ltable', ltable)
        cg.set_property(out_table, 'rtable', rtable)
        cg.set_property(out_table, 'fk_ltable', l_fk_attr)
        cg.set_property(out_table, 'fk_rtable', r_fk_attr)


        return out_table

    def block_tuples(self, ltuple, rtuple, l_block_attr, r_block_attr):
        return ltuple[l_block_attr] != rtuple[r_block_attr]

    # -----------------------------------------------------
    # utility functions -- this function seems to be specific to attribute equivalence blocking

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
            ret_l_col = [self.retain_names(x, col_names, '_ltable') for x in l_output_attrs]
            ret_cols.extend(ret_l_col)
        if r_output_attrs:
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
