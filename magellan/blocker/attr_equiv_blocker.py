#imports
import pandas as pd
import numpy as np
import logging


from magellan.blocker.blocker import Blocker
import magellan.core.catalog as cg
import magellan.utils.helperfunctions as helper

class AttrEquivalenceBlocker(Blocker):

    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None, l_key=None, r_key=None):

        ltable_reqd_metadata = ['key']
        rtable_reqd_metadata = ['key']

        # get metadata

        # ltable
        ltable_metadata = helper.get_reqd_metdata_from_catalog(ltable, ltable_reqd_metadata)
        if l_key is not None:
            ltable_metadata['key'] = l_key
        l_diff_reqd_metadata = list(helper.get_diff_with_reqd_metadata(ltable_metadata, ltable_reqd_metadata))
        assert len(l_diff_reqd_metadata) == 0, \
            'The following metadata for ltable is missing : ' + str(l_diff_reqd_metadata)

        # rtable
        rtable_metadata = helper.get_reqd_metdata_from_catalog(ltable, ltable_reqd_metadata)
        if r_key is not None:
            rtable_metadata['key'] = l_key
        r_diff_reqd_metadata = list(helper.get_diff_with_reqd_metadata(rtable_metadata, rtable_reqd_metadata))
        assert len(r_diff_reqd_metadata) == 0, \
            'The following metadata for ltable is missing : ' + str(r_diff_reqd_metadata)


        # print the metadata that we are going to use
        logging.info('ltable metadata ...')
        logging.info(ltable_metadata)
        logging.info('rtable metadata')
        logging.info(rtable_metadata)

        # check constraints
        # check key constraints
        # ltable
        lkey_status = helper.is_key_attribute(ltable, ltable_metadata['key'])
        assert lkey_status==False, 'ltable key attribute ' + ltable_metadata['key'] + ' does not qualify to be a key'

        # rtable
        rkey_status = helper.is_key_attribute(rtable, rtable_metadata['key'])
        assert rkey_status==False, 'rtable key attribute ' + rtable_metadata['key'] + ' does not qualify to be a key'

        # -- check attrs
        l_output_attrs, r_output_attrs = self.check_attrs(ltable, rtable, l_block_attr, r_block_attr,
                                                          l_output_attrs, r_output_attrs)
        # remove nans
        l_df = self.rem_nan(ltable, l_block_attr)
        r_df = self.rem_nan(rtable, r_block_attr)

        candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr,
                           suffixes=('_ltable', '_rtable'))

        # get output columns
        retain_cols, final_cols = self.output_columns(ltable.get_key(), rtable.get_key(), list(candset.columns),
                                                   l_output_attrs, r_output_attrs)

        candset = candset[retain_cols]
        candset.columns = final_cols

        cg.set_key(candset, '_id')
        cg.set_metadata(candset, 'ltable', ltable)
        cg.set_metadata(candset, 'rtable', rtable)
        cg.set_metadata(candset, 'foreign_key_ltable', 'ltable.' + ltable_metadata['key'])
        cg.set_metadata(candset, 'foreogn_key_rtable', 'rtable.'+rtable_metadata['key'])

        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr,
                      key=None, ltable=None, rtable=None, foreign_key_ltable=None, foreign_key_rtable=None):

        reqd_metadata = ['key', 'ltable', 'rtable', 'foreign_key_ltable', 'foreign_key_rtable']

        metadata = helper.get_reqd_metdata_from_catalog(candset, reqd_metadata)
        # update using kw args
        if key is not None:
            metadata['key'] = key
        if ltable is not None:
            metadata['ltable'] = ltable
        if rtable is not None:
            metadata['rtable'] = rtable
        if foreign_key_ltable is not None:
            metadata['foreign_key_ltable'] = foreign_key_ltable
        if foreign_key_rtable is not None:
            metadata['foreign_key_rtable'] = foreign_key_rtable

        # check diff
        diff_metadata = list(helper.get_diff_with_reqd_metadata(metadata, reqd_metadata))
        assert len(diff_metadata) == 0, \
            'The following metadata is missing : ' + str(diff_metadata)


        # constraints
        key_status = helper.is_key_attribute(candset, metadata['key'])
        assert key_status == True, "The key attribute " + metadata['key'] + " does not quality to be a key"
        # fk constraints
        assert cg.get_key(ltable) != None, 'Key for ltable is not set'
        assert cg.get_key(rtable) != None, 'Key for rtable is not set'
        l_fk_status = helper.check_fk_constraint(candset, metadata['foreign_key_ltable'], ltable, cg.get_key(ltable))
        r_fk_status = helper.check_fk_constraint(candset, metadata['foreign_key_rtable'], rtable, cg.get_key(rtable))

        assert l_fk_status == True, 'Attribute ' + metadata['foreign_key_ltable'] + ' does not satisfy ' \
                                                                                    'foreign key constraint'
        assert r_fk_status == True, 'Attribute ' + metadata['foreign_key_rtable'] + ' does not satisfy ' \
                                                                                    'foreign key constraint'

        valid = []
        l_df = ltable.copy()
        r_df = rtable.copy()
        l_df.set_index(cg.get_key(ltable), inplace=True)
        r_df.set_index(cg.get_key(rtable), inplace=True)
        l_key = cg.get_key(ltable)
        r_key = cg.get_key(rtable)

        for idx, row in candset.iterrows():
            # get the value of block attribute from ltuple
            l_val = l_df.ix[row[l_key], l_block_attr]
            r_val = r_df.ix[row[r_key], r_block_attr]
            if l_val != np.NaN and r_val != np.NaN:
                if l_val == r_val:
                    valid.append(True)
                else:
                    valid.append(False)
            else:
                valid.append(False)

        # update properties in catalog










    # check integrity of attrs in l_block_attr, r_block_attr
    def check_attrs(self, ltable, rtable, l_block_attr, r_block_attr, l_output_attrs, r_output_attrs):

        # check block attributes form a subset of left and right tables
        if not isinstance(l_block_attr, list):
            l_block_attr = [l_block_attr]
        assert set(l_block_attr).issubset(ltable.columns) is True, 'Left block attribute is not in left table'

        if not isinstance(r_block_attr, list):
            r_block_attr = [r_block_attr]
        assert set(r_block_attr).issubset(rtable.columns) is True, 'Right block attribute is not in right table'

        # check output columns form a part of left, right tables
        if l_output_attrs:
            if not isinstance(l_output_attrs, list):
                l_output_attrs = [l_output_attrs]
            assert set(l_output_attrs).issubset(ltable.columns) is True, 'Left output attributes ' \
                                                                         'are not in left table'
            l_output_attrs = [x for x in l_output_attrs if x not in [ltable.get_key()]]

        if r_output_attrs:
            if not isinstance(r_output_attrs, list):
                r_output_attrs = [r_output_attrs]
            assert set(r_output_attrs).issubset(rtable.columns) is True, 'Right output attributes ' \
                                                                         'are not in right table'
            r_output_attrs = [x for x in r_output_attrs if x not in [rtable.get_key()]]

        return l_output_attrs, r_output_attrs


 # get output columns
    def output_columns(self, l_key, r_key, col_names, l_output_attrs, r_output_attrs):

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
        fin_l_id = [self.final_names(x, 'ltable.') for x in [l_key]]
        fin_r_id = [self.final_names(x, 'rtable.') for x in [r_key]]
        fin_cols.extend(fin_l_id)
        fin_cols.extend(fin_r_id)

        # final output attrs from merge
        if l_output_attrs:
            fin_l_col = [self.final_names(x, 'ltable.') for x in l_output_attrs]
            fin_cols.extend(fin_l_col)
        if r_output_attrs:
            fin_r_col = [self.final_names(x, 'rtable.') for x in r_output_attrs]
            fin_cols.extend(fin_r_col)

        return ret_cols, fin_cols


    def retain_names(self, x, col_names, suffix):
        if x in col_names:
            return x
        else:
            return str(x) + suffix

    def final_names(self, x, prefix):
        return prefix + str(x)