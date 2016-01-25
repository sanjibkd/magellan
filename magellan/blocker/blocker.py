import numpy as np
import magellan.utils.metadata_utils as utils
import magellan.utils.helperfunctions as helper
import magellan.core.catalog as cg


class Blocker(object):
    pass


    # remove nows with nan values at block_attr
    def rem_nan(self, table, block_attr):
        l = table.index.values[np.where(table[block_attr].notnull())[0]]
        return table.ix[l]

    # process metadata for pandas dataframe
    def process_table_metadata(self, table, key, error_str='table', check_key=True):

        # get required metadata from catalog
        # update the metadata
        # check metadata
        # return a dict with metadata if everything is fine
        reqd_metadata=['key']
        metadata = utils.get_reqd_metadata_from_catalog(table, reqd_metadata)
        if key is not None:
            metadata['key'] = key

        # make sure that all the required metadata is present
        diff_metadata = list(utils.get_diff_with_reqd_metadata(metadata, reqd_metadata))
        assert len(diff_metadata) == 0, ' The following metadata for ' + error_str + ' is missing: ' \
                                        + str(diff_metadata)

        # check key
        if check_key:
            status = magellan.utils.metadata_utils.is_key_attribute(table, metadata['key'])
            assert status == True, error_str + ' key attribute ' + metadata['key'] + ' does not qualify to be a key'
        return metadata

    def process_candset_metadata(self, candset, key, fk_ltable, ltable, fk_rtable, rtable):
        # get the required metadata from catalog
        # update the metadata
        # check metadata
        # return the dict with metadata if everything is fine
        reqd_metadata = ['key', 'ltable', 'rtable', 'fk_ltable', 'fk_rtable']
        metadata = utils.get_reqd_metadata_from_catalog(candset, reqd_metadata)
        metadata.update(metadata)
        if key is not None:
            metadata['key'] = key
        if ltable is not None:
            metadata['ltable'] = ltable
        if rtable is not None:
            metadata['rtable'] = rtable
        if fk_ltable is not None:
            metadata['fk_ltable'] = fk_ltable
        if fk_rtable is not None:
            metadata['fk_rtable'] = fk_rtable
        diff_metadata = list(utils.get_diff_with_reqd_metadata())
        assert len(diff_metadata) == 0, ' The following metadata for the given table is missing ' + str(diff_metadata)

        # check metadata

        # key
        key_status = magellan.utils.metadata_utils.is_key_attribute(candset, metadata['key'])
        assert key_status == True, 'The key attribute ' + metadata['key'] + ' does not qualify to be a key'

        # fk
        l_meta = self.process_table_metadata(metadata['ltable'], None, 'ltable', False)
        r_meta = self.process_table_metadata(metadata['rtable'], None, 'rtable', False)
        fk_status = utils.check_fk_constraint(candset, metadata['fk_ltable'], metadata['ltable'], l_meta['key'])
        assert fk_status == True, 'Attribute ' + metadata['fk_ltable'] + ' does not satisfy foreign key constraint'
        fk_status = utils.check_fk_constraint(candset, metadata['fk_rtable'], metadata['rtable'], r_meta['key'])
        assert fk_status == True, 'Attribute ' + metadata['fk_rtable'] + ' does not satisfy foreign key constraint'
        return metadata

    def check_attrs_present(self, table, attrs):
        status = helper.are_all_attributes_present(table, attrs, verbose=True)
        return status

    def process_output_attrs(self, table, attrs, error_str=""):
        if attrs:
            if not isinstance(attrs, list):
                attrs = [attrs]
            assert set(attrs).issubset(table.columns) is True, 'Output attributes are not in ' + error_str + ' table'
            attrs = [x for x in attrs if x not in [cg.get_key(table)]]
        return attrs








