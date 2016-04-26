import numpy as np
import magellan.utils.helperfunctions as helper
import magellan.core.catalog as cg


class Blocker(object):
    pass

    # remove nows with nan values at block_attr

    @staticmethod
    def rem_nan(table, block_attr):
        l = table.index.values[np.where(table[block_attr].notnull())[0]]
        return table.ix[l]



    @staticmethod
    def process_output_attrs(table, key, attrs, error_str=""):
        if attrs:
            if not isinstance(attrs, list):
                attrs = [attrs]
            assert set(attrs).issubset(table.columns) == True, 'Output attributes are not in ' + error_str + ' table'

            attrs = [x for x in attrs if x not in [key]]

        return attrs

    def validate_output_attrs(self, ltable, rtable, l_output_attrs, r_output_attrs):
        if l_output_attrs:
            if not isinstance(l_output_attrs, list):
                l_output_attrs = [l_output_attrs]
            assert set(l_output_attrs).issubset(ltable.columns) is True, 'Left output attribtutes are not in the ' \
                                                                         'left table'
        if r_output_attrs:
            if not isinstance(r_output_attrs, list):
                r_output_attrs = [r_output_attrs]
            assert set(r_output_attrs).issubset(rtable.columns) is True, 'Right output attribtutes are not ' \
                                                                         'in the right table'
