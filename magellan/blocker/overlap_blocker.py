from  magellan.blocker.blocker import Blocker
import pyximport; pyximport.install()
import logging
import logging.config
import math
import re, string
import pandas as pd
import pyprind


from magellan.external.py_stringmatching.tokenizers import qgram
import magellan.utils.helperfunctions as helper
from collections import Counter, OrderedDict

logging.config.fileConfig(helper.get_install_path()+'/configs/logging.ini')
logger = logging.getLogger(__name__)


class OverlapBlocker(Blocker):
    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to',
                           'was', 'were', 'will', 'with']
        self.regex_punctuation = re.compile('[%s]' %re.escape(string.punctuation))
        super(OverlapBlocker, self).__init__()

    def block_tables(self, ltable, rtable, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, qgram=None, word_level=True, overlap_size=1,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_'):


        return False


    # helper functions
    # validate the blocking attrs
    def validate_overlap_attrs(self, ltable, rtable, l_overlap_attr, r_overlap_attr):
        if not isinstance(l_overlap_attr, list):
            l_overlap_attr = [l_overlap_attr]
        assert set(l_overlap_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_overlap_attr, list):
            r_overlap_attr = [r_overlap_attr]
        assert set(r_overlap_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'