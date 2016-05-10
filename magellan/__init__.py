from magellan.core.catalog import Catalog

__version__ = '0.1.0'


_catalog = Catalog.Instance()
#
# import catalog related methods
from magellan.core.catalog_manager import get_property, get_all_properties, set_property, del_property, del_all_properties
from magellan.core.catalog_manager import get_catalog, del_catalog, get_catalog_len, show_properties, \
    show_properties_for_id
from magellan.core.catalog_manager import is_property_present_for_df, is_dfinfo_present, is_catalog_empty
from magellan.core.catalog_manager import get_key, set_key
#
# # io related methods
#
from magellan.io.parsers import read_csv, to_csv
from magellan.io.pickles import load_object, load_table, save_object, save_table
#
#
# # blockers
# from magellan.blocker.attr_equiv_blocker import AttrEquivalenceBlocker
#
#


#
#
# # helper functions
from magellan.utils.generic_helper import get_install_path, load_dataset