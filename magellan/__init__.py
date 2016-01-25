from magellan.core.catalog import Catalog

_catalog = Catalog.Instance()

# import catalog related methods
from magellan.core.catalog import get_property, get_all_properties, set_property, del_property, del_all_properties
from magellan.core.catalog import get_catalog, del_catalog, get_catalog_len
from magellan.core.catalog import is_property_present_for_df, is_dfinfo_present, is_catalog_empty
from magellan.core.catalog import get_key, set_key

# io related methods

from magellan.io.parsers import read_csv_metadata, to_csv_metadata
from magellan.io.pickles import load_object, load_table_metadata, save_object, save_table_metadata


# blockers
from magellan.blocker.attr_equiv_blocker import AttrEquivalenceBlocker



# logger
import logging
logging.basicConfig(level=logging.INFO)