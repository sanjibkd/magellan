from magellan.core.catalog import Catalog

_catalog = Catalog.Instance()

# import catalog related methods
from magellan.core.catalog import get_metadata, get_all_metadata, set_metadata, del_metadata, del_all_metadata
from magellan.core.catalog import get_catalog, del_catalog, get_catalog_len
from magellan.core.catalog import is_metadata_present_for_df, is_dfinfo_present, is_catalog_empty
from magellan.core.catalog import get_key, set_key

# io related methods

from magellan.io.parsers import read_csv, to_csv
