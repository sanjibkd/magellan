
from magellan.core.catalog import get_all_metadata
import numpy as np 
import pandas as pd

def get_reqd_metdata_from_catalog(df, reqd_metadata):
    if isinstance(reqd_metadata, list) == False:
        reqd_metadata = [reqd_metadata]

    metadata = {}
    d = get_all_metadata(df)
    for m in reqd_metadata:
        if m in d:
            metadata[m] = d[m]
    return metadata
