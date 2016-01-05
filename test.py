import pandas as pd

from core.catalog_manager import CatalogManager

cat = CatalogManager()
A = pd.read_csv('datasets/table_A.csv')
cat.set_property(A, 'key', 'ID')

B = pd.read_csv('datasets/table_B.csv')
cat.set_property(B, 'key', 'ID')

print cat.get_property(B, 'key')

print cat.get_dict(A)

print cat.metadata
