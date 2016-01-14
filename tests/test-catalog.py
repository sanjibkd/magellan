import pandas as pd
import magellan as mg

A = pd.read_csv('magellan/datasets/table_A.csv')
B = pd.read_csv('magellan/datasets/table_B.csv')

print mg.is_catalog_empty()

mg.set_key(A, 'ID')

# mg.set_metadata(A, 'key', 'ID')
# print mg.get_metadata(A, 'key')

print mg.get_key(A)

print mg.is_dfinfo_present(B)
# mg.set_metadata(B, 'key', 'ID')
# print mg.get_metadata(B, 'key')
mg.set_key(B, 'ID')

print mg.is_dfinfo_present(B)
print mg.is_property_present_for_df(B, 'ltable')

print mg.get_catalog_len()

mg.del_property(B, 'key')
print mg.is_property_present_for_df(B, 'ID')


mg.del_all_properties(A)
print mg.get_catalog_len()


