import sys
sys.path.append('/scratch/pradap/python-work/magellan')
import magellan as mg
import pandas as pd
A = pd.read_csv('../magellan/datasets/table_A.csv')
B = pd.read_csv('../magellan/datasets/table_B.csv')

ab = mg.AttrEquivalenceBlocker()
C = ab.block_tables(A, B, 'zipcode', 'zipcode',
                    l_id_attr='ID', r_id_attr='ID',
                    l_output_attrs=['name', 'address', 'birth_year'], r_output_attrs=['name', 'address', 'birth_year'],
                    l_output_prefix='ltable_', r_output_prefix='rtable_',
                    update_catalog=True)
# D = ab.block_candset(C, 'birth_year', 'birth_year')


# print D

print C
