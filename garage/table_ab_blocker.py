import sys
sys.path.append('/scratch/pradap/python-work/magellan')
import magellan as mg
A = mg.read_csv_metadata('../magellan/datasets/table_A.csv', key='ID')
B = mg.read_csv_metadata('../magellan/datasets/table_B.csv', key='ID')

ab = mg.AttrEquivalenceBlocker()
C = ab.block_tables(A, B, 'zipcode', 'zipcode',
                    l_output_attrs=['name', 'address', 'birth_year'], r_output_attrs=['name', 'address', 'birth_year'],
                    l_output_prefix='ltable_', r_output_prefix='rtable_')
D = ab.block_candset(C, 'birth_year', 'birth_year')


print D