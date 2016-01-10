# import magellan as mg
# A = mg.read_csv('../magellan/datasets/table_A.csv', key='ID')
# print mg.get_catalog()
# mg.to_csv(A, '../magellan/datasets/A.csv')
# print 'Hello'
# B = mg.read_csv('../magellan/datasets/A.csv')
# print mg.get_catalog()

filepath = '../magellan/datasets/A.metadata'
metadata = dict()
num_lines = 0

num_lines = sum(1 for line in open(filepath))
print num_lines

if num_lines > 0:
    with open(filepath) as f:
        for i in range(num_lines):
            line = next(f)
            print line

            if line.startswith('#'):
                line = line.lstrip('#')
                tokens = line.split('=')
                assert len(tokens) is 2, "Error in file, the num tokens is not 2"
                key = tokens[0].strip()
                value = tokens[1].strip()
                if value is not "POINTER":
                    metadata[key] = value


            else:
                stop_flag = True






            # with open(filepath) as f:
            #     stop_flag = False
            #
            #     line = next(f)
            #     print line

            # if line.startswith('#'):
            #     line = line.lstrip('#')
            #     tokens = line.split('=')
            #     assert len(tokens) is 2, "Error in file, the num tokens is not 2"
            #     key = tokens[0].strip()
            #     value = tokens[1].strip()
            #     if value is not "POINTER":
            #         metadata[key] = value
            #     num_lines += 1
            #
            # else:
            #     stop_flag = True

print metadata
