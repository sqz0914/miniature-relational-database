import re
from BTrees.OOBTree import OOBTree

'''
Class represents tables which contains operations on tables
'''
class Table:
    '''
    Method which initializes instance variables of the current table
    '''
    def __init__(self):
        self.header = []
        self.data = []
        self.hash_structure = {}
        self.btree = OOBTree()
        self.attr_with_hash_index = {}
        self.attr_with_btree_index = {}
    '''
    Method which converts the current table to string for output
    Return: string representation of the current table
    '''
    def __str__(self):
        table_to_str = ""
        for i, attr_name in enumerate(self.header):
            table_to_str += attr_name
            if i != len(self.header) - 1:
                table_to_str += "|"
        table_to_str += "\n"
        for i, record in enumerate(self.data):
            for j, attr_value in enumerate(record):
                table_to_str += str(attr_value)
                if j != len(record) - 1:
                    table_to_str += "|"
            if i != len(self.data):
                table_to_str += "\n"
        return table_to_str
    '''
    Method which finds the index by attribute name
    Parameters: attr - attribute name
    Return: the index of the attribute
    '''
    def get_attr_index(self, attr):
        for index, attr_name in enumerate(self.header):
            if attr == attr_name:
                return index
    '''
    Method which performs select operation on the current table based on a single condition
    Parameters: condition - function that determines whether one row of data should be added to the result table
                index_attr - attribute with index
                index_key - index key value
    Return: a result table satisfying select condition
    '''
    def select(self, condition, index_attr, index_key):
        result_table = Table()
        result_table.header = self.header
        data = []
        if index_attr is not None:
            if index_attr in self.attr_with_hash_index:
                indices = self.hash_structure[index_key]
            else:
                indices = self.btree[index_key]
            for i in indices:
                data.append(self.data[i])
        else:
            for record in self.data:
                if condition(self, record):
                    data.append(record)
        result_table.data = data
        return result_table
    '''
    Method which performs select operation on the current table based on multiple conditions separated by "or"
    Parameters: conditions - function that determines whether one row of data should be added to the result table
                index_attr - attribute with index
                index_key - index key value
    Return: a result table satisfying select conditions
    '''
    def select_or(self, conditions, index_attr, index_key):
        result_table = Table()
        result_table.header = self.header
        data = []
        for record in self.data:
            flag = False
            for cond in conditions:
                flag = flag or cond(self, record)
            if flag:
                data.append(record)
        result_table.data = data
        return result_table

    '''
    Method which performs select operation on the current table based on multiple conditions separated by "and"
    Parameters: conditions - function that determines whether one row of data should be added to the result table
                index_attr - attribute with index
                index_key - index key value
    Return: a result table satisfying select conditions
    '''
    def select_and(self, conditions, index_attr, index_key):
        result_table = Table()
        result_table.header = self.header
        data = []
        if index_attr is not None:
            if index_attr in self.attr_with_hash_index:
                indices = self.hash_structure[index_key]
            else:
                indices = self.btree[index_key]
            for i in indices:
                record = self.data[i]
                flag = True
                for cond in conditions:
                    flag = flag and cond(self, record)
                if flag:
                    data.append(record)
        else:
            for record in self.data:
                flag = True
                for cond in conditions:
                    flag = flag and cond(self, record)
                if flag:
                    data.append(record)
        result_table.data = data
        return result_table
    '''
    Method which performs join operation on the current table based on a single condition
    Parameters: table2 - the second table that needs to be joined to the first table
                condition - function that determines whether one row of data should be added to the result table
                table1_name - name of the first table
                table2_name - name of the second table
                ref_attr - attribute whose value can be the index key value
                rev_flag - indicator that indicates whether the attribute with index is in the first or the second table
    Return: a result table satisfying join conditions
    '''
    def join(self, table2, condition, table1_name, table2_name, index_attr, ref_attr, rev_flag):
        result_table = Table()
        header = []
        if rev_flag:
            for attr_name in table2.header:
                header.append("{0}_{1}".format(table1_name, attr_name))
            for attr_name in self.header:
                header.append("{0}_{1}".format(table2_name, attr_name))
        else:
            for attr_name in self.header:
                header.append("{0}_{1}".format(table1_name, attr_name))
            for attr_name in table2.header:
                header.append("{0}_{1}".format(table2_name, attr_name))
        result_table.header = header
        data = []
        if ref_attr is not None:
            ref_attr_index = table2.get_attr_index(ref_attr)
            for record2 in table2.data:
                indices = []
                if index_attr in self.attr_with_hash_index:
                    if record2[ref_attr_index] in self.hash_structure:
                        indices = self.hash_structure[record2[ref_attr_index]]
                else:
                    if record2[ref_attr_index] in self.btree:
                        indices = self.btree[record2[ref_attr_index]]
                if indices:
                    for i in indices:
                        record1 = self.data[i]
                        new_record = []
                        if rev_flag:
                            new_record.extend(record2)
                            new_record.extend(record1)
                        else:
                            new_record.extend(record1)
                            new_record.extend(record2)
                        data.append(new_record)
        else:
            for record1 in self.data:
                for record2 in table2.data:
                    if condition(self, record1, table2, record2):
                        new_record = []
                        new_record.extend(record1)
                        new_record.extend(record2)
                        data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs join operation on the current table based on a single condition
    Parameters: table2 - the second table that needs to be joined to the first table
                condition - function that determines whether one row of data should be added to the result table
                table1_name - name of the first table
                table2_name - name of the second table
                ref_attr - attribute whose value can be the index key value
                rev_flag - indicator that indicates whether the attribute with index is in the first or the second table
    Return: a result table satisfying join conditions
    '''
    def join_and(self, table2, conditions, table1_name, table2_name, index_attr, ref_attr, rev_flag):
        result_table = Table()
        header = []
        if rev_flag:
            for attr_name in table2.header:
                header.append("{0}_{1}".format(table1_name, attr_name))
            for attr_name in self.header:
                header.append("{0}_{1}".format(table2_name, attr_name))
        else:
            for attr_name in self.header:
                header.append("{0}_{1}".format(table1_name, attr_name))
            for attr_name in table2.header:
                header.append("{0}_{1}".format(table2_name, attr_name))
        result_table.header = header
        data = []
        if ref_attr is not None:
            ref_attr_index = table2.get_attr_index(ref_attr)
            for record2 in table2.data:
                indices = []
                if index_attr in self.attr_with_hash_index:
                    if record2[ref_attr_index] in self.hash_structure:
                        indices = self.hash_structure[record2[ref_attr_index]]
                else:
                    if record2[ref_attr_index] in self.btree:
                        indices = self.btree[record2[ref_attr_index]]
                if indices:
                    for i in indices:
                        record1 = self.data[i]
                        flag = True
                        for cond in conditions:
                            if rev_flag:
                                flag = flag and cond(table2, record2, self, record1)
                            else:
                                flag = flag and cond(self, record1, table2, record2)
                        if flag:
                            new_record = []
                            if rev_flag:
                                new_record.extend(record2)
                                new_record.extend(record1)
                            else:
                                new_record.extend(record1)
                                new_record.extend(record2)
                            data.append(new_record)
        else:
            for record1 in self.data:
                for record2 in table2.data:
                    flag = True
                    for cond in conditions:
                        flag = flag and cond(self, record1, table2, record2)
                    if flag:
                        new_record = []
                        new_record.extend(record1)
                        new_record.extend(record2)
                        data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs project operation on the current table
    Parameters: attrs - attributes columns that need to be projected
    Return: a result table containing data of the attributes columns projected from the current table
    '''
    def project(self, attrs):
        result_table = Table()
        header = []
        attr_indices = []
        for i, attr_name in enumerate(self.header):
            if attr_name in attrs:
                header.append(attr_name)
                attr_indices.append(i)
        result_table.header = header
        data = []
        for record in self.data:
            new_record = []
            for i in attr_indices:
                new_record.append(record[i])
            data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs sum/avg operation on the current table based on a single attribute
    Parameters: attr - attribute that needs to be calculated sum/average
                avg - operation indicator: sum/avg
    Return: a result table containing the calculated sum/average of the given attribute
    '''
    def sum(self, attr, avg=False):
        result_table = Table()
        header = []
        if avg:
            header.append("avg{0}".format(attr))
        else:
            header.append("sum{0}".format(attr))
        result_table.header = header
        col_data = []
        attr_index = self.get_attr_index(attr)
        for record in self.data:
            col_data.append(record[attr_index])
        data = []
        if avg:
            data.append([sum(col_data)/len(col_data)])
        else:
            data.append([sum(col_data)])
        result_table.data = data
        return result_table
    '''
    Method which performs sumgroup/avggroup operation on the current table based on the given attributes
    Parameters: sum_attr - attribute that needs to be calculated sums/averages grouped by other given attributes
                group_attrs - group attributes
                avg - operation indicator: sumgroup/avggroup
    Return: a result table containing the calculated sums/averages of an attribute grouped by other given attributes
    '''
    def sumgroup(self, sum_attr, group_attrs, avg=False):
        result_table = Table()
        header = []
        if avg:
            header.append("avggroup{0}".format(sum_attr))
        else:
            header.append("sumgroup{0}".format(sum_attr))
        sum_attr_index = self.get_attr_index(sum_attr)
        group_attrs_indices = []
        for attr_name in self.header:
            if attr_name in group_attrs:
                header.append(attr_name)
                group_attrs_indices.append(self.get_attr_index(attr_name))
        result_table.header = header
        ga_sa_map = {}
        ga_sa_count = {}
        for record in self.data:
            ga_key = []
            for i in group_attrs_indices:
                ga_key.append(record[i])
            ga_key = tuple(ga_key)
            if ga_key in ga_sa_map:
                ga_sa_map[ga_key] += record[sum_attr_index]
                ga_sa_count[ga_key] += 1
            else:
                ga_sa_map[ga_key] = record[sum_attr_index]
                ga_sa_count[ga_key] = 1
        data = []
        for ga_key, sa_sum in ga_sa_map.items():
            new_record = []
            if avg:
                new_record.append(sa_sum/ga_sa_count[ga_key])
            else:
                new_record.append(sa_sum)
            new_record.extend(list(ga_key))
            data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs count operation on the current table
    Parameters: table_name - name of the current table
    Return: a result table containing the counted number of rows of the current table
    '''
    def count(self, table_name):
        result_table = Table()
        header = ["count{0}".format(table_name)]
        result_table.header = header
        data = []
        data.append([len(self.data)])
        result_table.data = data
        return result_table
    '''
    Method which performs countgroup operation on the current table
    Parameters: count_attr - attribute that needs to be counted grouped by other given attributes
                group_attrs - group attributes
    Return: a result table containing the counted number of rows of an attribute grouped by other given attributes 
    '''
    def countgroup(self, count_attr, group_attrs):
        result_table = Table()
        header = []
        header.append("countgroup{0}".format(count_attr))
        group_attrs_indices = []
        for attr_name in self.header:
            if attr_name in group_attrs:
                header.append(attr_name)
                group_attrs_indices.append(self.get_attr_index(attr_name))
        result_table.header = header
        ga_ca_count = {}
        for record in self.data:
            ga_key = []
            for i in group_attrs_indices:
                ga_key.append(record[i])
            ga_key = tuple(ga_key)
            if ga_key in ga_ca_count:
                ga_ca_count[ga_key] += 1
            else:
                ga_ca_count[ga_key] = 1
        data = []
        for ga_key, ca_num in ga_ca_count.items():
            new_record = []
            new_record.append(ca_num)
            new_record.extend(list(ga_key))
            data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs movsum/movavg operation on the current table
    Parameters: movsum_attr - attribute that needs to be calculated moving sums/averages
                movsum_range - size of the moving window
                avg - operation indicator: movsum/movavg
    Return: a result table containing the calculated moving sums/averages of the given attribute
    '''
    def movsum(self, movsum_attr, movsum_range, avg=False):
        result_table = Table()
        header = []
        header.extend(self.header)
        if avg:
            header.append("movavg{0}".format(movsum_attr))
        else:
            header.append("movsum{0}".format(movsum_attr))
        result_table.header = header
        new_attr_index = len(result_table.header) - 1
        movsum_attr_index = self.get_attr_index(movsum_attr)
        data = []
        for i,record in enumerate(self.data):
            new_record = []
            new_record.extend(record)
            new_record.append(0)
            mov_count = 0
            for j in range(movsum_range):
                if i - j >= 0:
                    mov_count += 1
                    new_record[new_attr_index] += self.data[i-j][movsum_attr_index]
            if avg:
                new_record[new_attr_index] = new_record[new_attr_index] / mov_count
            data.append(new_record)
        result_table.data = data
        return result_table
    '''
    Method which performs sort operation on the current table
    Parameters: sort_attrs - attributes that need to be sorted
    Return: a result table containing the sorted data of the current table by the given attributes
    '''
    def sort(self, sort_attrs):
        result_table = Table()
        result_table.header = self.header
        data = []
        for record in self.data:
            data.append(record)
        sort_attrs_indices = []
        for attr in sort_attrs:
            sort_attrs_indices.append(self.get_attr_index(attr))
        result_table.data = sorted(data, key=lambda x: tuple(x[i] for i in sort_attrs_indices))
        return result_table
    '''
    Method which performs concat operation on two tables with the same schema
    Parameters: table2 - another table that will be concatenated to the current table
    Return: a result table generated by the concatenation of the given two tables
    '''
    def concat(self, table2):
        result_table = Table()
        result_table.header = self.header
        data = []
        for record in self.data:
            data.append(record)
        for record in table2.data:
            data.append(record)
        result_table.data = data
        return result_table
    '''
    Method that performs outputtofile operation on the current table
    Parameters: file_name - the name of the output file
    Result: output the current table to the given file
    '''
    def output_to_file(self,file_name):
        with open("{0}.txt".format(file_name), 'w') as f:
            f.write(str(self))
    '''
    Method that performs Hash operation on the current table
    Parameters: attr - attribute that will be indexed
    Result: create hash index on the given attribute of the current table
    '''
    def create_hash_index(self, attr):
        self.hash_structure = {}
        self.attr_with_hash_index = {}
        attr_index = self.get_attr_index(attr)
        self.attr_with_hash_index[attr] = True
        for index in range(len(self.data)):
            if self.data[index][attr_index] in self.hash_structure:
                self.hash_structure[self.data[index][attr_index]].append(index)
            else:
                self.hash_structure[self.data[index][attr_index]] = [index]
    '''
    Method that performs Btree operation on the current table
    Parameters: attr - attribute that will be indexed
    Result: create BTree index on the given attribute of the current table    
    '''
    def create_btree_index(self, attr):
        self.btree = OOBTree()
        self.attr_with_btree_index = {}
        attr_index = self.get_attr_index(attr)
        self.attr_with_btree_index[attr] = True
        for index in range(len(self.data)):
            if self.data[index][attr_index] in self.btree:
                self.btree[self.data[index][attr_index]].append(index)
            else:
                self.btree[self.data[index][attr_index]] = [index]

