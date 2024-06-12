from table import Table
from utils import *

'''
Method which processes inputfromfile operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that needs to be filled with data from the data file
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table with the given table name
'''
def process_inputfromfile(op_name, result_table_name, params, tables):
    file_name = params[0]
    result_table = input_from_file(file_name)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes select operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the project result
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the select result
'''
def process_select(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    index_attr = None
    index_key = None
    if is_complex_expr(params[1]):
        # Deal with multiple conditions
        conditions, logops = parse_complex_expr(params[1])
        relop_evals = []
        for cond in conditions:
            relop_eval = evaluate_select_relop_expr(cond)
            relop_evals.append(relop_eval)
            relop_expr = parse_relop_expr(cond)
            # Find attribute with index inside equality condition
            if relop_expr[1] == "=":
                attr, value = get_select_equal_relop_attr_value(relop_expr)
                if attr in target_table.attr_with_hash_index or attr in target_table.attr_with_btree_index:
                    index_attr = attr
                    index_key = value
        if logops[0] == "and":
            result_table = target_table.select_and(relop_evals, index_attr, index_key)
        else:
            result_table = target_table.select_or(relop_evals, index_attr, index_key)
    else:
        # Deal with single condition
        condition = params[1]
        relop_eval = evaluate_select_relop_expr(condition)
        relop_expr = parse_relop_expr(condition)
        # Find attribute with index inside equality condition
        if relop_expr[1] == "=":
            attr, value = get_select_equal_relop_attr_value(relop_expr)
            if attr in target_table.attr_with_hash_index or attr in target_table.attr_with_btree_index:
                index_attr = attr
                index_key = value
        result_table = target_table.select(relop_eval, index_attr, index_key)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes join operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the project result
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the join result
'''
def process_join(op_name, result_table_name, params, tables):
    target_table1_name = params[0]
    target_table2_name = params[1]
    target_table1 = tables[target_table1_name]
    target_table2 = tables[target_table2_name]
    index_attr = None
    ref_attr = None
    rev_flag = False
    if is_complex_expr(params[2]):
        # Deal with multiple conditions
        conditions, logops = parse_complex_expr(params[2])
        relop_evals = []
        for cond in conditions:
            relop_eval = evaluate_join_relop_expr(cond)
            relop_evals.append(relop_eval)
            relop_expr = parse_relop_expr(cond)
            # Find attribute with index inside equality condition
            if relop_expr[1] == "=":
                if (not is_arithop_expr(relop_expr[0])) and (not is_arithop_expr(relop_expr[2])):
                    attr1 = parse_attr(relop_expr[0])[1]
                    attr2 = parse_attr(relop_expr[2])[1]
                    if attr1 in target_table1.attr_with_hash_index or attr1 in target_table1.attr_with_btree_index:
                        index_attr = attr1
                        ref_attr = attr2
                    elif attr2 in target_table2.attr_with_hash_index or attr2 in target_table2.attr_with_btree_index:
                        index_attr = attr2
                        ref_attr = attr1
                        rev_flag = True
        if rev_flag:
            result_table = target_table2.join_and(target_table1, relop_evals, target_table1_name, target_table2_name, index_attr, ref_attr, rev_flag)
        else:
            result_table = target_table1.join_and(target_table2, relop_evals, target_table1_name, target_table2_name, index_attr, ref_attr, rev_flag)
    else:
        # Deal with single condition
        condition = params[2]
        relop_eval = evaluate_join_relop_expr(condition)
        relop_expr = parse_relop_expr(condition)
        # Find attribute with index inside equality condition
        if relop_expr[1] == "=":
            if (not is_arithop_expr(relop_expr[0])) and (not is_arithop_expr(relop_expr[2])):
                attr1 = parse_attr(relop_expr[0])[1]
                attr2 = parse_attr(relop_expr[2])[1]
                if attr1 in target_table1.attr_with_hash_index or attr1 in target_table1.attr_with_btree_index:
                    index_attr = attr1
                    ref_attr = attr2
                elif attr2 in target_table2.attr_with_hash_index or attr2 in target_table2.attr_with_btree_index:
                    index_attr = attr2
                    ref_attr = attr1
                    rev_flag = True
        if rev_flag:
            result_table = target_table2.join(target_table1, relop_eval, target_table1_name, target_table2_name, index_attr, ref_attr, rev_flag)
        else:
            result_table = target_table1.join(target_table2, relop_eval, target_table1_name, target_table2_name, index_attr, ref_attr, rev_flag)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes project operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the project result
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the project result
'''
def process_project(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    attrs = params[1:]
    result_table = target_table.project(attrs)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes sum operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated sum result
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated sum result
'''
def process_sum(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    attr = params[1]
    result_table = target_table.sum(attr)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes avg operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated average result
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated average result
'''
def process_avg(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    attr = params[1]
    result_table = target_table.sum(attr, True)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes sumgroup operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated group sum results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated group sum results
'''
def process_sumgroup(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    sum_attr = params[1]
    group_attrs = params[2:]
    result_table = target_table.sumgroup(sum_attr, group_attrs)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes avggroup operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated group average results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated group average results
'''
def process_avggroup(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    sum_attr = params[1]
    group_attrs = params[2:]
    result_table = target_table.sumgroup(sum_attr, group_attrs, True)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes count operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the table row count results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the table row count results
'''
def process_count(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    result_table = target_table.count(target_table_name)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes countgroup operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the attribute values group count results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the attribute values group count results
'''
def process_countgroup(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    count_attr = params[1]
    group_attrs = params[2:]
    result_table = target_table.countgroup(count_attr, group_attrs)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes movsum operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated moving sum results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated moving sum results
'''
def process_movsum(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    movsum_attr = params[1]
    movsum_range = int(params[2])
    result_table = target_table.movsum(movsum_attr, movsum_range)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes movavg operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the calculated moving average results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the calculated moving average results
'''
def process_movavg(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    movsum_attr = params[1]
    movsum_range = int(params[2])
    result_table = target_table.movsum(movsum_attr, movsum_range, True)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes sort operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the sorted data results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the sorted data results
'''
def process_sort(op_name, result_table_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    sort_attrs = params[1:]
    result_table = target_table.sort(sort_attrs)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes sort operation
Parameters: op_name - name of the operation
            result_table_name - name of the table that stores the tables concatenation results
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: generate a new table containing the tables concatenation results
'''
def process_concat(op_name, result_table_name, params, tables):
    target_table_name1 = params[0]
    target_table_name2 = params[1]
    target_table1 = tables[target_table_name1]
    target_table2 = tables[target_table_name2]
    # Check the schema of the two tables
    if len(target_table1.header) != len(target_table2.header):
        print("Unable to process concat operation")
        print("Tables with different schema")
        return
    else:
        for i in range(len(target_table1.header)):
            if target_table1.header[i] != target_table2.header[i]:
                print("Unable to process concat operation")
                print("Tables with different schema")
                return
    result_table = target_table1.concat(target_table2)
    tables[result_table_name] = result_table
    output_operation_result(op_name, result_table_name, result_table)

'''
Method which processes outputtofile operation
Parameters: op_name - name of the operation
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: output the table to a seperate file
'''
def process_outputtofile(op_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    file_name = params[1]
    target_table.output_to_file(file_name)

'''
Method which processes Hash operation
Parameters: op_name - name of the operation
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: create index on a given attribute of a table
'''
def process_Hash(op_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    attr = params[1]
    target_table.create_hash_index(attr)

'''
Method which processes Btree operation
Parameters: op_name - name of the operation
            params - parameters of the operation
            tables - a dictionary that stores all the created tables
Result: create index on a given attribute of a table
'''
def process_Btree(op_name, params, tables):
    target_table_name = params[0]
    target_table = tables[target_table_name]
    attr = params[1]
    target_table.create_btree_index(attr)

'''
Method which preprocesses the input query, extracts the operation name and calls the corresponding operation process method
Parameters: query - input query
            tables - a dictionary that stores all the created tables
Result: call different operation process methods to deal with different operations
'''
def process_input_query(query, tables):
    # Get operation name
    if ':=' in query:
        op_name = query[query.index(":=")+2:query.index("(")].strip()
        # Get result table name
        result_table_name = query[:query.index(":=")].strip()
    else:
        op_name = query[:query.index("(")].strip()
    # Get and split parameters
    params = query[query.index("(")+1:query.rindex(")")].strip().split(",")
    params = [p.strip() for p in params]
    # Find the corresponding operation process method
    if op_name == "inputfromfile":
        process_inputfromfile(op_name, result_table_name, params, tables)
    elif op_name == "select":
        process_select(op_name, result_table_name, params, tables)
    elif op_name == "join":
        process_join(op_name, result_table_name, params, tables)
    elif op_name == "project":
        process_project(op_name, result_table_name, params, tables)
    elif op_name == "sum":
        process_sum(op_name, result_table_name, params, tables)
    elif op_name == "avg":
        process_avg(op_name, result_table_name, params, tables)
    elif op_name == "sumgroup":
        process_sumgroup(op_name, result_table_name, params, tables)
    elif op_name == "avggroup":
        process_avggroup(op_name, result_table_name, params, tables)
    elif op_name == "count":
        process_count(op_name, result_table_name, params, tables)
    elif op_name == "countgroup":
        process_countgroup(op_name, result_table_name, params, tables)
    elif op_name == "movsum":
        process_movsum(op_name, result_table_name, params, tables)
    elif op_name == "movavg":
        process_movavg(op_name, result_table_name, params, tables)
    elif op_name == "sort":
        process_sort(op_name, result_table_name, params, tables)
    elif op_name == "concat":
        process_concat(op_name, result_table_name, params, tables)
    elif op_name == "outputtofile":
        process_outputtofile(op_name, params, tables)
    elif op_name == "Hash":
        process_Hash(op_name, params, tables)
    elif op_name == "Btree":
        process_Btree(op_name, params, tables)
    else:
        raise ValueError("Operation Not Found")

