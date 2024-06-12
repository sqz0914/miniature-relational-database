import re
from table import Table

'''
Method which writes the result of an operation to the file "AllOperations.txt"
Parameters: op_name - name of the operation
            result_table_name - name of the result table created by the operation
            result_table - table containing the operation result
'''
def output_operation_result(op_name, result_table_name, result_table):
    with open("AllOperations.txt", "a") as f:
        f.write("Table Name: {0}    Operation Performed: {1}\n\n".format(result_table_name, op_name))
        f.write(str(result_table))
        f.write("\n\n\n")

'''
Method which reads data from a data file
Parameters: file_name - name of the data file
Return: a table filled with data
'''
def input_from_file(file_name):
    table = Table()
    with open("{0}.txt".format(file_name), 'r') as f:
        lines = f.readlines()
        # Store table header
        table.header = lines[0].strip().split("|")
        # Store data
        data = []
        for line in lines[1:]:
            values = line.strip().split("|")
            record = []
            for value in values:
                # Check numbers(integers)
                if bool(re.match(r"^[0-9]+$", value)):
                    record.append(int(value))
                else:
                    record.append(value)
            data.append(record)
    table.data = data
    return table

'''
Method which checks whether string is const value
Parameters: s - string
Return: boolean value
'''
def is_const(s):
    return bool(re.match(r"^[0-9]+$|^[0-9]+\.[0-9]+$|'.+'",s))

'''
Method which checkes whether string is number (integer/float)
Parameters: s - string
Return: boolean value
'''
def is_num(s):
    return bool(re.match(r"^[0-9]+$|^[0-9]+\.[0-9]+$",s))

'''
Method which checks whether string is literal
Parameters: s - string
Return: boolean value
'''
def is_str(s):
    return bool(re.match(r"'.+'",s))

'''
Method which gets string literal from string
Parameters: s - string
Return: string literal
'''
def get_str(s):
    return s[s.index("'")+1:s.rindex("'")]

'''
Method which gets number from string
Parameters: s - string
Return: float number
'''
def get_number(s):
    return float(s)

'''
Method which checks whether string is arithop expression
Parameters: s - string
Return: boolean value
'''
def is_arithop_expr(s):
    return bool(re.match(r".+\s*[\+\-\*\/]\s*.+",s))

'''
Method which parses arithop expression (e.g qty / 7)
Parameters: s - string
Return: parse results (e.g "qty", "/", "7")
'''
def parse_arithop_expr(s):
    match = re.match(r"(.+)\s*([\+\-\*\/])\s*(.+)",s)
    return match[1].strip(), match[2].strip(), match[3].strip()

'''
Method which parses relop expression (e.g itemid = 7)
Parameters: s - string
Return: parse results (e.g "itemid", "=", "7")
'''
def parse_relop_expr(s):
    if "!=" in s:
        part1 = s[:s.index("!=")]
        part2 = "!="
        part3 = s[s.index("!=")+2:]
        return part1.strip(), part2, part3.strip()
    elif ">=" in s:
        part1 = s[:s.index(">=")]
        part2 = ">="
        part3 = s[s.index(">=")+2:]
        return part1.strip(), part2, part3.strip()
    elif "<=" in s:
        part1 = s[:s.index("<=")]
        part2 = "<="
        part3 = s[s.index("<=")+2:]
        return part1.strip(), part2, part3.strip()
    else:
        match = re.match(r"(.+)\s*([=|>|<])\s*(.+)",s)
        return match[1].strip(), match[2].strip(), match[3].strip()

'''
Method which checks whether expression is complex
Parameters: s - string
Return: boolean value
'''
def is_complex_expr(s):
    return bool(re.match(r"\(.+\)",s))

'''
Method which parses complex expression (e.g (time > 50) or (qty < 30))
Parameters: s - string
Return: parse results (e.g "time > 50", "or", "qty < 30")
'''
def parse_complex_expr(s):
    conditions = re.findall(r"\((.*?)\)", s)
    conditions = [cond.strip() for cond in conditions]
    logops = re.findall(r"\)(.*?)\(", s)
    logops = [op.strip() for op in logops]
    return conditions, logops

'''
Method which evaluates simplified relop expression
Parameters: value1 - value of the left side of the expression
            operator - relop (=, !=, >, >=, <, <=)
            value2 - value of the right side of the expression
Return: boolean value
'''
def evaluate_relop(value1, operator, value2):
    if operator == "<":
        return value1 < value2
    if operator == "<=":
        return value1 <= value2
    if operator == ">":
        return value1 > value2
    if operator == ">=":
        return value1 >= value2
    if operator == "=":
        return value1 == value2
    if operator == "!=":
        return value1 != value2

'''
Method which evaluates simplified arithop expression
Parameters: value1 - value of the left side of the expression
            operator - arithop (+, -, *, /)
            value2 - value of the right side of the expression
Return: arithmetic value 
'''
def evaluate_arithop(value1, operator, value2):
    value1 = float(value1)
    value2 = float(value2)
    if operator == "-":
        return value1 - value2
    if operator == "+":
        return value1 + value2
    if operator == "*":
        return value1 * value2
    if operator == "/":
        return value1 / value2

'''
Method which evaluates value of an attribute with index in arithop expression
Parameters: value1 - value of the right side of the expression
            operator - arithop (+, -, *, /)
            value2 - value of the left side of the expression
Return: attribute value
'''
def rev_evaluate_arithop(value1, operator, value2):
    value1 = float(value1)
    value2 = float(value2)
    if operator == "-":
        return value1 + value2
    if operator == "+":
        return value1 - value2
    if operator == "*":
        return value1 / value2
    if operator == "/":
        return value1 * value2

'''
Method which evaluates arithop expression inside select operation
Parameters: expr - arithop expression that needs to be evaluated
Return: lambda function that evaluates arithop expression
'''
def evaluate_select_arithop_expr(expr):
    arithop_expr = parse_arithop_expr(expr)
    attr = arithop_expr[0]
    arithop_eval = lambda table, record: evaluate_arithop(record[table.get_attr_index(attr)], arithop_expr[1], arithop_expr[2])
    return arithop_eval

'''
Method which evaluates condition (relop expression) inside select operation
Parameters: expr - condition expression that needs to be evaluated
Return: lambda function that evaluates condition expression
'''
def evaluate_select_relop_expr(expr):
    relop_expr = parse_relop_expr(expr)
    operator = relop_expr[1]
    # evaluate expression like qty / 2 > 30
    if is_arithop_expr(relop_expr[0]):
        arithop_eval = evaluate_select_arithop_expr(relop_expr[0])
        if is_num(relop_expr[2]):
            constant = get_number(relop_expr[2])
        else:
            constant = get_str(relop_expr[2])
        relop_eval = lambda table, record: evaluate_relop(arithop_eval(table, record), operator, constant)
    # evaluate expression like 30 > qty / 2 or 30 > qty
    elif is_const(relop_expr[0]):
        if is_num(relop_expr[0]):
            constant = get_number(relop_expr[0])
        else:
            constant = get_str(relop_expr[0])
        # evaluate expression like 30 > qty / 2
        if is_arithop_expr(relop_expr[2]):
            arithop_eval = evaluate_select_arithop_expr(relop_expr[2])
            relop_eval = lambda table, record: evaluate_relop(constant, operator, arithop_eval(table, record))
        # evaluate expression like 30 > qty
        else:
            attr = relop_expr[2]
            relop_eval = lambda table, record: evaluate_relop(constant, operator, record[table.get_attr_index(attr)])
    # evaluate expression like qty > 30
    else:
        attr = relop_expr[0]
        if is_num(relop_expr[2]):
            constant = get_number(relop_expr[2])
        else:
            constant = get_str(relop_expr[2])
        relop_eval = lambda table, record: evaluate_relop(record[table.get_attr_index(attr)], operator, constant)
    return relop_eval

'''
Method which gets the attribute value of the equality condition inside select operation
Parameters: relop_expr - parsed equality condition
Return: attrbute name and its value
'''
def get_select_equal_relop_attr_value(relop_expr):
    if is_const(relop_expr[2]):
        if is_num(relop_expr[2]):
            value = get_number(relop_expr[2])
            if is_arithop_expr(relop_expr[0]):
                # Deal with expression like qty / 2 = 30
                arithop_expr = parse_arithop_expr(relop_expr[0])
                value = rev_evaluate_arithop(value, arithop_expr[1], arithop_expr[2])
                attr = arithop_expr[0]
            else:
                # Deal with expression like qty = 30
                attr = relop_expr[0]
        else:
            value = get_str(relop_expr[2])
            attr = relop_expr[0]
    else:
        if is_num(relop_expr[0]):
            value = get_number(relop_expr[0])
            if is_arithop_expr(relop_expr[2]):
                # Deal with expression like 30 = qty / 2
                arithop_expr = parse_arithop_expr(relop_expr[2])
                value = rev_evaluate_arithop(value, arithop_expr[1], arithop_expr[2])
                attr = arithop_expr[0]
            else:
                # Deal with expression like 30 = qty
                attr = relop_expr[2]
        else:
            value = get_str(relop_expr[0])
            attr = relop_expr[2]
    return attr, value


'''
Method which parses attribute string for join operation (e.g R.customerid)
Parameters: s - string
Return: parse results ("R", "customerid")
'''
def parse_attr(s):
    match = re.match(r"((?:\w|\d|\.)+)\.((?:\w|\d|\.)+)", s)
    return match[1].strip(), match[2].strip()

'''
Method which evaluates arithop expression inside join operation
Parameters: expr - arithop expression that needs to be evaluated
Return: lambda function that evaluates arithop expression
'''
def evaluate_join_arithop_expr(expr):
    arithop_expr = parse_arithop_expr(expr)
    attr = parse_attr(arithop_expr[0])[1]
    arithop_eval = lambda table, record: evaluate_arithop(record[table.get_attr_index(attr)], arithop_expr[1], arithop_expr[2])
    return arithop_eval

'''
Method which evaluates condition (relop expression) inside join operation
Parameters: expr - condition expression that needs to be evaluated
Return: lambda function that evaluates condition expression
'''
def evaluate_join_relop_expr(expr):
    relop_expr = parse_relop_expr(expr)
    operator = relop_expr[1]
    if is_arithop_expr(relop_expr[0]):
        arithop_eval1 = evaluate_join_arithop_expr(relop_expr[0])
        if is_arithop_expr(relop_expr[2]):
            # Deal with expression like R.qty * 5 > S.Q * 3
            arithop_eval2 = evaluate_join_arithop_expr(relop_expr[2])
            relop_eval = lambda table1, record1, table2, record2: evaluate_relop(arithop_eval1(table1, record1), operator, arithop_eval2(table2, record2))
        else:
            # Deal with expression like R.qty * 5 > S.Q
            attr = parse_attr(relop_expr[2])[1]
            relop_eval = lambda table1, record1, table2, record2: evaluate_relop(arithop_eval1(table1, record1), operator, record2[table2.get_attr_index(attr)])
    else:
        attr1 = parse_attr(relop_expr[0])[1]
        if is_arithop_expr(relop_expr[2]):
            # Deal with expression like R.qty > S.Q * 3
            arithop_eval2 = evaluate_join_arithop_expr(relop_expr[2])
            relop_eval = lambda table1, record1, table2, record2: evaluate_relop(record1[table1.get_attr_index(attr1)], operator, arithop_eval2(table2, record2))
        else:
            # Deal with expression like R.qty > S.Q
            attr2 = parse_attr(relop_expr[2])[1]
            relop_eval = lambda table1, record1, table2, record2: evaluate_relop(record1[table1.get_attr_index(attr1)], operator, record2[table2.get_attr_index(attr2)])
    return relop_eval