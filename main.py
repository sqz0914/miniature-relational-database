import time
from process import *

if __name__ == "__main__":
    tables = {}           # A dictionary which stores all the created tables
    with open("test.txt", "r") as f:
        queries = f.readlines()
    for query in queries:
        query = query.strip()
        start_time = time.time()
        process_input_query(query, tables)
        end_time = time.time()
        print("Query Executed: ", query)
        print("Execution Time: ", end_time-start_time, "sec\n")

    '''
    query = ""
    tables = {}
    while query != "exit":
        query = input("> ")
        query = query.strip()
        start_time = time.time()
        process_input_query(query, tables)
        end_time = time.time()
        print("Query Executed: ", query)
        print("Execution Time: ", end_time-start_time, "sec")
    '''
