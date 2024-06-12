# Miniature Relational Database

## Project Description

This project implements a miniature relational database system in Python that supports basic operations of relational algebra including selection, projection, join, group by, and aggregates like count, sum, and average. It also supports operations like sorting, moving sums, and moving averages on columns. The database can import and export data from vertical bar-delimited files and create in-memory B-trees and hash structures for indexing.

## Features

- **Relational Algebra Operations**: Selection, projection, join, group by, count, sum, and average.
- **Sorting and Moving Aggregates**: Sort tables by columns and perform moving sums and averages.
- **File Operations**: Import and export vertical bar-delimited files.
- **Indexing**: Create in-memory B-trees and hash structures for efficient data retrieval.
- **Performance Tracking**: Print the time taken to execute each operation.

## Constraints

- No use of relational algebra or SQL libraries or systems (e.g., SQLite, MySQL, Pandas).
- The program should take operations from standard input.
- The input operations should be one per line with optional comments starting with `//`.

## Usage Instructions

1. **Prerequisites**

- Python 3.6 or higher

2. **Setup**

- Clone the repository

```bash
git clone https://github.com/sqz0914/miniature-relational-database.git
cd miniature-relational-database
```

- Install necessary packages:

```bash
pip install -r requirements.txt
```

3. **File Structure**

- `table.py`: Contains the `Table` class which defines the structure and operations of the tables.
- `utils.py`: Contains the utility functions.
- `process.py`: Contains the functions to process each operation.
- `main.py`: The main script to run the database operations.
- `test.txt`: Example file containing database operations.
- `sales1.txt` and `sales2.txt`: Example data files.

4. **Running the Program**

- Prepare your operations in a file, e.g., test.txt, with one operation per line. Here is an example:

```bash
R := inputfromfile(sales1)
R1 := select(R, (time > 50) or (qty < 30))
R2 := project(R1, saleid, qty, pricerange)
R3 := avg(R1, qty)
R4 := sumgroup(R1, time, qty)
R5 := sumgroup(R1, qty, time, pricerange)
R6 := avggroup(R1, qty, pricerange)
S := inputfromfile(sales2)
T := join(R, S, R.customerid = S.C)
T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid))
T2 := sort(T1, S_C)
T2prime := sort(T1, R1_time, S_C)
T3 := movavg(T2prime, R1_qty, 3)
T4 := movsum(T2prime, R1_qty, 5)
Q1 := select(R, qty = 5)
Btree(R, qty)
Q2 := select(R, qty = 5)
Q3 := select(R, itemid = 7)
Hash(R, itemid)
Q4 := select(R, itemid = 7)
Q5 := concat(Q4, Q2)
outputtofile(Q5, Q5)
outputtofile(T, T)

```

- Run the program:

```bash
python main.py
```

- The program will read operations from `test.txt`, execute them, and print the execution time for each operation.

5. **Example Data Files**

- Example data files can be found [here](/sales1.txt) and [here](/sales2.txt).

6. **Operations Overview**
- **inputfromfile**: Imports data from a file into a table.
- **select**: Selects rows from a table based on conditions.
- **project**: Projects specific columns from a table.
- **join**: Joins two tables based on a condition.
- **sum**: Calculates the sum of a column.
- - **sumgroup**: Calculates the sum of a column grouped by other columns.
- **avg**: Calculates the average of a column.
- **avggroup**: Calculates the average of a column grouped by other columns.
- **count**: Counts the number of rows in a table.
- **countgroup**: Counts the number of rows grouped by other columns.
- **movsum**: Calculates the moving sum of a column.
- **movavg**: Calculates the moving average of a column.
- **sort**: Sorts a table by specific columns.
- **concat**: Concatenates two tables with the same schema.
- **outputtofile**: Outputs a table to a file.
- **Hash**: Creates a hash index on a column.
- **Btree**: Creates a B-tree index on a column.