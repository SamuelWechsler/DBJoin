from sqlalchemy import *
import csv
import pandas as pd
from datetime import datetime


def getConnection(dbPath):
    """
    dbPath: path to database
    return: engine object
    """
    return create_engine(dbPath, echo=False)


def outputCSV(data, header):
    """
    This function creates a csv file containing a header and the specified
    data.
    """
    csvfile = open('result.csv', 'w')
    csvwriter = csv.writer(csvfile, delimiter=';')
    csvwriter.writerow(header)
    csvwriter.writerows(data)


def reprTbl(data, header):
    """
    This function creates a pandas dataframe to represent a header and 
    data in a table.
    """
    outputCSV(data, header)
    tbl = pd.read_csv('result.csv', delimiter=';', header=0)
    print(tbl.to_string(index=False))


def joinQry(db):
    """
    This function runs a SQL query that utilizes the INNER JOIN syntax. 
    It retrieves all rows from table A and table B where the condition 
    "table_A.time = table_B.time_B" is met.
    """
    t1 = datetime.now()
    engine = getConnection(db)
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT table_A.primaryKey_A, table_A.time_A, table_A.text_A, table_B.moreText_B FROM table_A INNER JOIN table_B ON table_A.time_A = table_B.time_B"))
        t2 = datetime.now()
        print("Qry finished in ", t2-t1)
        return result.fetchall()


# get join of tables
db = "sqlite:///merge_db.sqlite"
result = joinQry(db)

# show result
header = ['primaryKey', 'time', 'text', 'moreText']
reprTbl(result, header)
