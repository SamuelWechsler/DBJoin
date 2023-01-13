from sqlalchemy import *
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base


def getConnection(dbPath):
    """
    dbPath: path to database
    return: engine object
    """
    return create_engine(dbPath, echo=False)


def getColumnNames(db: str, tbl: str) -> list:
    """
    db: str of database name
    tbl: str of table name

    return: list containing all column names of a table
    """
    engine = getConnection(db)
    with engine.connect() as conn:
        result = conn.execute(f"SELECT * FROM {tbl}")
        return list(result.keys())


def addTblSchema(srcDB: str, srcTbl: str, destDB: str, destTbl: str) -> None:
    """
    srcDB: str of source database
    srcTbl: str of source table (in source database)
    destDB: str of destination database
    destTbl: str of destination table

    This function queries the table schema of a given source table and then creates
    a destination table with an identical schema in the destination database.
    """
    srcEngine = getConnection(srcDB)
    srcEngine._metadata = MetaData(bind=srcEngine)
    srcEngine._metadata.reflect(srcEngine)  # get columns from existing table
    srcTable = Table(srcTbl, srcEngine._metadata)

    destEngine = getConnection(destDB)
    destEngine._metadata = MetaData(bind=destEngine)
    destTable = Table(destTbl, destEngine._metadata)

    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.create()


def addTblData(srcDB, srcTbl, destDB, destTbl):
    """
    srcDB: str of source database
    srcTbl: str of source table (in source database)
    destDB: str of destination database
    destTbl: str of destination table

    This function queries all data of a given source table and then appends this
    data to a destination table (in a destination database).
    """
    srcEngine = getConnection(srcDB)
    srcData = []

    with srcEngine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {srcTbl}"))
        for row in result:
            srcData.append(row)

    destEngine = getConnection(destDB)
    columnNames = getColumnNames(destDB, destTbl)[1:]

    with destEngine.connect() as conn:
        for row in srcData:
            row = row[1:]
            conn.execute(
                f"INSERT INTO {destTbl} {tuple(columnNames)} VALUES {row}")
            print(
                f"INSERT INTO {destTbl} {tuple(columnNames)} VALUES {row}")


def mergeDatabases(dbA, tblA, dbB, tblB, mergeDB):
    """
    This function creates a new table in a mergeDB using the addTblSchema 
    and addTblData functions, and then appends the data of the corresponding 
    destination table. This process is repeated for both tblA and tblB.
    """
    addTblSchema(dbA, tblA, mergeDB, tblA)
    addTblSchema(dbB, tblB, mergeDB, tblB)
    addTblData(dbA, tblA, mergeDB, tblA)
    addTblData(dbB, tblB, mergeDB, tblB)


dbA = 'sqlite:///database_A.sqlite'
dbB = 'sqlite:///database_B.sqlite'
tblA = 'table_A'
tblB = 'table_B'
mergeDB = 'sqlite:///merge_db.sqlite'

mergeDatabases(dbA, tblA, dbB, tblB, mergeDB)
