from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base


def getConnection(dbPath: str):
    """
    dbPath: path to database
    return: engine object
    """
    return create_engine(dbPath, echo=True)


def createSampleTbl1(data):
    """
    data: List containing dicts, each entry in the dict specifiy one entry in 
    the database

    This function appends data to database_A.
    """
    engine = getConnection("sqlite:///database_A.sqlite")
    Base = declarative_base()

    class tableA(Base):
        __tablename__ = 'table_A'

        primaryKey_A = Column(Integer, primary_key=True, autoincrement=True)
        time_A = Column(Integer)
        text_A = Column(String(230))

    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        for row in data:
            conn.execute(text("INSERT INTO table_A (time_A, text_A) VALUES (:time, :text)"),
                         row
                         )


def createSampleTbl2(data):
    """
    data: List containing dicts, each entry in the dict specifiy one entry in 
    the database

    This function appends data to database_B.
    """
    engine = getConnection("sqlite:///database_B.sqlite")
    Base = declarative_base()

    class tableB(Base):
        __tablename__ = 'table_B'

        primaryKey_B = Column(Integer,  primary_key=True, autoincrement=True)
        time_B = Column(Integer)
        text_B = Column(String(230))
        moreText_B = Column(String(230))

    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        for row in data:
            conn.execute(text("INSERT INTO table_B (time_B, text_B, moreText_B) VALUES (:time, :text, :moreText)"),
                         row
                         )


# arbitrary sampel data
data_A = [
    {"time": 301, "text": "ABC"},
    {"time": 303, "text": "ABL"},
    {"time": 305, "text": "ADF"},
    {"time": 314, "text": "XSD"},
    {"time": 316, "text": "ACN"},
    {"time": 323, "text": "MSK"},
    {"time": 332, "text": "GLP"},
    {"time": 339, "text": "ESM"},
    {"time": 354, "text": "OKS"},
    {"time": 402, "text": "NSL"},
]


data_B = [
    {"time": 301, "text": "ABC", "moreText": "xyz"},
    {"time": 302, "text": "ABD", "moreText": "xya"},
    {"time": 305, "text": "ADF", "moreText": "xyd"},
    {"time": 311, "text": "ASD", "moreText": "lms"},
    {"time": 316, "text": "ACN", "moreText": "ysd"},
    {"time": 323, "text": "MSK", "moreText": "asd"},
    {"time": 332, "text": "GLP", "moreText": "lsk"},
    {"time": 334, "text": "AES", "moreText": "bnc"},
    {"time": 354, "text": "OKS", "moreText": "oek"},
    {"time": 402, "text": "NSL", "moreText": "suz"},
]

# call functions to create sqlite databases
createSampleTbl1(data_A)
createSampleTbl2(data_B)
