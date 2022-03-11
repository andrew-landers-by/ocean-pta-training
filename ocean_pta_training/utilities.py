import logging
import os
import pyodbc
from . import Environment

logger = logging.getLogger(f"{__name__}")


def pyodbc_connect(autocommit: bool = False) -> pyodbc.Connection:
    """TODO"""
    logger.info("Creating Synapse database connection...")
    driver = os.environ.get(Environment.Vars.ODBC_DRIVER)
    server = os.environ.get(Environment.Vars.SYNAPSE_SERVER)
    database = os.environ.get(Environment.Vars.SYNAPSE_DATABASE)
    uid = os.environ.get(Environment.Vars.SYNAPSE_UID)
    pwd = os.environ.get(Environment.Vars.SYNAPSE_PASSWORD)

    conn_string = f"Driver={driver};Server={server}; Database={database};Uid={uid}; Pwd={pwd};Encrypt=yes; TrustServerCertificate=no;Connection Timeout=30"
    conn = pyodbc.connect(conn_string)

    if autocommit:
        conn.autocommit = True

    return conn
