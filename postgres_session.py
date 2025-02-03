
import logging
import time
from typing import Any, List, Optional
import psycopg2
from psycopg2 import DatabaseError, OperationalError
from psycopg2 import pool
from locust import events  # Import Locust events for reporting

import psycogreen.gevent
psycogreen.gevent.patch_psycopg()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
log_file = 'log.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.ERROR)  # Capture all ERROR and above to the file
logger.addHandler(file_handler)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(formatter)
logger.info('This is an info message')


class PostgresResponse:
    def __init__(
        self,
        success: bool,
        response_time: float,
        exception: Optional[Exception],
        response_length: int,
        result: Optional[List[Any]] = None
    ):
        self.success = success
        self.response_time = response_time
        self.exception = exception
        self.response_length = response_length
        self.result = result or []

    def __str__(self):
        return f"success: {self.success}, result: {self.result}, exception: {self.exception}"


CONNECTION_LIMIT_RETRIES = 3


class PostgresSession:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        request_event,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.request_event = request_event
        self.connection = None
        self._cursor = None
        self.reconnect = True  # Define reconnect behavior
        self.init()

    def init(self):
        try:
            self.connect()
            self.cursor()
        except Exception as oe:
            logger.error(
                f"OperationalError during Initialize postgres session: {str(oe)}"
            )

    def connect(self):
        """
        Establish a connection to the PostgreSQL server with retry logic.
        Tracks downtime and retry attempts and reports them to Locust.
        """
        if not self.connection:
            try:
                start_time = time.time()
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.database,
                    user=self.user,
                    password=self.password,
                    connect_timeout=1,
                )
                self.connection.autocommit = True
                elapsed_time = (time.time() - start_time) * 1000  # milliseconds

                self.request_event.fire(
                    request_type="PG_QUERY",
                    name="CONNECT",
                    response_time=elapsed_time,
                    response_length=0,
                )
                logger.info(
                    f"Established connection to PostgreSQL at {self.host}:{self.port}"
                )
                return self.connection
            except (Exception, psycopg2.Error, psycopg2.OperationalError) as oe:
                elapsed_time = (time.time() - start_time) * 1000  # milliseconds

                self.request_event.fire(
                    request_type="PG_QUERY",
                    name="CONNECT",
                    response_time=elapsed_time,
                    response_length=0,
                    exception=oe,
                )

                logger.error(
                    f"OperationalError during connection attempt: {str(oe)}"
                )
                raise Exception('OperationalError during connection: ' + str(oe))
        return self.connection

    def reset(self):
        self.close()
        self.connect()
        self.cursor()

    def cursor(self):
        try:
            if not self._cursor or self._cursor.closed:
                if not self.connection:
                    self.connect()
                self._cursor = self.connection.cursor()
                return self._cursor
            return self._cursor
        except psycopg2.InterfaceError as e:
            logger.error(f"InterfaceError Connection Closed, closing connection: {e}")
            if self.connection:
                if self._cursor:
                    self._cursor.close()
                self.connection.close()
            self.connection = None
            self._cursor = None
            time.sleep(1)
            raise Exception('InterfaceError Connection Closed, closing connection: ' + str(e))

    def execute_query(self, query: str, params: Optional[tuple] = None, retry_counter=0) -> PostgresResponse:
        """
        Execute a SQL query with optional parameters.
        """
        try:
            start_time = time.time()
            cursor = self.cursor()
            cursor.execute(query, params)
            if cursor.description:
                result = cursor.fetchall()
                response_length = len(result)
            else:
                result = []
                response_length = cursor.rowcount
            response_time = (time.time() - start_time) * 1000  # milliseconds
            self.request_event.fire(
                request_type="PG_QUERY",
                name=query.split()[0].upper(),
                response_time=response_time,
                response_length=response_length,
            )
            return PostgresResponse(
                success=True,
                response_time=response_time,
                exception=None,
                response_length=response_length,
                result=result
            )
        except (OperationalError, DatabaseError, Exception, psycopg2.Error) as oe:
            self.request_event.fire(
                request_type="PG_QUERY",
                name=query.split()[0].upper(),
                response_time=0,
                response_length=0,
                exception=oe,
            )
            raise Exception(oe)

    def close(self):
        """
        Close the PostgreSQL connection.
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("Closed PostgreSQL connection.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.connection = None
