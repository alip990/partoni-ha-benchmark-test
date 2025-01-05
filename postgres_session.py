from psycopg2 import DatabaseError, OperationalError, pool
import logging
import time
# Configure logging
from typing import Any, List, Optional
import psycopg2
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
log_file = 'log.log'
file_handler = logging.FileHandler(log_file)

file_handler.setLevel(logging.ERROR)  # Capture all levels to the file

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
        self.connect()
    
    def connect(self):
        """
        Establish a connection to the PostgreSQL server.
        """
        try:
            start_time = time.time()
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=5 ,
            )
            self.connection.autocommit = True
            response_time = (time.time() - start_time) * 1000  # milliseconds
            self.request_event.fire(
                request_type="PG_QUERY",
                name="CONNECT",
                response_time=response_time,
                response_length=0,
            )

            logger.info(f"Established connection to PostgreSQL at {self.host}:{self.port}")
        except OperationalError as oe:
            logger.error(f"OperationalError during connection: {oe}")
            response_time = (time.time() - start_time) * 1000  # milliseconds
            self.request_event.fire(
                request_type="PG_QUERY",
                name="CONNECT",
                response_time=response_time,
                response_length=0,
                exception=oe,
            )
            self.connection = None
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            self.request_event.fire(
                request_type="PG_QUERY",
                name="CONNECT",
                response_time=0,
                response_length=0,
                exception=e,
            )
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> PostgresResponse:
        """
        Execute a SQL query with optional parameters.
        """
        if not self.connection:
            # Attempt to reconnect
            logger.info("No active connection. Attempting to reconnect...")
            self.connect()
            if not self.connection:
                # Connection failed
                exception = OperationalError("Failed to establish connection")
                self.request_event.fire(
                    request_type="PG_QUERY",
                    name="RECONNECT_FAILED",
                    response_time=0,
                    response_length=0,
                    exception=exception,
                )
                return PostgresResponse(
                    success=False,
                    response_time=0,
                    exception=exception,
                    response_length=0,
                    result=[]
                )
        
        start_time = time.time()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    result = cursor.fetchall()
                    response_length = len(result)
                else:
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
                result=result if cursor.description else []
            )
        except (OperationalError, DatabaseError) as e:
            response_time = (time.time() - start_time) * 1000
            logger.error(f"Database error during query '{query.split()[0].upper()}': {e}")
            self.request_event.fire(
                request_type="PG_QUERY",
                name=query.split()[0].upper(),
                response_time=response_time,
                response_length=0,
                exception=e,
            )
            # Attempt to reconnect on next query
            self.connection = None
            return PostgresResponse(
                success=False,
                response_time=response_time,
                exception=e,
                response_length=0,
                result=[]
            )
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            logger.error(f"Unexpected error during query '{query.split()[0].upper()}': {e}")
            self.request_event.fire(
                request_type="PG_QUERY",
                name=query.split()[0].upper(),
                response_time=response_time,
                response_length=0,
                exception=e,
            )
            return PostgresResponse(
                success=False,
                response_time=response_time,
                exception=e,
                response_length=0,
                result=[]
            )
    
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

