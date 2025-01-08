import logging
import time
from typing import Any, List, Optional
import psycopg2
from psycopg2 import DatabaseError, OperationalError
from psycopg2 import pool
from locust import events  # Import Locust events for reporting

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
        self.connect()
        self.cursor()

    def connect(self, retry_counter=0):
        """
        Establish a connection to the PostgreSQL server with retry logic.
        Tracks downtime and retry attempts and reports them to Locust.
        """
        if not self.connection:
            total_downtime = 0  # Total downtime in milliseconds
            retry_attempts = 0

            while retry_counter <= CONNECTION_LIMIT_RETRIES:
                try:
                    start_time = time.time()
                    self.connection = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        dbname=self.database,
                        user=self.user,
                        password=self.password,
                        connect_timeout=3,
                    )
                    self.connection.autocommit = True
                    elapsed_time = (time.time() - start_time) * 1000  # milliseconds
                    total_downtime += elapsed_time

                    self.request_event.fire(
                        request_type="PG_QUERY",
                        name="CONNECT",
                        response_time=elapsed_time,
                        response_length=0,
                    )
                    logger.info(
                        f"Established connection to PostgreSQL at {self.host}:{self.port}"
                    )

                    # Report reconnection metrics to Locust
                    if retry_attempts > 0:
                        events.request_success.fire(
                            request_type="DB_RECONNECT",
                            name="Reconnect",
                            response_time=total_downtime,
                            response_length=retry_attempts
                        )
                        logger.info(
                            f"Reconnected to PostgreSQL after {retry_attempts} attempts with total downtime {total_downtime:.2f} ms"
                        )

                    return self.connection

                except psycopg2.OperationalError as oe:
                    elapsed_time = (time.time() - start_time) * 1000  # milliseconds
                    total_downtime += elapsed_time
                    retry_attempts += 1
                    retry_counter += 1

                    self.request_event.fire(
                        request_type="PG_QUERY",
                        name="CONNECT",
                        response_time=elapsed_time,
                        response_length=0,
                        exception=oe,
                    )

                    logger.error(
                        f"OperationalError during connection attempt {retry_attempts}: {oe}"
                    )

                    if not self.reconnect or retry_counter > CONNECTION_LIMIT_RETRIES:
                        logger.error(
                            f"OperationalError reconnection retry exceeded after {retry_attempts} attempts: {oe}"
                        )

                        # Report failure to Locust
                        events.request.fire(
                            request_type="DB_RECONNECT",
                            name="Reconnect",
                            response_time=total_downtime,
                            response_length=0,
                            exception=oe
                        )


                        raise oe
                    else:
                        logger.info(
                            f"Retrying connection in 2 seconds... (Attempt {retry_attempts}/{CONNECTION_LIMIT_RETRIES})"
                        )
                        time.sleep(2)

                except (Exception, psycopg2.Error) as error:
                    logger.error(f"Unknown error during connect: {error}")
                    self.request_event.fire(
                        request_type="PG_QUERY",
                        name="CONNECT",
                        response_time=0,
                        response_length=0,
                        exception=error,
                    )

                    # Report failure to Locust
                    events.request.fire(
                        request_type="DB_RECONNECT",
                        name="Reconnect",
                        response_time=0,
                        response_length=0,
                        exception=error
                    )

                    self.connection = None
                    break  # Exit the loop on unknown errors

        return self.connection

    def reset(self):
        self.close()
        self.connect()
        self.cursor()

    def cursor(self):
        if not self._cursor or self._cursor.closed:
            if not self.connection:
                self.connect()
            self._cursor = self.connection.cursor()
            return self._cursor

    def execute_query(self, query: str, params: Optional[tuple] = None, retry_counter=0) -> PostgresResponse:
        """
        Execute a SQL query with optional parameters.
        """
        try:
            start_time = time.time()
            self._cursor.execute(query, params)
            if self._cursor.description:
                result = self._cursor.fetchall()
                response_length = len(result)
            else:
                result = []
                response_length = self._cursor.rowcount
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
        except (OperationalError, DatabaseError) as oe:
            if retry_counter >= 5:
                exception = OperationalError(
                    "Failed to establish connection: " + str(oe))
                self.request_event.fire(
                    request_type="PG_QUERY",
                    name=query.split()[0].upper(),
                    response_time=0,
                    response_length=0,
                    exception=exception,
                )

                # Report failure to Locust
                events.request.fire(
                    request_type="DB_RECONNECT",
                    name="Reconnect",
                    response_time=0,
                    response_length=0,
                    exception=exception
                )

                return PostgresResponse(
                    success=False,
                    response_time=0,
                    exception=exception,
                    response_length=0,
                    result=[]
                )
            else:
                retry_counter += 1
                time.sleep(1)
                self.reset()
                return self.execute_query(query, params, retry_counter)
        except (Exception, psycopg2.Error) as error:
            exception = OperationalError("Unknown error connection: " + str(error))
            self.request_event.fire(
                request_type="PG_QUERY",
                name=query.split()[0].upper(),
                response_time=0,
                response_length=0,
                exception=exception,
            )

            # Report failure to Locust
            events.request.fire(
                request_type="DB_RECONNECT",
                name="Reconnect",
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
