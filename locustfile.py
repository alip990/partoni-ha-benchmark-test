import os
import time
import logging
import inspect

from locust import User, events, tag, task
from postgres_session import PostgresSession

# Import the database operation functions
from db_tasks import (
    create_schema,
    seed_data,
    write_data, 
    read_join,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
log_file = 'log.log'
file_handler = logging.FileHandler(log_file)

file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(formatter)
logger.info('This is an info message')


def custom_timer(func):
    """Measure execution time and send it to Locust events."""

    def func_wrapper(*args, **kwargs):
        previous_frame = inspect.currentframe().f_back
        (_, _, function_name, _, _) = inspect.getframeinfo(previous_frame)

        start_time = time.time()
        result = None
        try:
            result = func(*args, **kwargs)
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="TASK",
                name=func.__name__,
                response_time=total_time,
                response_length=0,
                tag=function_name
            )
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="TASK",
                name=func.__name__,
                response_time=total_time,
                response_length=0,
                exception=e,
                tag=function_name
            )
        return result

    return func_wrapper


class ComplexDBUser(User):
    """
    Represents a complex PostgreSQL user which executes various SQL queries.
    """

    PGHOST = os.getenv("PG_HOST", "localhost")
    PGPORT = int(os.getenv("PG_PORT", "5000"))
    PGDATABASE = os.getenv("PG_DATABASE", "db")
    PGUSER = os.getenv("PG_USER", "user")
    PGPASSWORD = os.getenv("PG_PASSWORD", "pass")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize the PostgresSession
        self.client = PostgresSession(
            host=self.PGHOST,
            port=self.PGPORT,
            database=self.PGDATABASE,
            user=self.PGUSER,
            password=self.PGPASSWORD,
            request_event=self.environment.events.request,
        )

    def on_start(self):
        """
        Called when a simulated user starts executing.
        """
        try:
            create_schema(self.client)
            seed_data(self.client)  
        except Exception as e:
            # You can handle the exception here if needed, or even stop the user
            self.environment.events.request.fire(
                request_type="TASK",
                name="on_start - create_schema",
                response_time=0,
                response_length=0,
                exception=e,
            )

    @tag('write_data')
    @task(2)
    @custom_timer
    def task_write_data(self):
        """
        Task to insert a new user into the database.
        """
        result = write_data(self.client)
        if not result.success:
            raise Exception("Failed to write data")


    @task(10)
    @custom_timer
    def task_read_with_join(self):
        """
        Task to perform a SELECT query with JOIN operations.
        """
        read_join(self.client)

    def on_stop(self):
        """
        Called when the user stops. Ensures that the database connection pool is closed.
        """
        self.client.close()
