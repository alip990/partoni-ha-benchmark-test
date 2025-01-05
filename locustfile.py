import os
import random
import string
import datetime , time
import logging

from locust import User, events, tag, task, between
import inspect
from postgres_session import PostgresSession

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



def custom_timer(func):
    """ measure time and send to Locust """

    def func_wrapper(*args, **kwargs):
        """ wrap functions and measure time """

        previous_frame = inspect.currentframe().f_back
        (filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame)

        start_time = time.time()
        result = None
        try:
            result = func(*args, **kwargs)
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(request_type="TASK", name=func.__name__,
                                    response_time=total_time, response_length=0, tag=function_name)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(request_type="TASK", name=func.__name__,
                                        response_time=total_time,response_length=0 ,  exception=e, tag=function_name)
        return result

    return func_wrapper


class ComplexDBUser(User):
    """
    Represents a complex PostgreSQL "user" which executes various SQL queries against the database.
    """
    
    # Define the wait time between tasks
    # wait_time = between(1, 2)  # wait between 1 and 3 seconds

    # Database connection parameters sourced from environment variables
    PGHOST = os.getenv("PG_HOST", "localhost")
    PGPORT = int(os.getenv("PG_PORT", "5000"))  # Default PostgreSQL port is 5432
    PGDATABASE = os.getenv("PG_DATABASE", "db")
    PGUSER = os.getenv("PG_USER", "user")
    PGPASSWORD = os.getenv("PG_PASSWORD", "pass")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initialize the PostgresSession client with connection pooling
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
        Initializes the database schema and seeds initial data.
        """
        self._create_schema()
        # self._seed_data()

    def _create_schema(self):
        """
        Creates the necessary database schema and tables for load testing.
        """
        print('Creates the necessary database schema and tables for load testing')

        try:
            # self.client.execute_query('CREATE DATABASE test')
            self.client.execute_query("CREATE SCHEMA IF NOT EXISTS public;")
            logger.info('exec: CREATE SCHEMA IF NOT EXISTS public')
            self.client.execute_query("""
                CREATE TABLE IF NOT EXISTS public.users (
                    user_id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    location VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info('exec: CREATE TABLE IF NOT EXISTS public.users')
            self.client.execute_query("""
                CREATE TABLE IF NOT EXISTS public.orders (
                    order_id SERIAL PRIMARY KEY,
                    user_id INT REFERENCES public.users(user_id),
                    order_date DATE,
                    total_amount NUMERIC(10,2)
                );
            """)
            logger.info('exec: CREATE TABLE IF NOT EXISTS public.orders')
            self.client.execute_query("""
                CREATE TABLE IF NOT EXISTS public.order_items (
                    item_id SERIAL PRIMARY KEY,
                    order_id INT REFERENCES public.orders(order_id),
                    product_name VARCHAR(100),
                    quantity INT,
                    price NUMERIC(10,2)
                );
            """)
        except Exception as e:
            # Any exception here is critical; you might choose to stop the user or handle accordingly
            self.environment.events.request.fire(
                request_type="TASK",
                name="create_schema",
                response_time=0,
                response_length=0,
                exception=e,
            )
        logger.info('create schema finished')


    def _seed_data(self):
        """
        Seeds the database with initial data for load testing.
        """
        try:
            # Check if users table already has data
            response = self.client.execute_query("SELECT COUNT(*) FROM public.users;")
            count_users = response.response_length if response.success else 0

            if count_users == 0:
                # Insert users
                for _ in range(10):
                    name = ''.join(random.choices(string.ascii_letters, k=8))
                    location = ''.join(random.choices(string.ascii_letters, k=5))
                    self.client.execute_query(
                        "INSERT INTO public.users (name, location) VALUES (%s, %s);",
                        (name, location)
                    )

                # Insert orders
                for user_id in range(1, 11):
                    for _ in range(random.randint(1, 3)):
                        order_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 30))
                        total_amount = round(random.uniform(10, 200), 2)
                        self.client.execute_query(
                            "INSERT INTO public.orders (user_id, order_date, total_amount) VALUES (%s, %s, %s);",
                            (user_id, order_date, total_amount)
                        )

                # Insert order_items
                orders_response = self.client.execute_query("SELECT order_id FROM public.orders;")
                if orders_response.success and orders_response.response_length > 0:
                    order_ids = [row[0] for row in orders_response.response_length and orders_response.response_length or []]
                    for order_id in order_ids:
                        for _ in range(random.randint(1, 5)):
                            product_name = ''.join(random.choices(string.ascii_letters, k=6))
                            quantity = random.randint(1, 5)
                            price = round(random.uniform(1, 50), 2)
                            self.client.execute_query(
                                "INSERT INTO public.order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s);",
                                (order_id, product_name, quantity, price)
                            )
                else:
                    raise Exception("Failed to retrieve order IDs for seeding order_items.")
        except Exception as e:
            self.environment.events.request.fire(
                request_type="PG_QUERY",
                name="seed_data",
                response_time=0,
                exception=e,
            )
            raise

    @tag('write_data')
    @task(2)
    @custom_timer
    def write_data(self):
        """
        Task to insert a new user into the database.
        """
        name = ''.join(random.choices(string.ascii_letters, k=8))
        location = ''.join(random.choices(string.ascii_letters, k=5))
        result =self.client.execute_query(
            "INSERT INTO public.users (name, location) VALUES (%s, %s);",
            (name, location)
        )
        if not result.success:
            raise Exception()

    @task(3)
    def read_simple(self):
        """
        Task to perform a simple SELECT query.
        """
        self.client.execute_query("""
            SELECT user_id, name, location, created_at
            FROM public.users
            WHERE user_id < %s;
        """, (5,))

    @task(1)
    def read_with_join(self):
        """
        Task to perform a SELECT query with JOIN operations.
        """
        self.client.execute_query("""
            SELECT u.user_id, u.name, o.order_id, o.order_date, oi.product_name, oi.quantity
            FROM public.users u
            JOIN public.orders o ON u.user_id = o.user_id
            JOIN public.order_items oi ON o.order_id = oi.order_id
            WHERE o.total_amount > %s
            ORDER BY u.user_id
            LIMIT %s;
        """, (50, 10))

    def on_stop(self):
        """
        Called when the user stops. Ensures that the database connection pool is closed.
        """
        self.client.close()
