import random
import string
import datetime

def write_data(client):
    """
    Insert a new user into the database.
    """
    name = ''.join(random.choices(string.ascii_letters, k=8))
    location = ''.join(random.choices(string.ascii_letters, k=5))
    return client.execute_query(
        "INSERT INTO public.users (name, location) VALUES (%s, %s);",
        (name, location)
    )


def read_simple(client):
    """
    Perform a simple SELECT query.
    """
    return client.execute_query("""
        SELECT user_id, name, location, created_at
        FROM public.users
        WHERE user_id < %s;
    """, (5,))


def read_with_join(client):
    """
    Perform a SELECT query with JOIN operations.
    """
    return client.execute_query("""
        SELECT u.user_id, u.name, o.order_id, o.order_date, oi.product_name, oi.quantity
        FROM public.users u
        JOIN public.orders o ON u.user_id = o.user_id
        JOIN public.order_items oi ON o.order_id = oi.order_id
        WHERE o.total_amount > %s
        ORDER BY u.user_id
        LIMIT %s;
    """, (50, 10))
