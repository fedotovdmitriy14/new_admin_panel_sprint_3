import os

from dotenv import load_dotenv

load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}


redis_config = {
        'host': os.environ.get('REDIS_HOST'),
        'port': os.environ.get('REDIS_PORT'),
}
