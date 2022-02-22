import os
import urllib
import urllib.parse
from dotenv import load_dotenv

load_dotenv()


host_server = os.environ.get('PG_HOST')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('PG_PORT')))
database_name = os.environ.get('PG_DB')
db_username = urllib.parse.quote_plus(str(os.environ.get('PG_USER_EXTRACT')))
db_password = urllib.parse.quote_plus(str(os.environ.get('PG_PASSWORD_EXTRACT')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
table = os.environ.get('PG_TABLE')

API_KEY=os.environ.get('API_KEY')

DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}?sslmode={}'.format(db_username,db_password, host_server, db_server_port, database_name, ssl_mode)
