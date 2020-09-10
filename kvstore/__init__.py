from flask import Flask
from os import environ

app = Flask(__name__)
app.config['FORWARDING_ADDRESS'] = environ.get('FORWARDING_ADDRESS')

from kvstore import routes