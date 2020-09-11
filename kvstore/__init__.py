from flask import Flask
from flask_restx import Api
from os import environ

app = Flask(__name__)
app.config['FORWARDING_ADDRESS'] = environ.get('FORWARDING_ADDRESS')
api = Api(app, title='Distributed KV Store', description='asdf')

from kvstore import routes
 