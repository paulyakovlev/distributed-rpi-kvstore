from flask import Flask
from kvstore import api
from os import environ

if __name__ == '__main__':
    app = Flask(__name__)
    api.init_app(app)
    app.config['FORWARDING_ADDRESS'] = environ.get('FORWARDING_ADDRESS')
    app.run(host='0.0.0.0', debug=True, port=8085)
