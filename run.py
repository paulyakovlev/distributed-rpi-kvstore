from flask import Flask
from apis import api
from os import environ

if __name__ == '__main__':
    app = Flask(__name__)
    api.init_app(app)
    app.run(host='0.0.0.0', debug=True, port=8085)
