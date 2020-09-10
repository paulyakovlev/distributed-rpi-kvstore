from flask import Flask

app = Flask(__name__)

from kvstore import routes