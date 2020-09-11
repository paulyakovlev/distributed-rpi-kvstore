from flask import Flask
from flask_restx import Api
from os import environ
from .kvstore import api as ns1

api = Api(title='Distributed KV Store', description='asdf')
api.add_namespace(ns1)
