from flask import Flask, request, jsonify, make_response
from flask_restx import Namespace, Resource, fields
from functools import wraps
import os
import requests

# our key-value store
key_store = {}

# find out if we're a forwarding container
is_forwarding = False
if "FORWARDING_ADDRESS" in os.environ:
    forwarding_address = os.environ['FORWARDING_ADDRESS']
    is_forwarding = True

# set namespace
api = Namespace('kvstore', description='Key-value store related operations')

key_value = api.model('Value', {"value": fields.String(
    required=True, description="The task details")})


def forward_request(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if is_forwarding:
            print('Forwarding container received %s' %
                  request.method, flush=True)
            # forward request to main, based onthe FORWARDING_ADDRESS we have
            url = 'http://' + forwarding_address + \
                '/key-value-store/' + kwargs['key']
            try:
                # forward GET
                if(request.method == 'GET'):
                    r = requests.get(url=url, timeout=2.5)
                    response = r.json()
                    status_code = r.status_code
                if(request.method == 'PUT'):
                    r = requests.put(url=url, json=request.json, timeout=2.5)
                    response = r.json()
                    status_code = r.status_code
                if(request.method == 'DELETE'):
                    r = requests.delete(url=url, timeout=2.5)
                    response = r.json()
                    status_code = r.status_code

            except requests.exceptions.ConnectionError:
                print('Connection error', flush=True)
                response = {
                    'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
                status_code = 503

            except requests.Timeout:
                print('Request to main timed out', flush=True)
                response = {
                    'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
                status_code = 503

            return make_response(jsonify(response), status_code)
        else:
            return f(*args, **kwargs)
    return decorated_function


@api.route('/key-value-store/<string:key>')
class KeyStore(Resource):
    @forward_request
    def get(self, key):
        print('Main container received %s' % request.method, flush=True)

        if(key in key_store):
            response = {
                'doesExist': True, 'message': 'Retrieved successfully', 'value': key_store[key]}
            status_code = 200
        else:
            response = {
                'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in GET'}
            status_code = 404

        return make_response(jsonify(response), status_code)

    @forward_request
    @api.expect(key_value)
    def put(self, key):
        print('Main container received %s' % request.method, flush=True)

        if('value' in request.json):
            if (len(key) > 50):
                response = {
                    'error': 'Key is too long', 'message': 'Error in PUT'}
                status_code = 400
            else:
                if (key in key_store):
                    key_store[key] = request.json['value']
                    response = {
                        'message': 'Updated successfully', 'replaced': True}
                    status_code = 200
                else:
                    key_store[key] = request.json['value']
                    response = {'message': 'Added successfully',
                                'replaced': False}
                    status_code = 201
        else:
            response = {'error': 'Value is missing',
                        'message': "Error in PUT"}
            status_code = 400

        return make_response(jsonify(response), status_code)

    @forward_request
    def delete(self, key):
        print('Main container received %s' % request.method, flush=True)

        if (key in key_store):
            del key_store[key]
            response = {
                'doesExist': True, 'message': 'Deleted successfully'}
            status_code = 200
        else:
            response = {
                'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in DELETE'}
            status_code = 404

        return make_response(jsonify(response), status_code)
