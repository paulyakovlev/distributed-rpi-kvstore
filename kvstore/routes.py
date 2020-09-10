
from flask import Flask, request, jsonify
from kvstore import app
import os
import requests

# our key-value store
key_store = {}

# find out if we're a forwarding container
is_forwarding = False
if app.config['FORWARDING_ADDRESS'] is not None:
    forwarding_address = app.config['FORWARDING_ADDRESS']
    is_forwarding = True

# /key-value-store GET request
@app.route('/key-value-store/<key>', methods=['GET'])
def get_key(key):
    if is_forwarding:
        print('Forwarding container received %s' % request.method, flush=True)
        url = 'http://' + forwarding_address + '/key-value-store/' + key

        try:
            r = requests.get(url=url, timeout=2.5)
            response = r.json()
            status_code = r.status_code
        except requests.exceptions.ConnectionError:
            print('Connection error', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
        # if request to main takes too long then we assume its down
        except requests.Timeout:
            print('Request to main timed out', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
    else:
        print('Main container received %s' % request.method, flush=True)

        if(key in key_store):
            response = {
                'doesExist': True, 'message': 'Retrieved successfully', 'value': key_store[key]}
            status_code = 200
        else:
            response = {
                'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in GET'}
            status_code = 404
            
    return jsonify(response), status_code

# /key-value-store PUT request
@app.route('/key-value-store/<key>', methods=['PUT'])
def add_or_update_key(key):
    if is_forwarding:
        print('Forwarding container received %s' % request.method, flush=True)
        url = 'http://' + forwarding_address + '/key-value-store/' + key

        try:
            r = requests.put(url=url, json=request.json, timeout=2.5)
            response = r.json()
            status_code = r.status_code
        except requests.exceptions.ConnectionError:
            print('Connection error', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
        # if request to main takes too long then we assume its down
        except requests.Timeout:
            print('Request to main timed out', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
    else:
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
            
    return jsonify(response), status_code

# /key-value-store endpoint
@app.route('/key-value-store/<key>', methods=['DELETE'])
def delete_key(key):
    if is_forwarding:
        print('Forwarding container received %s' % request.method, flush=True)
        url = 'http://' + forwarding_address + '/key-value-store/' + key

        try:
            r = requests.delete(url=url, timeout=2.5)
            response = r.json()
            status_code = r.status_code
        except requests.exceptions.ConnectionError:
            print('Connection error', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
        # if request to main takes too long then we assume its down
        except requests.Timeout:
            print('Request to main timed out', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            status_code = 503
    else:
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
            
    return jsonify(response), status_code