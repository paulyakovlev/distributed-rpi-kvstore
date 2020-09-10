
from flask import Flask, request, jsonify
from kvstore import app
import os
import requests

# our key-value store
keyStore = {}

# find out if we're a forwarding container
isForwarding = False
if "FORWARDING_ADDRESS" in os.environ:
    fwdAddress = os.environ['FORWARDING_ADDRESS']
    isForwarding = True

# /key-value-store endpoint
@app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def keyValueStore(key):

    # handle forwarding
    # if we're a forwarding instance, don't process request and just send it to main
    if isForwarding:
        print('Forwarding container received %s' % request.method, flush=True)
        # forward request to main, based onthe FORWARDING_ADDRESS we have
        url = 'http://' + fwdAddress + '/key-value-store/' + key
        try:
            # forward GET
            if(request.method == 'GET'):
                r = requests.get(url=url, timeout=2.5)
                response = r.json()
                statusCode = r.status_code
            # forward PUT
            if(request.method == 'PUT'):
                r = requests.put(url=url, json=request.json, timeout=2.5)
                response = r.json()
                statusCode = r.status_code
            # forward DELETE
            if(request.method == 'DELETE'):
                r = requests.delete(url=url, timeout=2.5)
                response = r.json()
                statusCode = r.status_code
        except requests.exceptions.ConnectionError:
            print('Connection error', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            statusCode = 503
        # if request to main takes too long then we assume its down
        except requests.Timeout:
            print('Request to main timed out', flush=True)
            response = {
                'error': 'Main instance is down',  'message': 'Error in %s' % request.method}
            statusCode = 503
    else:
        print('Main container received %s' % request.method, flush=True)
        # GET
        if(request.method == 'GET'):
            if(key in keyStore):
                response = {
                    'doesExist': True, 'message': 'Retrieved successfully', 'value': keyStore[key]}
                statusCode = 200
            else:
                response = {
                    'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in GET'}
                statusCode = 404

        # PUT
        if(request.method == 'PUT'):
            if('value' in request.json):
                if (len(key) > 50):
                    response = {
                        'error': 'Key is too long', 'message': 'Error in PUT'}
                    statusCode = 400
                else:
                    if (key in keyStore):
                        keyStore[key] = request.json['value']
                        response = {
                            'message': 'Updated successfully', 'replaced': True}
                        statusCode = 200
                    else:
                        keyStore[key] = request.json['value']
                        response = {'message': 'Added successfully',
                                    'replaced': False}
                        statusCode = 201
            else:
                response = {'error': 'Value is missing',
                            'message': "Error in PUT"}
                statusCode = 400

        # DELETE
        if(request.method == 'DELETE'):
            if (key in keyStore):
                del keyStore[key]
                response = {
                    'doesExist': True, 'message': 'Deleted successfully'}
                statusCode = 200
            else:
                response = {
                    'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in DELETE'}
                statusCode = 404

    return jsonify(response), statusCode