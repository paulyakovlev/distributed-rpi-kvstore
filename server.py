
from flask import Flask, request, jsonify, make_response
from flask_script import Manager, Server
import threading
import vector_functions as vf
import partition_functions as pf
import os
import requests
import time

app = Flask(__name__)
key_store = {}

# debugging variables
broadcast_count = 0
retry_attempts = 0

# each replica maintains a vector clock of size n (n: # of nodes in network)
vector_clock = []

print("VECTOR CLOCK: %s" % vector_clock, flush=True)
is_new_node = True

# for testing without docker, will get overwritten if run with docker-compose
socket_address = "10.0.0.3:8085"
view = ["10.0.0.2:8085", "10.0.0.3:8085", "10.0.0.4:8085"]

# ip address and port of replica
if "SOCKET_ADDRESS" in os.environ:
    socket_address = os.environ['SOCKET_ADDRESS']

# socket addresses of all replicas
if "VIEW" in os.environ:
    view = os.environ['VIEW'].replace('"', '').split(",")
    # load the initial vector clock with zeroes
    for item in view:
        vector_clock.append(0)

# new nodes get spun up without SHARD_COUNT environment variable
shard_count = 0
if "SHARD_COUNT" in os.environ:
    shard_count = int(os.environ['SHARD_COUNT'].strip('\"'))
    # if we have SHARD_COUNT, then we must be part of the original spun up nodes
    # new nodes broadcast their existence across shard on boot
    # original nodes figure out their shard partition on boot
    is_new_node = False

# Make list of shard ids [1,2,3,4..]
shard_ids = list(range(1, int(shard_count) + 1))

# shard_members = { "1" : ["10.0.0.2:8085", "10.0.0.3:8085", "10.0.0.4:8085"], "2" : ["10.10.0.5:8085","10.10.0.6:8085","10.10.0.7:8085"] }
# dict of all shards and which nodes belong to them
shard_members = {}

# dict of all nodes and which shard they belong to
# node_shard_ids = {node : [shard_id], ... }
node_shard_ids = {}

thread = threading.Thread()


def initialize_node():
    global thread, shard_members, view, shard_count, shard_ids

    # if new node, let other nodes know
    if (is_new_node):
        print(
            "I am a new node! Broadcasting viewchange PUT in background thread", flush=True)
        threading.Thread(target=broadcast_view_change_put())
        thread.start()

    # if original node, organize each other into shards
    else:
        print(
            "I am an original node! Partitioning nodes into shards", flush=True)
        total_nodes = len(view)
        capacity_of_shards = total_nodes // shard_count

        # partition nodes into shards
        current_shard = 1
        for current_node in range(1, total_nodes + 1):
            if current_shard not in shard_members:
                shard_members[current_shard] = []

            shard_members[current_shard].append(view[current_node-1])
            node_shard_ids[view[current_node-1]] = current_shard

            # If we are not in the last shard and it's not time to move to the next shard
            if(current_shard != shard_count and current_node % capacity_of_shards == 0):
                current_shard += 1

    print("shard_members: %s" % str(shard_members), flush=True)


def broadcast_view_change_put():
    global socket_address, view, vector_clock, key_store

    got_added = False
    get_address = ""

    for address in view:
        data = {'socket-address': socket_address}
        if (address == socket_address):
            continue
        else:
            # set target url
            url = 'http://' + address + '/key-value-store-view'
            print('broadcasting view change PUT...', flush=True)
            try:
                print('Sending PUT view change request to %s' %
                      address, flush=True)
                r = requests.put(url=url, json=data, timeout=5)
                response = r.json()
                statusCode = r.status_code
            # if request doesn't go through then assume replica is down and tell the others
            except requests.exceptions.ConnectionError:
                print('Connection error', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # if response from replica takes too long then we assume its down and tell the others
            except requests.Timeout:
                print('Request timed out', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # TODO: do we need to do something with the response and status code?
            print('Response from replica %s: %s, %s' %
                  (address, statusCode, response), flush=True)

            if (statusCode == 201):
                got_added = True
                get_address = address
                print(
                    '%s accepted my socket-address, will grab data from it later' % address, flush=True)
    # if (got_added == True):
        # get_latest_data(get_address)


@app.route('/get_latest_data/<address>', methods=['GET'])
def get_latest_data(address):
    global vector_clock, key_store
    print('Grabbing latest data from %s!' % address, flush=True)
    url = 'http://' + address + '/key-value-store'

    # grab vector clock and key-store
    print('getting key store and vector clock data from %s!' %
          address, flush=True)
    r = requests.get(url=url, timeout=5)
    response = r.json()
    vector_clock = response['causal-metadata']
    key_store = response['key-store']
    print('vector_clock %s!' % str(vector_clock), flush=True)
    print('key_store %s!' % str(key_store), flush=True)

    # grab shard ids
    print('Grabbing shard ids from %s!' % address, flush=True)
    url = 'http://' + address + '/key-value-store-shard/shard-ids'
    r = requests.get(url=url, timeout=5)
    response = r.json()
    shard_ids = response['shard-ids']
    print('shard_ids %s!' % str(shard_ids), flush=True)

    # grab shard members
    print('Grabbing shard members from %s!' % address, flush=True)
    for id in shard_ids:
        url = 'http://' + address + \
            '/key-value-store-shard/shard-id-members/' + str(id)
        r = requests.get(url=url, timeout=5)
        response = r.json()
        shard_members[id] = response["shard-id-members"]
    print('shard_members %s!' % str(shard_members), flush=True)

    # populate node_shard ids
    print('Populating node shard ids!', flush=True)
    for shard in shard_members:
        for address in shard_members[shard]:
            node_shard_ids[address] = shard
    print('node_shard_ids %s!' % str(node_shard_ids), flush=True)

    return response


def broadcast_kv_store(key, request):
    global socket_address, view, vector_clock
    my_shard = node_shard_ids[socket_address]

    downed_addresses = []
    print('***IN BROADCAST***', flush=True)
    print('view: %s' %
          view, flush=True)
    # forward request to every replica except ourselves
    for address in shard_members[my_shard]:
        if (address == socket_address):
            continue
        else:
            # set target url
            url = 'http://' + address + '/key-value-store/' + key
            try:
                if(request.method == 'PUT'):
                    print('Forwarding PUT to %s from %s' %
                          (address, socket_address), flush=True)
                    print('FORWARDING TO THESE ADDRESSES: %s' %
                          shard_members[my_shard], flush=True)
                    r = requests.put(url=url, json=request.json, timeout=5)
                    response = r.json()
                    statusCode = r.status_code
                if(request.method == 'DELETE'):
                    print('Forwarding DELETE to %s from %s' %
                          (address, socket_address), flush=True)
                    r = requests.delete(url=url, timeout=5)
                    response = r.json()
                    statusCode = r.status_code
            # if request doesn't go through then assume replica is down and tell the others
            except requests.exceptions.ConnectionError:
                print('Connection error', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                downed_addresses.append(address)
            # if response from replica takes too long then we assume its down and tell the others
            except requests.Timeout:
                print('Request timed out', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                downed_addresses.append(address)
            # TODO: do we need to do something with the response and status code?
            print('Response from replica %s: %s, %s' %
                  (address, statusCode, response), flush=True)

    print("REPLICAS THAT WERE DOWN: %s" % str(downed_addresses), flush=True)
    for item in downed_addresses:
        broadcast_view_change_delete(item)


def broadcast_view_change_delete(downed_address):
    print('***IN DELETE BROADCAST***', flush=True)
    print('view: %s' %
          view, flush=True)
    data = {'socket-address': downed_address}

    # forward request to every replica (including ourselves), except the one that's down
    for address in view:
        if (address == downed_address):
            continue
        else:
            # set target url
            url = 'http://' + address + '/key-value-store-view/'
            print('broadcasting view change delete...', flush=True)
            try:
                print('Sending DELETE view change request to %s' %
                      address, flush=True)
                r = requests.delete(url=url, json=data, timeout=5)
                response = r.json()
                statusCode = r.status_code
            # if request doesn't go through then assume replica is down and tell the others
            except requests.exceptions.ConnectionError:
                print('Connection error', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # if response from replica takes too long then we assume its down and tell the others
            except requests.Timeout:
                print('Request timed out', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # TODO: do we need to do something with the response and status code?
            print('Response from replica %s: %s, %s' %
                  (address, statusCode, response), flush=True)


def broadcast_vector_clock_update():
    # forward request to every replica, except ourselves and in our
    for address in view:
        my_shard = node_shard_ids[socket_address]

        if (node_shard_ids[address] == my_shard):
            continue
        else:
            # set target url
            url = 'http://' + address + '/vector-clock-update'
            # send get request to vector clock udate
            try:
                print('Sending vector clock update request to %s' %
                      address, flush=True)
                r = requests.get(url=url, timeout=5)
                response = r.json()
                statusCode = r.status_code
            # if request doesn't go through then assume replica is down and tell the others
            except requests.exceptions.ConnectionError:
                print('Connection error', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # if response from replica takes too long then we assume its down and tell the others
            except requests.Timeout:
                print('Request timed out', flush=True)
                response = {
                    'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
                statusCode = 503
                # broadcast_view_change_delete(address)
            # TODO: do we need to do something with the response and status code?
            print('Response from replica %s: %s, %s' %
                  (address, statusCode, response), flush=True)

# /vector-clock-update endpoint
# TODO: broadcast view changes for PUT and DELETE methods
@app.route('/vector-clock-update', methods=['GET'])
def vector_clock_update():
    global vector_clock
    sender_address = request.remote_addr + ":8085"
    print("vector clock update request from %s, updating their vc position in my clock" %
          sender_address, flush=True)

    if(request.method == 'GET'):
        vector_clock = vf.increment_clock(
            sender_address, view, vector_clock)
        response = {'message': 'Vector clock updated', }
        statusCode = 200

    return make_response(jsonify(response), statusCode)


@app.route('/key-value-store', methods=['GET'])
def keyValueCopy():
    if(request.method == 'GET'):
        response = {'message': 'Retrieved successfully',
                    'causal-metadata': vector_clock, 'key-store': key_store}
        statusCode = 200

    return make_response(jsonify(response), statusCode)


@app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def keyValueStore(key):
    global key_store, vector_clock, socket_address, view, broadcast_count, retry_attempts
    my_shard = node_shard_ids[socket_address]
    sender_address = request.remote_addr + ":8085"

    # Check if request is from inside or outside shard
    is_request_from_shard_member = (
        sender_address in shard_members[my_shard])

    print('%s received %s from %s' %
          (socket_address, request.method, sender_address), flush=True)
    # hash the socket address to determine shard
    target_shard = pf.get_key_location(key, shard_count)

    print("Key: *%s*, shard_count: %s" %
          (key, shard_count), flush=True)
    print("Target shard: %s, My shard: %s" %
          (target_shard, my_shard), flush=True)

    if (target_shard != my_shard):
        print('Wot r ye doan in my shard?!?!', flush=True)
        target_node = shard_members[target_shard][0]
        url = 'http://' + target_node + '/key-value-store/' + key
        try:
            if(request.method == 'PUT'):
                print('Forwarding PUT to %s from %s' %
                      (target_node, socket_address), flush=True)
                r = requests.put(url=url, json=request.json, timeout=5)
                response = r.json()
                statusCode = r.status_code
            if(request.method == 'DELETE'):
                print('Forwarding DELETE to %s from %s' %
                      (target_node, socket_address), flush=True)
                r = requests.delete(url=url, timeout=5)
                response = r.json()
                statusCode = r.status_code
            if(request.method == 'GET'):
                print('Forwarding GET to %s from %s' %
                      (target_node, socket_address), flush=True)
                r = requests.get(url=url, timeout=5)
                response = r.json()
                statusCode = r.status_code
            # if request doesn't go through then assume replica is down and tell the others
        except requests.exceptions.ConnectionError:
            print('Connection error', flush=True)
            response = {
                'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
            statusCode = 503
           # downed_addresses.append(address)
        # if response from replica takes too long then we assume its down and tell the others
        except requests.Timeout:
            print('Request timed out', flush=True)
            response = {
                'error': 'Replica is down',  'message': 'Error in %s' % request.method, 'causal-metadata': ''}
            statusCode = 503
            # downed_addresses.append(address)
            # TODO: do we need to do something with the response and status code?
            print('Response from replica %s: %s, %s' %
                  (target_node, statusCode, response), flush=True)

    else:
        print('Request belongs in my shard', flush=True)
        # GET
        if(request.method == 'GET'):
            if(key in key_store):
                response = {'message': 'Retrieved successfully',
                            'causal-metadata': vector_clock, 'value': key_store[key]}
                statusCode = 200
            else:
                response = {
                    'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in GET'}
                statusCode = 404

        # PUT and DELETE
        if(request.method == 'PUT' or request.method == 'DELETE'):
            # if causal meta data included
            if('causal-metadata' in request.json):
                if(request.json['causal-metadata'] == ""):
                    metadata = []
                else:
                    metadata = request.json['causal-metadata']
                    print('causal-metadata: %s' % metadata, flush=True)

                # check for causal consistency
                if (metadata <= vector_clock):

                    # TODO:
                    # 1. Confirm causal matadate is good and we can serve the request`
                    # 2. Run hashmod and find out which shard this request should go to
                    # 3. If belongs to our shard, process request
                    # 4. If belongs to other shard, forward request to first node in that shard

                    if(request.method == 'PUT'):
                        if('value' in request.json):
                            if (len(key) > 50):
                                response = {
                                    'error': 'Key is too long', 'message': 'Error in PUT'}
                                statusCode = 400
                            else:
                                if (key in key_store):
                                    key_store[key] = request.json['value']
                                    response = {
                                        'message': 'Updated successfully',
                                        'causal-metadata': vector_clock, 'shard-id': my_shard}
                                    statusCode = 200
                                else:
                                    # TODO: remove following vector clock code:
                                    # add a new item to vector clock for new key
                                    # vc_position = len(vector_clock)
                                    # vector_clock.append(0)

                                    key_store[key] = request.json['value']
                                    response = {'message': 'Added successfully',
                                                'causal-metadata': vector_clock, 'shard-id': my_shard}
                                    statusCode = 201

                                print('updated key store: %s' %
                                      str(key_store), flush=True)

                    # DELETE
                    if(request.method == 'DELETE'):
                        if (key in key_store and key_store[key] is not None):
                            key_store[key] = None
                            response = {
                                'doesExist': True, 'message': 'Deleted successfully'}
                            statusCode = 200

                            # TODO: change how we increment vector clock
                            vector_clock = vf.increment_clock(
                                key, key_store, vector_clock)
                        else:
                            response = {
                                'doesExist': False, 'error': 'Key does not exist', 'message': 'Error in DELETE'}
                            statusCode = 404

                    # TODO: change how we increment vector clock
                    # 1. If we are first node to serve request, increment our vector clock position
                    # 2. If we got this request forwarded from another node, increment that node's vector clock position
                    # 3. Basically, don't increment our vc position unless we are the first ones to serve the request

                    # TODO: Change following if statement to:
                    # 1. If request didn't come from one of our shard members AND it belongs in our shard, broadcast to our shard members.
                    # 2. Then broadcast the new vector clock to every node NOT in our shard group
                    # 2a.Vector clock broadcast can be a simple ping, like a GET to secret endpoint.
                    #     When a node receives a request on this endpoint, increment the vector clock on the sendee's position
                    # Maybe move inside the logic where we've already determined it belongs in our shard

                    # only broadcast if request came from outside our shard
                    # otherwise this will trigger an infinite broadcast loop
                    if(is_request_from_shard_member and (socket_address != sender_address)):
                        # increment the sender node's vector clock
                        print(
                            "request from shard member, ima chill", flush=True)
                        vector_clock = vf.increment_clock(
                            sender_address, view, vector_clock)
                    else:
                        # Increment our own vector clock if it's an outside request or we sent it to ourselves
                        print(
                            "request not from shard member, broadcasting to shard homies and incrementing my own clock", flush=True)
                        vector_clock = vf.increment_clock(
                            socket_address, view, vector_clock)

                        broadcast_kv_store(key, request)
                        broadcast_vector_clock_update()
                        broadcast_count += 1

                # if not causally consistent, queue up this request and wait for other requests to get us caught up
                else:
                    print('%s request with metadata %s not causally consistent - retrying in 5s...' % (
                        request.method, metadata), flush=True)

                    # Not causally consistent, wait 5 for a broadcast to come in
                    # number of retry attempts not to exceed 1
                    if(retry_attempts < 1):
                        print('Retrying...', flush=True)
                        time.sleep(5)
                        # retry once
                        retry_attempts += 1
                        r = keyValueStore(key)
                        # look through view, and for each replica do a get to see if they can respond

                        response = r.get_json()
                        statusCode = r.status_code
                        retry_attempts = 0
                    # if we've already retried once before, ask the other replicas what you're missing
                    elif(retry_attempts == 999):
                        print(
                            'No more retries left...client causal metadata must be invalid', flush=True)
                        response = {"message": "invalid causal metadata"}
                        statusCode = 404
                    else:
                        for address in view:
                            if (address == socket_address):
                                continue
                            else:
                                print(
                                    '2nd retry attempt still failed, going to ask for help from other replicas', flush=True)
                                get_latest_data(address)
                        print(
                            '3rd retry attempt with hopefully new data', flush=True)
                        r = keyValueStore(key)
                        response = r.get_json()
                        statusCode = r.status_code

    return make_response(jsonify(response), statusCode)


# /key-value-store-view endpoint
# TODO: broadcast view changes for PUT and DELETE methods
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def keyValueStoreView():
    if(request.method == 'GET'):
        response = {'message': 'View retrieved successfully', 'view': view}
        statusCode = 200

    if(request.method == 'PUT'):
        print('received PUT in key-value-store-view', flush=True)
        if('socket-address' in request.json):
            new_socket_address = request.json['socket-address']
            print('need to add %s to my view' % new_socket_address, flush=True)
            address_in_view = False
            for address in view:
                if address == new_socket_address:
                    address_in_view = True
            if(address_in_view):
                response = {
                    "error": "Socket address already exists in the view", "message": "Error in PUT"}
                statusCode = 404
            else:
                view.append(new_socket_address)
                vector_clock.append(0)
                response = {
                    'message': 'Replica added successfully to the view', 'view': view}
                statusCode = 201

    if(request.method == 'DELETE'):
        if('socket-address' in request.json):
            deleted_socket_address = request.json['socket-address']
            print('deleting socket address: %s' %
                  deleted_socket_address, flush=True)
            for address in view:
                if address == deleted_socket_address:
                    view.remove(deleted_socket_address)
                    # if in view, delete it
                    response = {
                        "message": "Replica deleted successfully from the view"}
                    statusCode = 200
                    print('new view: %s' % view, flush=True)
                else:
                    # else return item not found
                    response = {
                        "error": "Socket address does not exist in the view", "message": "Error in DELETE"}
                    statusCode = 404

    return jsonify(response), statusCode

# Shard View where you can view the shards
# /key-value-store-shard endpoint
@app.route('/key-value-store-shard/<route>', methods=['GET', 'PUT', 'DELETE'])
def keyValueStoreShard(route):
    # shard_view = { "shard1" : ["node1, node2, node3"], "shard2" : ["node4, node5, node6"] }

    # determine if key is "shard-id" or "node-shard-id"
    if(request.method == 'GET'):

        # Get the IDs of all shards in the store
        if(route == "shard-ids"):
            # curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n"
            # http://<node-socket-address>/key-value-store-shard/shard-ids
            response = {'message': 'Shard IDS retrieved successfully',
                        'shard-ids': shard_ids}
            statusCode = 200

        # Get the shard ID of a node
        elif(route == "node-shard-id"):
            # curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n"
            # http://<node-socket-address>/key-value-store-shard/node-shard-id
            response = {
                "message": "Shard ID of the node retrieved successfully", "shard-id": node_shard_ids[socket_address]}
            statusCode = 200
        else:
            # completely wrong, not supported return error
            response = {
                "message": "completely wrong, not supported return error"}
            statusCode = 404

    if(request.method == 'PUT'):
        # Reshard
        # TODO: Resharding
        if(route == "reshard"):
            response = {'message': 'Reshard request'}
            statusCode = 200

    return jsonify(response), statusCode

# get the members of a shard with a given ID
@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def get_shard_members(shard_id):
    if(request.method == 'GET'):
        response = {
            "message": "Members of shard ID retrieved successfully", "shard-id-members": shard_members[int(shard_id)]}
        statusCode = 200
    else:
        # completely wrong, not supported return error
        response = {
            "message": "completely wrong, not supported return error"}
        statusCode = 404

    return jsonify(response), statusCode

# get the number of keys stored in a shard
@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=['GET'])
def get_shard_key_count(shard_id):
    shard_id = int(shard_id)
    address = shard_members[shard_id][0]
    url = 'http://' + address + '/key-value-store'
    # grab vector clock and key-store
    print('getting key store from %s!' %
          address, flush=True)
    r = requests.get(url=url, timeout=5)
    response = r.json()
    key_count = len(response['key-store'])

    if(request.method == 'GET'):
        response = {
            "message": "Key count of shard ID retrieved successfully", "shard-id-key-count": key_count}
        statusCode = 200

    return jsonify(response), statusCode

# curl --request PUT --header "Content-Type: application/json" --write-out "\n%{http_code}\n"
# --data'{"socket-address": <new-node-socket-address>}'http://<node-socket-address>/key-value-store-shard/add-member/<shard-id>
# Add a node to a shard
@app.route('/key-value-store-shard/add-member/<shard_id>', methods=['PUT'])
def add_member(shard_id):
    global view, shard_members
    shard_id = int(shard_id)
    new_address = request.json['socket-address']
    data = {'socket-address': new_address}

    print('Adding new address %s to shard: %s' %
          (new_address, shard_id), flush=True)
    for address in view:
        url = 'http://' + \
            str(address) + '/add_me/' + str(shard_id)
        r = requests.put(url=url, json=data, timeout=5)
        #response = r.json()
        statusCode = r.status_code

    # grab key_store and vector clock copy from someone else in my shard

    # instead, tell new node to call get_latest_data
    # get_latest_data(shard_members[shard_id][0])
    url = 'http://' + \
        str(address) + '/get_latest_data/' + shard_members[shard_id][0]
    r = requests.get(url=url,  timeout=5)

    response = ""
    statusCode = r.status_code

    return jsonify(response), statusCode


@app.route('/add_me/<shard_id>', methods=['PUT'])
def add_me(shard_id):
    global shard_members, node_shard_ids
    print("shard_members: %s" % str(shard_members))
    new_address = request.json['socket-address']
    shard_id = int(shard_id)
    #sender_address = request.remote_addr + ":8085"
    # if shard_id not in shard_members:
    #shard_members[shard_id] = []

    shard_members[shard_id].append(new_address)
    node_shard_ids[new_address] = shard_id

    response = "added node " + \
        str(new_address) + "to shard " + str(shard_id)
    statusCode = 200

    return jsonify(response), statusCode


# reshard the key-value store
@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
    global view
    if(request.method == 'PUT'):
        new_shard_count = request.json['shard-count']
        if((len(view)//new_shard_count) >= 2):
            # call reshard function
            reshard_keys(new_shard_count)
            # check response from reshard function
            response = {
                "message": "Resharding done successfully"}
            statusCode = 200
        else:
            response = {
                "message": "Not enough nodes to provide fault-tolerance with the givenshard count!"}
            statusCode = 400

    return jsonify(response), statusCode


def reshard_keys(new_shard_count):
    global view
    master_key_store = {}

    # Make master list of all keys
    for shard in shard_ids:
        target_node = shard_members[shard][0]

        url = 'http://' + target_node + '/key-value-store'

        print('GETTING KEY STORE FROM %s' %
              (target_node), flush=True)
        r = requests.get(url=url,  timeout=5)
        response = r.json()
        statusCode = r.status_code

        received_key_store = response['key-store']
        print('COPYING KEYSTORE %s FROM %s' %
              (received_key_store, shard), flush=True)
        for key in received_key_store:
            print('ADDING %s' % key, flush=True)
            master_key_store[key] = received_key_store[key]
            print('master_key_store updated: %s' %
                  master_key_store, flush=True)

    # tell all nodes in view to prepare for reshard, repartition the nodes
    print('Sending prepare message to all nodes', flush=True)
    for address in view:
        url = 'http://' + \
            str(address) + '/key-value-store-shard/prepare-reshard/' + \
            str(new_shard_count)

        r = requests.get(url=url,  timeout=5)
        response = r.json()
        statusCode = r.status_code

    # Repartition the keys
    for key in master_key_store:
        url = 'http://' + socket_address + '/key-value-store/' + key
        data = {
            'value': master_key_store[key], 'causal-metadata': vector_clock}

        print('Repartitioning key %s ' %
              (key), flush=True)
        r = requests.put(url=url, json=data, timeout=5)
        response = r.json()
        statusCode = r.status_code

        print('Response: %s, Status: %s ' % (response, statusCode), flush=True)


# reshard the key-value store
@app.route('/key-value-store-shard/prepare-reshard/<new_shard_count>', methods=['GET'])
def prepare_reshard(new_shard_count):
    global view, shard_count, shard_ids, shard_members, node_shard_ids, key_store
    new_shard_count = int(new_shard_count)
    print("Received prepare reshard..", flush=True)

    # Make new variables to hold shards (shard_count, shard_members, shard_ids)
    # Make list of shard ids [1,2,3,4..]
    new_shard_ids = list(range(1, new_shard_count + 1))
    # shard_members = { "1" : ["10.0.0.2:8085", "10.0.0.3:8085", "10.0.0.4:8085"], "2" : ["10.10.0.5:8085","10.10.0.6:8085","10.10.0.7:8085"] }
    new_shard_members = {}
    # node_shard_ids = {node : [shard_id], ... }
    new_node_shard_ids = {}

    # Split nodes into shards (already have code)
    total_nodes = len(view)
    capacity_of_shards = total_nodes // new_shard_count

    # partition nodes into shards
    current_shard = 1
    for current_node in range(1, total_nodes + 1):
        if current_shard not in new_shard_members:
            new_shard_members[current_shard] = []

        new_shard_members[current_shard].append(view[current_node-1])
        new_node_shard_ids[view[current_node-1]] = current_shard

        # If we are not in the last shard and it's not time to move to the next shard
        if(current_shard != new_shard_count and current_node % capacity_of_shards == 0):
            current_shard += 1

    # set all temps to be replacements
    shard_ids = new_shard_ids
    shard_members = new_shard_members
    node_shard_ids = new_node_shard_ids
    shard_count = new_shard_count
    print('new shard count: %s, new shards: %s' %
          (shard_count, shard_members), flush=True)
    print('new shard ids: %s' % str(shard_ids), flush=True)

    # Wipe key_store
    key_store = {}

    response = {
        "message": "Repartitioned nodes successfully"}
    statusCode = 200

    return jsonify(response), statusCode


manager = Manager(app)


@manager.command
def run_server():
    print("initializing new replica...", flush=True)
    initialize_node()
    app.run(host='0.0.0.0', port=8085)


if __name__ == '__main__':
    manager.run()
