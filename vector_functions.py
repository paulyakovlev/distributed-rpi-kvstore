def increment_clock(socket_address, view, vector_clock):
    # position of key in store is its position in vector clock
    # dicts are order-preserving since Python 3.7, so we can get the key's position in
    # the store by getting a list of keys in key_store and grabbing the key's index in this list

    vc_position = list(view).index(socket_address)
    print('our vector clock before increment: %s' %
          str(vector_clock), flush=True)

    vector_clock[vc_position] += 1
    print('our vector clock after increment: %s' %
          str(vector_clock), flush=True)
    return vector_clock


def get_max_clock(my_vc, incoming_vc):
    max_vc = []
    if (len(my_vc) != len(incoming_vc)):
        print('vector clocks must be of same size to compare!', flush=True)
        return my_vc
    else:
        for i in range(0, len(my_vc)):
            max_val = max(my_vc[i], incoming_vc[i])
            max_vc.append(max_val)

        print('max of %s and %s is %s' %
              (str(my_vc), str(incoming_vc), str(max_vc)), flush=True)
        return max_vc
