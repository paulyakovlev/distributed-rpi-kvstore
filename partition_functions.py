def get_key_location(key, shard_count):
    hashed_key = hash(key)
    key_location = hashed_key % shard_count

    return key_location + 1


# TODO: repartition the data when number of nodes changes
def repartition():
    print("repartitioning data...")


# A Get/Put/Delete request comes in
# First we have to determine which shard it should be directed at
# Run the value through the hash and mod by number of shards
# Now we have the shard we need
# choose any replica in that shard view to handle initial get/put/delete
    # if a put/deleteit should be broadcasst to every node in the shard view
# then update the vector clock
# broadcast the vector clock update to all other shards, which much update all relicas in that shard
