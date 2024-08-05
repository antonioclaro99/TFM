import redis
import os

def initialize_redis():
    r = redis.StrictRedis(host='redis', port=6379, db=0)
    r.set('group_a:total_count', 3)
    r.set('group_a:completed_count', 0)
    r.set('group_a:lock', 1)

if __name__ == "__main__":
    initialize_redis()