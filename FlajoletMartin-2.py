import random
import hashlib

max_zeros = [0]*5

def trailing_zeros(n):
    s = str(n)
    return len(s)-len(s.rstrip('0'))

def flajolet_martin(message_value):
    global max_zeros
    for i in range(5):
        hasher = hashlib.md5()
        hasher.update(str(message_value['PowerFlowValue']).encode('utf-8'))
        hasher.update(str(i).encode('utf-8'))
        hash_value = int(hasher.hexdigest(), 16)
        max_zeros[i] = max(max_zeros[i], trailing_zeros(bin(hash_value)))
    return 2**max(max_zeros)

