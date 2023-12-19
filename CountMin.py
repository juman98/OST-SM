import numpy as np

def hash_mod(mod):
    return lambda x: (hash(x) % mod)
    
class CountMin:
    def __init__(self, delta, epsilon):
        self.w = int(2/epsilon)
        self.d = int(np.ceil(np.log(1/delta)))
        self.table = np.zeros((self.d, self.w))
        self.hashes = [hash_mod(self.w) for _ in range(self.d)]
    
    def update(self, value):
        for row,h in enumerate(self.hashes):
            column = h(value)
            self.table[row,column] += 1
    
    def query(self, value):
        counts = []
        for row,h in enumerate(self.hashes):
            column = h(value)
            counts.append(self.table[row,column])
        return int(min(counts))


countmin_table = CountMin(0.0001,0.1)
    
def count_min(message_value):
    global countmin_table
    PowerLineID = message_value["PowerLineID"]
    countmin_table.update(PowerLineID)
    count = countmin_table.query(PowerLineID)
    return count



