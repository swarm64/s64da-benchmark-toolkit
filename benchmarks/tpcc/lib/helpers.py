
import math

from numpy.random import RandomState

ALPHA = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
NUM = list('0123456789')
NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']

class Random:
    def __init__(self, seed):
        self.rng = RandomState(seed)

        self.C_255 = self.rng.randint(0, 256)
        self.C_1023 = self.rng.randint(0, 1024)
        self.C_8191 = self.rng.randint(0, 8192)

    def nurand(self, A, x, y):
        if A == 255:
            C = self.C_255
        elif A == 1023:
            C = self.C_1023
        elif A == 8191:
            C = self.C_8191

        return (((self.rng.randint(0, A + 1) | self.rng.randint(x, y + 1)) + C) % (y - x + 1)) + x

    def lastname(self, num):
        part_a = NAMES[math.floor(num / 100)]
        part_b = NAMES[math.floor(num / 10) % 10]
        part_c = NAMES[num % 10]
        return part_a + part_b + part_c

    def randint_inclusive(self, low, high):
        return self.rng.randint(low, high + 1)

    def sample(self):
        return self.rng.random_sample()

    def shuffle(self, l):
        self.rng.shuffle(l)

    def string(self, length, prefix=''):
        return prefix + ''.join(self.rng.choice(ALPHA, size=length))

    def numstring(self, length, prefix=''):
        return prefix + ''.join(self.rng.choice(NUM, size=length))

    def get_state(self):
        return self.string(2).upper()

    def from_list(self, l):
        return self.rng.choice(l)
