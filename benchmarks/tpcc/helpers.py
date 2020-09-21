
import math

from numpy import random

ALPHA = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
NUM = list('0123456789')
NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']

class Random:
    def __init__(self, seed):
        random.seed(seed)

        self.C_255 = random.randint(0, 256)
        self.C_1023 = random.randint(0, 1024)
        self.C_8191 = random.randint(0, 8192)

    def nurand(self, A, x, y):
        if A == 255:
            C = self.C_255
        elif A == 1023:
            C = self.C_1023
        elif A == 8191:
            C = self.C_8191

        return (((random.randint(0, A + 1) | random.randint(x, y + 1)) + C) % (y - x + 1)) + x

    @classmethod
    def lastname(cls, num):
        part_a = NAMES[math.floor(num / 100)]
        part_b = NAMES[math.floor(num / 10) % 10]
        part_c = NAMES[num % 10]
        return part_a + part_b + part_c

    @classmethod
    def randint_inclusive(cls, low, high):
        return random.randint(low, high + 1)

    @classmethod
    def sample(cls):
        return random.random_sample()

    @classmethod
    def shuffle(cls, l):
        random.shuffle(l)

    @classmethod
    def string(cls, length, prefix=''):
        return prefix + ''.join(random.choice(ALPHA, size=length))

    @classmethod
    def numstring(cls, length, prefix=''):
        return prefix + ''.join(random.choice(NUM, size=length))

    @classmethod
    def get_state(cls):
        return cls.string(2).upper()
