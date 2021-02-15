import math
import random

from contextlib import AbstractContextManager
from dateutil.parser import isoparse
from typing import Iterator, Optional
import io

# see section 4.2.2.12, page 81 of http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.2.pdf
TPCH_DATE_RANGE = [isoparse('1992-01-01'), isoparse('1998-12-31')]

# see http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf page 62
DIST_PER_WARE = 10
CUST_PER_DIST = 30000
NUM_ORDERS = 30000
MAXITEMS = 100000
STOCKS = 100000
# tpch constants
NUM_SUPPLIERS = 10000
NUM_NATIONS = 62
NUM_REGIONS = 5

ALPHA_LOWER = list('abcdefghijklmnopqrstuvwxyz')

ALPHA_UPPER = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

NUM = list('0123456789')

ALPHA = ALPHA_LOWER + ALPHA_UPPER

ALPHA54 = ALPHA_LOWER + list('?@') + ALPHA_UPPER

ALPHANUM = ALPHA + NUM

ALPHANUM64 = ALPHA54 + NUM

NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']

# yapf: disable
NATIONS = [
    # nation key, name, region key
    (48, 'Australia', 4),         (49, 'Belgium', 5),            (50, 'Cameroon', 1),
    (51, 'Denmark', 5),           (52, 'Equador', 2),            (53, 'France', 5),
    (54, 'Germany', 5),           (55, 'Hungary', 5),            (56, 'Italy', 5),
    (57, 'Japan', 3),

    (65, 'Kenya', 1),             (66, 'Lithuania', 5),          (67, 'Mexico', 2),
    (68, 'Netherlands', 5),       (69, 'Oman', 1),               (70, 'Portugal', 5),
    (71, 'Qatar', 1),             (72, 'Rwanda', 1),             (73, 'Serbia', 5),
    (74, 'Togo', 1),              (75, 'United States', 2),      (76, 'Vietnam', 3),
    (77, 'Singapore', 3),         (78, 'Cambodia', 3),           (79, 'Yemen', 1),
    (80, 'Zimbabwe', 1),          (81, 'Argentina', 2),          (82, 'Bolivia', 2),
    (83, 'Canada', 2),            (84, 'Dominican Republic', 2), (85, 'Egypt', 1),
    (86, 'Finland', 5),           (87, 'Ghana', 1),              (88, 'Haiti', 2),
    (89, 'India', 3),             (90, 'Jamaica', 4),

    ( 97, 'Kazakhstan', 3),       ( 98, 'Luxemburg', 5),         ( 99, 'Marocco', 1),
    (100, 'Norway', 5),           (101, 'Poland', 5),            (102, 'Peru', 2),
    (103, 'Nicaragua', 2),        (104, 'Romania', 5),           (105, 'South Africa', 1),
    (106, 'Thailand', 3),         (107, 'United Kingdom', 5),    (108, 'Venezuela', 2),
    (109, 'Liechtenstein', 5),    (110, 'Austria', 5),           (111, 'Laos', 3),
    (112, 'Zambia', 1),           (113, 'Switzerland', 5),       (114, 'China', 3),
    (115, 'Papua New Guinea', 4), (116, 'East Timor', 4),        (117, 'Bulgaria', 5),
    (118, 'Brazil', 2),           (119, 'Albania', 5),           (120, 'Andorra', 5),
    (121, 'Belize', 2),           (122, 'Botswana', 1),
]
# yapf: enable

REGIONS = [(1, 'Africa'), (2, 'America'), (3, 'Asia'), (4, 'Australia'), (5, 'Europe')]

NOUNS = [
        'foxes', 'ideas', 'theodolites', 'pinto beans', 'instructions', 'dependencies', 'excuses',
        'platelets', 'asymptotes', 'courts', 'dolphins', 'multipliers', 'sauternes', 'warthogs',
        'frets', 'dinos', 'attainments', 'somas', 'Tiresias', 'patterns', 'forges', 'braids',
        'hockey players', 'frays', 'warhorses', 'dugouts', 'notornis', 'epitaphs', 'pearls',
        'tithes', 'waters', 'orbits', 'gifts', 'sheaves', 'depths', 'sentiments', 'decoys',
        'realms', 'pains', 'grouches', 'escapades'
]

VERBS = [
        'sleep', 'wake', 'are', 'cajole', 'haggle', 'nag', 'use', 'boost', 'affix', 'detect',
        'integrate', 'maintain', 'nod', 'was', 'lose', 'sublate', 'solve', 'thrash', 'promise',
        'engage', 'hinder', 'print', 'x-ray', 'breach', 'eat', 'grow', 'impress', 'mold', 'poach',
        'serve', 'run', 'dazzle', 'snooze', 'doze', 'unwind', 'kindle', 'play', 'hang', 'believe',
        'doubt'
]

ADJECTIVES = [
        'furious', 'sly', 'careful', 'blithe', 'quick', 'fluffy', 'slow', 'quiet', 'ruthless',
        'thin', 'close', 'dogged', 'daring', 'brave', 'stealthy', 'permanent', 'enticing', 'idle',
        'busy', 'regular', 'final', 'ironic', 'even', 'bold', 'silent'
]

ADVERBS = [
        'sometimes', 'always', 'never', 'furiously', 'slyly', 'carefully', 'blithely', 'quickly',
        'fluffily', 'slowly', 'quietly', 'ruthlessly', 'thinly', 'closely', 'doggedly', 'daringly',
        'bravely', 'stealthily', 'permanently', 'enticingly', 'idly', 'busily', 'regularly',
        'finally', 'ironically', 'evenly', 'boldly', 'silently'
]

PREPOSITIONS = [
        'about', 'above', 'according to', 'across', 'after', 'against', 'along', 'alongside of',
        'among', 'around', 'at', 'atop', 'before', 'behind', 'beneath', 'beside', 'besides',
        'between', 'beyond', 'by', 'despite', 'during', 'except', 'for', 'from', 'in place of',
        'inside', 'instead of', 'into', 'near', 'of', 'on', 'outside', 'over', 'past', 'since',
        'through', 'throughout', 'to', 'toward', 'under', 'until', 'up', 'upon', 'without', 'with',
        'within'
]

TERMINATORS = ['.', ';', ':', '?', '!', '--']

AUXILIARIES = [
        'do', 'may', 'might', 'shall', 'will', 'would', 'can', 'could', 'should', 'ought to',
        'must', 'will have to', 'shall have to', 'could have to', 'should have to', 'must have to',
        'need to', 'try to'
]


class Random:
    def __init__(self, seed):
        self.rng = random.Random(seed)

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

    def decision(self, frac):
        return self.rng.random() < frac

    def randint_inclusive(self, low, high):
        return self.rng.randint(low, high)

    def sample(self):
        return self.rng.random()

    def gaussian(self, mean, variance):
        return self.rng.gauss(mean, variance)

    def shuffle(self, l):
        self.rng.shuffle(l)

    def from_list(self, l, length=1):
        return self.rng.choices(l, k=length)


class TPCCText:
    def __init__(self, random):
        self.random = random

    def lastname(self, num):
        part_a = NAMES[math.floor(num / 100)]
        part_b = NAMES[math.floor(num / 10) % 10]
        part_c = NAMES[num % 10]
        return part_a + part_b + part_c

    def string(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHA, length))

    def numstring(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(NUM, length))

    def alnumstring(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHANUM, length))

    def alnum64string(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHANUM64, length))

    def data(self, min_length, max_length):
        length = self.random.randint_inclusive(min_length, max_length)
        return self.alnum64string(length)

    def data_original(self, min_length, max_length):
        original = 'ORIGINAL'
        length = self.random.randint_inclusive(min_length, max_length - len(original))
        alphanum64 = self.alnum64string(length)
        l1 = self.random.randint_inclusive(0, length - len(original))
        return '{}{}{}'.format(alphanum64[0:l1], original, alphanum64[l1:length])

    def state(self):
        return self.alnumstring(2).upper()


class TPCHText:
    def __init__(self, random):
        self.random = random

    def random_noun(self):
        return NOUNS[self.random.randint_inclusive(0, len(NOUNS) - 1)]

    def random_verb(self):
        return VERBS[self.random.randint_inclusive(0, len(VERBS) - 1)]

    def random_adjective(self):
        return ADJECTIVES[self.random.randint_inclusive(0, len(ADJECTIVES) - 1)]

    def random_adverb(self):
        return ADVERBS[self.random.randint_inclusive(0, len(ADVERBS) - 1)]

    def random_preposition(self):
        return PREPOSITIONS[self.random.randint_inclusive(0, len(PREPOSITIONS) - 1)]

    def random_terminator(self):
        return TERMINATORS[self.random.randint_inclusive(0, len(TERMINATORS) - 1)]

    def random_auxiliary(self):
        return AUXILIARIES[self.random.randint_inclusive(0, len(AUXILIARIES) - 1)]

    def random_noun_phrase(self):
        if self.random.decision(1 / 4):
            return self.random_noun()
        elif self.random.decision(1 / 4):
            return f'{self.random_adjective()} {self.random_noun()}'
        elif self.random.decision(1 / 4):
            return f'{self.random_adjective()}, {self.random_adjective()} {self.random_noun()}'
        else:
            return f'{self.random_adverb()} {self.random_adjective()} {self.random_noun()}'

    def random_verb_phrase(self):
        if self.random.decision(1 / 4):
            return self.random_verb()
        elif self.random.decision(1 / 4):
            return f'{self.random_auxiliary()} {self.random_verb()}'
        elif self.random.decision(1 / 4):
            return f'{self.random_verb()} {self.random_adverb()}'
        else:
            return f'{self.random_auxiliary()} {self.random_verb()} {self.random_adverb()}'

    def random_prepositional_phrase(self):
        return f'{self.random_preposition()} the {self.random_noun_phrase()}'

    def random_sentence(self):
        if self.random.decision(1 / 5):
            return (
                    f'{self.random_verb_phrase()} {self.random_noun_phrase()} '
                    f'{self.random_terminator()}'
            )
        elif self.random.decision(1 / 5):
            return (
                    f'{self.random_noun_phrase()} {self.random_verb_phrase()} '
                    f'{self.random_prepositional_phrase()} {self.random_terminator()}'
            )
        elif self.random.decision(1 / 5):
            return (
                    f'{self.random_noun_phrase()} {self.random_verb_phrase()} '
                    f'{self.random_noun_phrase()} {self.random_terminator()}'
            )
        elif self.random.decision(1 / 5):
            return (
                    f'{self.random_noun_phrase()} {self.random_prepositional_phrase()} '
                    f'{self.random_verb_phrase()} {self.random_noun_phrase()}'
                    f'{self.random_terminator()}'
            )
        else:
            return (
                    f'{self.random_noun_phrase()} {self.random_prepositional_phrase()} '
                    f'{self.random_verb_phrase()} {self.random_prepositional_phrase()}'
                    f'{self.random_terminator()}'
            )

    def random_text(self, length):
        text = ''
        for i in range(25):
            if i != 0:
                text += ':'
            text += self.random_sentence()
        pos = self.random.randint_inclusive(0, len(text) - length)
        return text[pos:pos + length]

    def random_length_text(self, min_length, max_length):
        random_length = self.random.randint_inclusive(min_length, max_length)
        return self.random_text(random_length)

    def random_customer_text(self, min_length, max_length, action):
        total_length = self.random.randint_inclusive(min_length, max_length)
        l1 = self.random.randint_inclusive(0, total_length - 8 - len(action))
        l2 = self.random.randint_inclusive(0, total_length - l1 - 8 - len(action))
        l3 = total_length - l1 - l2 - 8 - len(action)
        return '{}Customer{}{}{}'.format(
                self.random_text(l1), self.random_text(l2), action, self.random_text(l3)
        )

    def random_phone_number(self, key):
        country = (key % 90) + 10
        local1 = self.random.randint_inclusive(100, 999)
        local2 = self.random.randint_inclusive(100, 999)
        local3 = self.random.randint_inclusive(1000, 9999)
        return '{}-{}-{}-{}'.format(country, local1, local2, local3)


class TimestampGenerator:
    def __init__(self, start_date, random, scalar = 1.0):
        self.random = random
        self.current = start_date

        orders_per_warehouse = NUM_ORDERS * DIST_PER_WARE
        date_range = TPCH_DATE_RANGE[1] - TPCH_DATE_RANGE[0]
        self.increment = (date_range / orders_per_warehouse) * scalar

    def next(self):
        self.current += self.increment * self.random.gaussian(mean=1, variance=0.05)
        return self.current


# Taken from CPython 3.7 for compatibility with Python 3.6
class nullcontext(AbstractContextManager):
    """Context manager that does no additional processing.
    Used as a stand-in for a normal context manager, when a particular
    block of code is only sometimes used with a normal context manager:
    cm = optional_cm if condition else nullcontext()
    with cm:
        # Perform operation, using optional_cm if condition is True
    """

    def __init__(self, enter_result=None):
        self.enter_result = enter_result

    def __enter__(self):
        return self.enter_result

    def __exit__(self, *excinfo):
        pass

# from https://hakibenita.com/fast-load-data-python-postgresql
class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)
