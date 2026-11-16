"""Номер розділу shuffle так, як його рахує Spark: pmod(murmur3_hash(ключі, seed=42), P).

HashPartitioning(keys, P) у Spark = Pmod(Murmur3Hash(keys, 42), P). Для int і date
(date = кількість днів від 1970-01-01) використовується Murmur3_x86_32.hashInt, а кожен
наступний стовпець ключа хешується з seed = хеш попереднього.
"""
import datetime as dt

_C1, _C2, _M = 0xCC9E2D51, 0x1B873593, 0xFFFFFFFF


def _rotl(x, r):
    return ((x << r) | (x >> (32 - r))) & _M


def _hash_int(value, seed):
    k1 = (value & _M) * _C1 & _M
    k1 = _rotl(k1, 15) * _C2 & _M
    h1 = (seed & _M) ^ k1
    h1 = (_rotl(h1, 13) * 5 + 0xE6546B64) & _M
    h1 ^= 4
    h1 ^= h1 >> 16
    h1 = h1 * 0x85EBCA6B & _M
    h1 ^= h1 >> 13
    h1 = h1 * 0xC2B2AE35 & _M
    h1 ^= h1 >> 16
    return h1 - (1 << 32) if h1 & 0x80000000 else h1  # знаковий int32, як у Java


def spark_hash(*values):
    h = 42
    for v in values:
        if isinstance(v, dt.date):
            v = (v - dt.date(1970, 1, 1)).days
        h = _hash_int(int(v), h)
    return h


def partition(num_partitions, *values):
    return spark_hash(*values) % num_partitions  # Python % для додатного P = pmod
