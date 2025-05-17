import random

INT32_MIN = -2**31
INT32_MAX = 2**31 - 1

dist = 10000000

for _ in range(100):
    x = random.randint(INT32_MIN, INT32_MAX - dist)  # ensure y stays in bounds
    y = x + dist
    print(f"r {x} {y}")