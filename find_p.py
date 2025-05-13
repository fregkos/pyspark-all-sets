d = 5000  # our total multitude of data
q = 1000  # our max reducer capacity for elements

# we have established the value of `p` to be
p = d // q  # p is 5 in this case, a prime number
# such that p^2 divides d AND q=d/p :)

g = p ** 2  # number of groups
items_per_group = d // p**2  # the number of items per group

# since we need many of the p*p groups to exist in the reducers, we shall
# split them in the following manner
# (
#     [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], etc],
#     [[], [], []],
#     [[], [], []],
# )


rdd = []
for group_counter in range(d//q): # for each supergroup
    supergroup = []
    for subgroup_counter in range(q//items_per_group): # for each subgroup
        subgroup = []
        for quantum in range(items_per_group): # for each quantum of the subgroup
            subgroup.append(quantum + subgroup_counter*items_per_group + group_counter*q) # plus some counter
        supergroup.append(subgroup)
    rdd.append(supergroup)

print(f"{p=}")
print(f"{d=}, {q=}, {g=}, {items_per_group=}")
from pprint import pprint
# pprint(rdd)
from itertools import combinations_with_replacement

group_pairs = list(combinations_with_replacement(enumerate(rdd, 1), 2))
# pprint(group_pairs)
# print(len(rdd))

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True


def find_p(d, q):
    if d % q != 0:
        return None
    p = d // q
    if p * p == d and is_prime(p):
        return p
    return None


# d = 1001
# q = 1000

# print('d % q =',d % q)

# start_q = q
# start_d = d

# res  = find_p(q,d)
# print(res)
# while res == None:
#     q -= 1
#     res = find_p(q, d)
#     if q < 0 or q > d:
#         q = start_q
#         d += 2
#         if d > start_d + 20000:
#             break


# print(res, d, q)
