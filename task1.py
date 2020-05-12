from pyspark import SparkContext
import sys
import json
import time
import copy
import itertools

start_time = time.time()

input_file = sys.argv[1]
output_file = sys.argv[2]

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

# spark-submit task1.py train_review.json task1.res
# hyper-parameters
HASH_LEN = 50
BAND_LEN = 50
ROW_LEN = int(HASH_LEN / BAND_LEN)

RDD0 = sc.textFile(input_file)

output = open(output_file, 'w')

RDD1 = RDD0.map(lambda line: (json.loads(line)['user_id'], json.loads(line)['business_id']))


# #####
# test = RDD1.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a, b: a + b) \
#     .collect()
#
#
# dic = {}
# for i in test:
#     dic[i[0]] = i[1]
#
# def JacSim(l1, l2):
#     s1 = set(l1)
#     s2 = set(l2)
#     print(s1)
#     print(s2)
#     return len(s1.intersection(s2)) / len(s1.union(s2))
# t = JacSim(dic['W6M3kA70puRD8dhqIDtDmw'], dic['l6x6p6WDzyDujr2xscPx3A'])
# print(t)
# #####

u_ids = RDD1.map(lambda x: x[0]).distinct().collect()
b_ids = RDD1.map(lambda x: x[1]).distinct().collect()
u_ids.sort()
b_ids.sort()

u_len = len(u_ids)
b_len = len(b_ids)

u_index_map = {}
b_index_map = {}
index_u_map = {}
index_b_map = {}

for index, user in enumerate(u_ids):
    u_index_map[user] = index
    index_u_map[index] = user

for index, business in enumerate(b_ids):
    b_index_map[business] = index
    index_b_map[index] = business

RDD2 = RDD1.map(lambda x: (b_index_map[x[1]], [u_index_map[x[0]]])).reduceByKey(lambda a, b: a + b) \
    .mapValues(lambda x: sorted(list(set(x)))).sortBy(lambda x: x[0])

characteristic_matrix = RDD2.collect()

prime_numbers = [4391, 6113, 11587, 13441, 23189, 41851, 61379, 74923, 81343, 102001, 151507]


# hash

def hashFunc(r, h):
    return (74923 * r + 13441 * h) % 151507


def sigFunc(x):
    sig_list = []
    for hash_id in range(HASH_LEN):
        sig = float("inf")
        for row_id in x[1]:
            sig = min(sig, hashFunc(row_id, hash_id))
        sig_list.append(sig)
    return sig_list


signature_matrix = RDD2.map(lambda x: sigFunc(x)).collect()

# LSH
candidate_set = set()
similar_set = set()


def dotProductFunc(l1, l2):
    dot = sum([l1[i] * l2[i] for i in range(len(l2))])
    return dot


def hashBandFunc(signatures, start, end):
    hashVal = dotProductFunc(prime_numbers[:ROW_LEN], signatures[start:(end + 1)])
    hashVal = hashVal % 23189
    return hashVal


for band in range(BAND_LEN):
    start = band * ROW_LEN
    end = band * ROW_LEN + ROW_LEN - 1

    k2bdict = {}

    for business in range(b_len):
        k = hashBandFunc(signature_matrix[business], start, end)
        if k in k2bdict.keys():
            k2bdict[k].append(business)
        else:
            k2bdict[k] = []
            k2bdict[k].append(business)

    for key, val in k2bdict.items():
        cand_pairs = itertools.combinations(val, 2)

        for pair in cand_pairs:
            candidate_set.add(tuple(sorted(pair)))


for pair in candidate_set:
    l1 = characteristic_matrix[pair[0]][1]
    l2 = characteristic_matrix[pair[1]][1]
    s1 = set(l1)
    s2 = set(l2)
    jac = len(s1.intersection(s2)) / len(s1.union(s2))
    if jac >= 0.05:
        similar_set.add((index_b_map[pair[0]], index_b_map[pair[1]], jac))

for pair in similar_set:
    output.write('{"b1": "' + str(pair[0]) + '", "b2": "' + str(pair[1]) + '", "sim": ' + str(pair[2]) + '}')
    output.write("\n")

print('Duration: ' + str(time.time() - start_time))
