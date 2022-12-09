from subprocess import Popen, PIPE
import time
import os
import argparse
import random
import numpy as np
from collections import defaultdict

parser = argparse.ArgumentParser(description="Generate TPCH queries.")

parser.add_argument(
    "-s", "--workload_size", type=int, help="Size of workload.", default=1000
)

parser.add_argument(
    "-t",
    "--transition_steps",
    type=int,
    help="Number of Markov transitions.",
    default=100,
)

parser.add_argument("-p", "--transition_probability",
                    type=float, help="Probability of Markov transition", default=0.1)

parser.add_argument("-o", type=str, help="Output filename",
                    default="workload.sql")
parser.add_argument("-y", "--workload_type",
                    help="workload type", choices=["hard", "soft"], default="soft")
parser.add_argument("-a", "--alpha", type=float,
                    help="weight of uniform distribution", default=0)
parser.add_argument("-r", '--random_seed', type=int,
                    help="Random seed for qgen", default=int(time.time()) * 1000 + 1)
parser.add_argument("-i", "--include_template_id", action='store_true')
args = parser.parse_args()

workload_size = args.workload_size
workload_type = args.workload_type
transition_steps = args.transition_steps
num_nodes = workload_size // transition_steps
transition_prob = args.transition_probability
p = args.transition_probability
alpha = args.alpha
seed = args.random_seed
include_template_id = args.include_template_id

print("Workload Type: {} shift".format(workload_type))
print("Workload Size: {}".format(workload_size))
print("Number of Transitions: {}".format(transition_steps))
print("Number of Nodes: {}".format(num_nodes))

num_of_templates = 22
# templates_ids = list(range(1, num_of_templates + 1))
templates_ids = [7, 5, 8, 21, 4, 12, 10, 3, 18, 2,
                 16, 9, 11, 20, 17, 19, 13, 22, 15, 14, 1, 6]

if workload_type == "hard":
    template_groups = [templates_ids[:3],
                       templates_ids[3:9],
                       templates_ids[9:14],
                       templates_ids[14:]]
    curr_group = 0
    query_count = 0
    fo = open(args.o, "w")
    while query_count < workload_size:
        for node_id in range(num_nodes):
            curr_state = np.random.choice(template_groups[curr_group])
            output = os.popen(
                "DSS_QUERY=./queries ./qgen -r {} -d {}".format(
                    seed, curr_state
                )
            ).read()
            if include_template_id:
                fo.write(str(curr_state) + "|")
            fo.write(" ".join(output.split("\n")[1:]) + "\n")
            fo.flush()
            query_count += 1
        curr_group = (curr_group + 1) % 4
    fo.close()

elif workload_type == "soft":
    states = {node_id: templates_ids[0] for node_id in range(num_nodes)}

    transition_matrix = defaultdict(dict)
    for i, src in enumerate(templates_ids):
        for j, tgt in enumerate(templates_ids):
            uniform = 1 / num_of_templates
            if j == (i+1) % num_of_templates:
                skewed = p
            elif j == i:
                skewed = 1 - p
            else:
                skewed = 0
            weighted = alpha * uniform + (1-alpha) * skewed
            transition_matrix[src][tgt] = weighted
    query_count = 0

    fo = open(args.o, "w")
    while query_count < workload_size:
        for node_id in range(num_nodes):
            curr_state = states[node_id]
            output = os.popen(
                "DSS_QUERY=./queries ./qgen -r {} -d {}".format(
                    seed, curr_state
                )
            ).read()
            if include_template_id:
                fo.write(str(curr_state) + "|")
            fo.write(" ".join(output.split("\n")[1:]) + "\n")
            fo.flush()
            states[node_id] = np.random.choice(list(transition_matrix[curr_state].keys(
            )), 1, p=list(transition_matrix[curr_state].values())).item()
            query_count += 1
    fo.close()

else:
    raise ("Invalid workload_type.")
