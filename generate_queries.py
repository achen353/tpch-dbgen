from subprocess import Popen, PIPE
import time
import os
import argparse
import random

parser = argparse.ArgumentParser(description="Generate TPCH queries.")

parser.add_argument(
    "--workload_size", metavar="S", type=int, hlp="Size of workload.", default=1000
)

parser.add_argument(
    "--transition_steps",
    metavar="t",
    type=int,
    help="Number of Markov transitions.",
    default=100,
)

parser.add_argument("-o", type=str, help="Output filename", default="workload.sql")

args = parser.parse_args()

workload_size = args.workload_size
transition_steps = args.transition_steps
num_nodes = workload_size // transition_steps

print("Workload Size: {}".format(workload_size))
print("Number of Transitions: {}".format(transition_steps))
print("Number of Nodes: {}".format(num_nodes))

num_of_templates = 22
templates_ids = list(range(1, num_of_templates + 1))

transition_order = list(templates_ids)
random.shuffle(transition_order)
states = {node_id: transition_order[0] for node_id in range(num_nodes)}

transition_prob = 0.1
query_count = 0

fo = open(args.o, "w")
while query_count < workload_size:
    for node_id in range(num_nodes):
        template_id = states[node_id]
        output = os.popen(
            "DSS_QUERY=/workspaces/verdict/tpch-dbgen/queries ./qgen -r {} {}".format(
                int(time.time()) * 1000 + 1, template_id
            )
        ).read()
        fo.write(" ".join(output.split("\n")[1:]) + "\n")
        fo.flush()
        if random.random() <= transition_prob:
            states[node_id] = template_id + 1
        query_count += 1
fo.close()
