import argparse
import re
import json
import datetime
import os
import errno

from sets import Set

parser = argparse.ArgumentParser(description="Convert swift CP8 results to json format.")
parser.add_argument('-i', '--input_file',
					help="Path to swift output.")
parser.add_argument('-o', '--output_file',
					help="Path to write json file to.")
parser.add_argument('-t', '--test_file',
					help="Path to test events file.")

# Taken from http://stackoverflow.com/a/600612/119527
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

args = parser.parse_args()

input_file = args.input_file
input_str = open(input_file, 'r').read()
patn = '.*query : cur_wf(.*)>> query : state.*'
prog = re.compile(patn, re.DOTALL)
result = prog.match(input_str)

mkdir_p(os.path.dirname(args.output_file))
output_file = open(args.output_file, 'w')

test_file = open(args.test_file, 'r')
test_str = test_file.read()
events = json.loads(test_str)

num_workflows = 6

# workflow id to (start index, timestamp)
workflow_to_timestamp = {}
for i in range(num_workflows):
	workflow_to_timestamp[i] = []

index = 0
i = -1
while(index != -1):
	i += 1
	search_str = 'position(@' + str(i) + ')'
	index = input_str.find(search_str)
	
num_timesteps = i

for i in range(0, num_timesteps):
	start_str = 'cur_wf_type(@' + str(i) + ')'
	end_str = 'position(@' + str(i) + ')'

	start = input_str.find(start_str)
	end = input_str.find(end_str)
	cur_str = input_str[start + len(start_str):end]
	lines = cur_str.split('\n')[1:-1] # [1:-1] to remove empty entries
	if len(lines) == 0:
		continue
	best_id = None
	highest_prob = 0
	patn = '.*\[(\d*)\]\t(\d\.\d*)'
	prog = re.compile(patn)
	for j in range(len(lines)):
		line = lines[j]
		result = prog.match(line)
		prob = float(result.group(2))
		if prob > 1:
			continue
		if prob > highest_prob:
			highest_prob = prob
			best_id = int(result.group(1))
	events[i]['workflowTemplateId'] = best_id + 1

	start_str = end_str
	end_str = 'cur_wf_type(@' + str(i+1) + ')'
	start = input_str.find(start_str)
	end = input_str.find(end_str)
	cur_str = input_str[start + len(start_str):end]
	lines = cur_str.split('\n')[1:-1] # [1:-1] to remove empty entries
	best_pos = None
	highest_prob = 0
	patn = '(.*)\t(\d\.\d*)'
	prog = re.compile(patn)
	for j in range(len(lines)):
		line = lines[j]
		if len(line) == 0:
			continue
		result = prog.match(line)
		if result == None:
			continue
		prob = float(result.group(2))
		if prob > highest_prob:
			highest_prob = prob
			best_pos = result.group(1)
	best_pos = best_pos[1:] # remove \t
	if best_pos == 'END':
		event_runner = None
		pos = 'END'
		j = i - 1
		while pos != 'START':
			pos = events[j]['workflowPosition']
			events[j]['workflowTemplateId'] = best_id + 1
			j -= 1


	events[i]['workflowPosition'] = best_pos

for i in range(0, num_timesteps):
	if events[i]['workflowPosition'] == 'START':
		workflow_type = events[i]['workflowTemplateId']
		timestamp = events[i]['parameters']['EMAIL_IN']['Timestamp'][:-1]
		parsed_timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
		workflow_to_timestamp[workflow_type - 1].append((i, parsed_timestamp))

for workflow in workflow_to_timestamp.keys():
	lst = workflow_to_timestamp[workflow]
	lst = sorted(lst, key=lambda x: x[1])
	workflow_to_timestamp[workflow] = lst

workflow_to_id = {}
for workflow in workflow_to_timestamp.keys():
	lst = workflow_to_timestamp[workflow]
	for i in range(len(lst)):
		pair = lst[i]
		workflow_to_id[pair[0]] = i + 1

prev_id = -1
for i in range(0, num_timesteps):
	if i in workflow_to_id.keys():
		events[i]['workflowTemplateInstanceId'] = workflow_to_id[i]
		prev_id = workflow_to_id[i]
	else:
		events[i]['workflowTemplateInstanceId'] = prev_id

output_file.write(json.dumps(events, indent=2))


output_file.close()
test_file.close()
