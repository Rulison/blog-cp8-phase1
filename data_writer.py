import json
from sets import Set
import argparse
import os
import re

parser = argparse.ArgumentParser(description="Writes model to blog file.")
parser.add_argument('-o', '--output_file',
					help="Where to write model.")
parser.add_argument('-p', '--param_file',
					help="Where to write auxiliary data structures computed here.")
args = parser.parse_args()


# TODO: look for patterns in email text
# (workflow template id, oldState, newState) to count
transition_counts = {}
# (workflow template id, oldState, newState) to list of param names that must match
transition_params = {}
# maps from (workflow id, workflow type id) to (latest action, parameters and values)
state = {}

all_param_types = Set()

null_str = 'null0'

end_states = {}
end_counts = {}

middle_counts = {}
middle_end_probs = {}

max_workflow_type_id = 0
# Set of all possible actions, learned from training data.
actions = Set()

def full_match(match, s):
	return match != None and match.group(0) == s

path = '.'
only_dirs = [f for f in os.listdir(path) if not os.path.isfile(os.path.join(path, f))]

patn = 'user\d\d'
prog = re.compile(patn)
training_dirs = [d for d in only_dirs if full_match(prog.match(d), d)]

excluded_params = ['FILE_IN', 'FILE_OUT']
dict_params = ['EMAIL_IN', 'EMAIL_OUT']

same_type_count = 0
diff_type_count = 0

for name in training_dirs:
	file_name = os.path.join(name, 'events.json')
	events_file = open(file_name)
	events = json.loads(events_file.read())
	for event in events:
		if event['workflowPosition'] == 'END':
			if event['workflowTemplateId'] in end_states.keys():
				end_states[event['workflowTemplateId']].add(event['type'])
			else:
				end_states[event['workflowTemplateId']] = Set([event['type']])

for num in end_states.keys():
	states = end_states[num]
	for s in states:
		end_counts[(num, s)] = 0
		middle_counts[(num, s)] = 0

for name in training_dirs:
	file_name = os.path.join(name, 'events.json')
	events_file = open(file_name)
	events = json.loads(events_file.read())
	prev_type = None
	prev_id = None
	for event in events:
		actions.add(event['type'])
		key = (event['workflowTemplateId'], event['type'])
		if key in middle_counts.keys():
			if event['workflowPosition'] == 'MIDDLE':
				middle_counts[key] += 1
			else:
				end_counts[key] += 1
		if prev_type != None:
			if event['workflowTemplateId'] == prev_type and event['workflowTemplateInstanceId'] == prev_id:
				same_type_count += 1
			else:
				diff_type_count += 1
		prev_type = event['workflowTemplateId']
		prev_id = event['workflowTemplateInstanceId']

		# New workflow instance
		if event['workflowPosition'] == 'START':
			# Learn how many workflow types there are (6).
			max_workflow_type_id = max(max_workflow_type_id, event['workflowTemplateId'])
			state[(event['workflowTemplateId'], event['workflowTemplateInstanceId'])] = (event['type'], event['parameters'])
			key = (event['workflowTemplateId'], None, event['type'])
			if key in transition_counts.keys():
				transition_counts[key] += 1
			else:
				transition_counts[key] = 1
		# Continuing workflow instance
		else:
			workflowId = event['workflowTemplateId']
			instanceId = event['workflowTemplateInstanceId']
			currentState = state[(workflowId, instanceId)]
			runningParams = currentState[1]
			newParams = event['parameters']				
			for param in newParams.keys():
				# Learn what params exist.
				if param not in dict_params:
					all_param_types.add(param)
			param_check_list = []
			matchingParamNames = None
			for param in newParams.keys():
				if param in excluded_params:
					continue
				if param in dict_params:
					inner_params = tuple(newParams[param].keys())
					param_check_list.append((param,inner_params))
					for p in inner_params:
						all_param_types.add(param + '_' + p)
				else:
					param_check_list.append(param)
			for param in param_check_list:
				if type(param) == tuple:
					for dict_param in dict_params:
						if dict_param in runningParams.keys():
							inner_params = runningParams[dict_param].keys()
							for old_inner_param in inner_params:
								for new_inner_param in newParams[param[0]].keys():
									if runningParams[dict_param][old_inner_param] == newParams[param[0]][new_inner_param]:
										if matchingParamNames == None:
											matchingParamNames = (((param[0], new_inner_param) ,(dict_param, old_inner_param)),)
										else:
											matchingParamNames += (((param[0], new_inner_param) ,(dict_param, old_inner_param)),)
				else:
					for oldParam in runningParams.keys():
						if type(oldParam) == tuple:
							top_param = oldParam[0]
							inner_params = oldParam[1]

						if newParams[param] == runningParams[oldParam]:
							if matchingParamNames == None:
								matchingParamNames = ((param, oldParam),)
							else:
								matchingParamNames += ((param, oldParam),)


			for param, val in newParams.items():
				runningParams[param] = val
			key = (event['workflowTemplateId'], currentState[0], event['type'])
			if key in transition_params.keys():
				oldMatching = transition_params[key]
				oldMatchingSet = Set()
				if oldMatching == None:
					oldMatchingSet.add(None)
				else:
					for i in oldMatching:
						oldMatchingSet.add(i)
				newMatchingSet = Set()
				if matchingParamNames == None:
					newMatchingSet.add(None)
				else:
					for i in matchingParamNames:
						newMatchingSet.add(i)
				overlapSet = oldMatchingSet.intersection(newMatchingSet)
				overlap = ()
				for i in overlapSet:
					overlap += (i,)
				if overlap == (None,) or overlap == ():
					overlap = None
				transition_params[key] = overlap
			else:
				transition_params[key] = matchingParamNames

			if key in transition_counts.keys():
				transition_counts[key] += 1
			else:
				transition_counts[key] = 1
			state[(workflowId, instanceId)] = (event['type'], runningParams)

transition_totals = {}
for key in transition_counts:
	key2 = key[:2]
	if key2 in transition_totals:
		transition_totals[key2] += transition_counts[key]
	else:
		transition_totals[key2] = transition_counts[key]

for key in middle_counts.keys():
	middle_end_probs[key] = float(middle_counts[key])/(middle_counts[key] + end_counts[key])

transition_probs = {}
for key in transition_counts:
	transition_probs[key] = transition_counts[key]/float(transition_totals[key[:2]])

same_type_prob = float(same_type_count)/(same_type_count+diff_type_count)
diff_type_count = 1 - same_type_prob

def stringify(item):
	return str(item).replace('\\', '\\\\')


def format_set(s):
	out = ''
	for item in s:
		out += item + ', '
	return out

def format_probs(s, probs):
	out = ''
	names = Set([i[2] for i in s])
	for item in s:
		out += item[2] + ' -> ' + str(probs[item])
		out += ', '
	# Now add every possible state as an option with a small probability,
	# so no unforseen transition leads to all particles having 0 weight.
	for action in actions:
		if action not in names:
			out += action + ' -> ' '0.01'
			out += ', '
	return out[:-2] #[:-2] to remove trailing ', '

# write blog file
def write_file():
	output_file = open(args.output_file, 'w')
	output = ''
	output += 'type WorkflowType; distinct WorkflowType WT[' + str(max_workflow_type_id) + '];\n'
	output += '\n'
	output += 'type Action;\n'
	output += 'distinct Action ' + format_set(actions) + 'Invalid, Start;\n'
	output += '\n'
	output += 'type Position;\n'
	output += 'distinct Position START, MIDDLE, END, Invalid_Position;\n'
	output += 'random WorkflowType cur_wf_type(Timestep t) ~ \n'
	output += '\tif position(t) == START then UniformChoice({wt for WorkflowType wt})\n'
	output += '\telse\n'
	output += '\t\tcur_wf_type(prev(t))\n'
	output += ';\n'
	output += '\n'
	# position function -----------------------
	output += 'random Position position(Timestep t) ~ \n'
	output += '\tif t == @0 then START\n'
	output += '\telse if position(prev(t)) == START then MIDDLE\n'
	output += '\telse if position(prev(t)) == MIDDLE then\n'
	for wf in range(max_workflow_type_id):
		if wf == 0:
			output += '\t\tif cur_wf_type(prev(t)) == WT[' + str(wf) + '] then\n'
		else:
			output += '\t\telse if cur_wf_type(prev(t)) == WT[' + str(wf) + '] then\n'
		cur_keys = [k for k in middle_end_probs.keys() if k[0] == wf+1]
		for key in cur_keys:
			state = key[1]
			if key == cur_keys[0]:
				output += '\t\t\tif state(t) == ' + state + ' then\n'
			else:
				output += '\t\t\telse if state(t) == ' + state + ' then\n'
			output += '\t\t\t\tCategorical({MIDDLE -> ' + str(middle_end_probs[key]) + ', END -> ' + str(1 - middle_end_probs[key]) + '})\n'
		output += '\t\t\telse Categorical({MIDDLE -> 0.99, END -> 0.01})\n' # for robustness, instead of just MIDDLE
	output += '\t\telse Invalid_Position\n'
	output += '\telse\n'
	output += '\t\tSTART\n'
	output += ';\n'

	# state function ------------------
	output += 'random Action state(Timestep t) ~\n'
	# assume each instance starts with ReceiveEmail
	output += '\tif t == @0 | position(prev(t)) == END then ReceiveEmail\n'

	sorted_keys = transition_probs.keys()
	sorted_keys.sort(key=lambda x: x[0])
	for w in range(max_workflow_type_id):
		output += '\telse if cur_wf_type(t) == WT[' + str(w)+ '] then\n'
		# get all states this workflow instances can be
		s = Set()
		# get all transitions found for this workflow
		keys = [key for key in sorted_keys if key[0] - 1 == w]
		for key in keys:
			if key[1] != None:
				s.add(key[1])
		added = False
		for state in s:
			cur_keys = [key for key in keys if key[1] == state]
			if not added:
				output += '\t\tif state(prev(t)) == ' + state + ' then\n'
			else:
				output += '\t\telse if state(prev(t)) == ' + state + ' then\n'
			added = True
			output += '\t\t\t\tCategorical({' + format_probs(cur_keys, transition_probs) + '})\n'
		output += '\t\telse Invalid\n'
	output += '\telse Invalid\n'
	output += ';\n'

	# valid function -------------------
	output += 'random Boolean valid(Timestep t) ~ \n'
	output += '\tif position(t) == START then true\n'
	for w in range(max_workflow_type_id):
		output += '\telse if cur_wf_type(t) == WT[' + str(w) + '] then\n'
		s = Set()
		from_keys = [key for key in sorted_keys if key[0] - 1 == w]
		for key in from_keys:
			if key[1] != None:
				s.add(key[1])
		first_state = True
		for state in s:
			output += '\t\t'
			if not first_state:
				output += 'else '
			first_state = False
			output += 'if state(prev(t)) == ' + state + ' then\n'
			to_keys = [key for key in from_keys if key[1] == state]
			for t in to_keys:
				output += '\t\t\t'
				if t != to_keys[0]:
					output += 'else '
				output += 'if state(t) == ' + t[2] + ' & '
				params = transition_params[(w+1, state, t[2])]
				added_something = False
				if params == None or params == ():
					output += 'true\n'
					output += '\t\t\t\tthen true\n'
					continue
				if added_something:
					output += ' & '
				for param in params:
					if param != params[0]:
						output += ' & '
					if type(param[0]) == tuple:
						output += param[0][0] + '_' + param[0][1] + '_obs(t) == ' + param[1][0] + '_' + param[1][1] + '_obs(prev(t)) & ' + param[1][0] + '_' + param[1][1] + '_obs(prev(t)) != "' +null_str + '" '
					else:
						output += param[0] + '_obs(t) == ' + param[1] + '_obs(prev(t)) & ' + param[1] + '_obs(prev(t)) != "' + null_str + '" '
					added_something = True
				output += '\n'
				output += '\t\t\t\t'
				output += ' then true\n'
			output += '\t\t\telse false\n'
		output += '\t\telse false\n'
	output += ';\n'

	output_file.write(output)
	output_file.close()

write_file()

param_name = args.param_file
param_lst = [p for p in all_param_types]
param_str = json.dumps(param_lst)
param_file = open(param_name, 'w')
param_file.write(param_str)
param_file.close()
