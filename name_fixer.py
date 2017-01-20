import os
import argparse
import re

parser = argparse.ArgumentParser(description="Rename blog output according to CP8 convention.")
parser.add_argument('-i', '--input_dir',
					help="Path to dir containing outputs.")
args = parser.parse_args()

base_dir = args.input_dir

dirs = [d for d in os.listdir(base_dir) if not os.path.isfile(d)]

def full_match(match, s):
	return match != None and match.group(0) == s

patn = 'user(\d\d)'
prog = re.compile(patn)
output_dirs = [d for d in dirs if full_match(prog.match(d), d)]

for d in output_dirs:
	num = prog.match(d).group(1)
	f = os.listdir(os.path.join(base_dir, d))[0] # assume one file in dir
	os.rename(os.path.join(base_dir, d, f), os.path.join(base_dir, d, 'USER-' + num + '.map.json'))
