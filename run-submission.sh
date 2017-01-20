#!/bin/bash

for var in "$@"
do
	python test_writer.py -i just_model.blog -o models -t "$var" -p "params.txt"
	blog/dblog "models/$var.blog" > "$var.out"
	echo "finished blog inference for $var"
	python json_maker_final.py -i "$var.out" -o "out/$var" -t "$var"
	echo "finished processing $var"
done
python name_fixer.py -i "out"
