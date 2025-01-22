test:
	set -e
	exec python3 -m unittest discover tests "*_test.py"
run:
	./your_program.sh
