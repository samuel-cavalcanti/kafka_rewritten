test: 
	set -e
	KAFKA_LOG="kafka.log" KAFKA_LOG_DIR="./logs" exec python3 -m unittest discover tests "*_test.py"
run:
	./your_program.sh
