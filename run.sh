#!/usr/bin/env bash

set -x -e

for server_threads in "2" "4" "8" 
do
#    for workload in "5000" "10000" "20000" "50000" "500000"
#    do
	    formated_threads="$(printf '%03d' "$server_threads")"
	    formated_workload="$(echo "$workload / 1000" | bc)"
	    formated_workload="$(printf '%03.f' "$formated_workload")"
	    #results_path="results/final-a2/${formated_threads}server-001client-100sparseness-000conflict-50ops-001k-${formated_workload}us-${scheduler_type}"
	
#	    if [ ! -d "$results_path" ]
#	    then
#	        time ansible-playbook -i hosts run_experiment.yaml \
#	            -e server_threads="$server_threads" \
#	            -e scheduler_type="$scheduler_type" \
#	            -e results_path="$results_path" \
#	            -e cost_per_op_ns="$workload" \
#	            -t run,stop,fetch
	            
	            time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -r graph "$server_threads" recovery-w-50000-conflict-0.0.dat
#	    fi

 #   done
done
