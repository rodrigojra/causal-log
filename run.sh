#!/usr/bin/env bash

set -x -e

#time mvn clean compile assembly:single

for workload in "100000"
do
	#time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -g "$(echo "$workload")" 0 false
	time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -g "$workload" 0 false true
done


for workload in "100000"
do
	#time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -g "$(echo "$workload")" 0 false
	time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -r "sequential" "1"  "$workload"-log-without-conflict.dat 1000000
done

for model in "graph" "attached"
do    	
	for workload in "100000"
	do
		for server_threads in "1" "2" "4" "8" 
		do    
			time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -r "$model" "$server_threads" "$workload"-log-without-conflict.dat 1000000
    	done
	done
done