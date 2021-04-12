#!/usr/bin/env bash

set -x -e

#time mvn clean compile assembly:single

#for workload in "5000" "10000" "20000" "50000" "500000"
#do
#	time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -g "$(echo "$workload")" 0 false
#done


for workload in "5000" "10000" "20000" "50000" "500000"
do
	for server_threads in "2" "4" "8" 
	do    
		time java -Xmx5g -jar target/causalLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar -r attached "$(echo "$server_threads")" recovery-w-"$(echo "$workload")"-conflict-0.0.dat
    done
done
