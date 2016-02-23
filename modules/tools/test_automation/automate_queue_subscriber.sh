#!/usr/bin/env bash

source ./automation_config.sh
source ./util.sh

# make diractory to store results
mkdir $queue_result_dir

# set the message sizes
num_message_sizes=${#message_sizes[@]};
last_message_size_index=`expr $num_message_sizes - 1`;

# set the total number of messages
num_num_messages=${#messages[@]};
last_num_messages_index=`expr $num_num_messages - 1`;

# set the queue scenarios
num_queue_sizes=${#queues[@]};
last_num_queue_index=`expr $num_queue_sizes - 1`;

for i in $(seq $starting_message_size_index $last_num_messages_index); do 
	message_size=${message_sizes[$i]};
	num_messages=${messages[$i]};
	starting_j=0;
	if [ "$i" -eq "$starting_message_size_index" ]; then
		starting_j=$starting_queue_size_index;
	fi
    	for j in $(seq $starting_j $last_num_queue_index); do
		num_queues=${queues[$j]};
		num_clients_for_node_1=$((($num_queues) / $num_nodes));
		num_clients_for_node_2=`expr $num_queues - $num_clients_for_node_1`;
		num_messages_per_client=`expr $num_messages / $num_queues`;

		scenario="Queues message_size ${message_size} nodes:${num_nodes} queues:${num_queues}";	
		logSub "$scenario"

		/usr/bin/expect << EOF
		spawn ./test_queue_subscriber.sh
		
		expect "Number of queues:"
		send -- "$num_queues\n"
		expect "Number of nodes:"
		send -- "$num_nodes\n"
		expect "Number of subscribers for node 1:"
		send -- "$num_clients_for_node_1\n"
		expect "Number of subscribers for node 2:"
		send -- "$num_clients_for_node_2\n"
		expect "Number of messages per subscriber:"
		send -- "$num_messages_per_client\n"
		expect "Message size (1KB):"
		send -- "$message_size\n"
		expect "*#*"
	
EOF
		sleep $approximate_test_run_time;
		error=false;
		if [ "$pubSubInSameMachine" == "false" ]; then
			waitForJmeterStop $error;
		fi
		if [ "$error" == "true" ];then
			break;
		fi
		subscriber_resultfile_name=$queue_result_dir/$message_size\_test_subscriber_nodes_$num_nodes\_queues_$num_queues\_result.txt;
		writeResult $subscriber_resultfile_name $num_messages;
		logSubTestEnd
    	done
    	if [ "$error" == "true" ];then
		logSub "Jmeter did not stop, exiting";
		break;
    	fi
done
