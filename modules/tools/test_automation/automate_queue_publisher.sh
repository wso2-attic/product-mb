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
queues=(5 10 20 50 100);
num_queue_sizes=${#queues[@]};
last_num_queue_index=`expr $num_queue_sizes - 1`;

num_pub_tps=${#publisher_tps[@]};

expected_num_pub_tps=`expr $num_queue_sizes \\* $num_num_messages`;

if [ "$num_pub_tps" -ne "$expected_num_pub_tps" ]; then
        echo "Incorrect message tps size"
else

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
			pub_tps_index=$(((($i) * $num_queue_sizes) + $j))

			scenario="Queues message_size ${message_size} nodes:${num_nodes} queues:${num_queues}";		
			logPub "$scenario"
	
			/usr/bin/expect << EOF
			spawn ./test_queue_publisher.sh
			expect "Number of queues:"
			send -- "$num_queues\n"
			expect "Number of nodes:"
			send -- "$num_nodes\n"
			expect "Number of publishers for node 1:"
			send -- "$num_clients_for_node_1\n"
			expect "Number of publishers for node 2:"
			send -- "$num_clients_for_node_2\n"
			expect "Number of messages per publisher:"
			send -- "$num_messages_per_client\n"
			expect "Maximum publisher TPS:"
			send -- "${publisher_tps[$pub_tps_index]}\n"
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
			publisher_resultfile_name=$queue_result_dir/$message_size\_test_publisher_nodes_$num_nodes\_queues_$num_queues\_result.txt;
			writeResult $publisher_resultfile_name $num_messages
			logPubTestEnd
	   	done
	   	if [ "$error" == "true" ];then
			logPub "Jmeter did not stop, exiting";
			break;
	   	fi
	done

fi
