 #!/bin/bash#!/usr/bin/env bash

source ./automation_config.sh
source ./util.sh

# make diractory to store results
mkdir $durable_topics_result_dir

# set the message sizes
num_message_sizes=${#message_sizes[@]};
last_message_size_index=`expr $num_message_sizes - 1`;

# set the total number of messages
num_num_messages=${#messages[@]};
last_num_messages_index=`expr $num_num_messages - 1`;

# set the topic scenarios
num_topic_sizes=${#topics[@]};
last_num_topic_index=`expr $num_topic_sizes - 1`;

# set the topic subs per topic
num_sub_sizes=${#subs[@]};
last_num_sub_index=`expr $num_sub_sizes - 1`;

for i in $(seq $starting_message_size_index $last_num_messages_index); do 
	message_size=${message_sizes[$i]};
	num_messages=${messages[$i]};
	starting_j=0;
	if [ "$i" -eq "$starting_message_size_index" ]; then
		starting_j=$starting_topic_size_index;
	fi
    for j in $(seq $starting_topic_size_index $last_num_topic_index); do
        num_topics=${topics[$j]};
	starting_k=0;
	if [ "$i" -eq "$starting_message_size_index" ]; then
		if [ "$j" -eq "$starting_topic_size_index" ]; then
			starting_k=$starting_clients_per_topic_index;
		fi
	fi
	for k in $(seq $starting_k $last_num_sub_index); do
		num_subs_per_topic=${subs[$k]};
		num_subs=`expr $num_subs_per_topic \\* $num_topics`;
		num_clients_for_node_1=$((($num_subs) / $num_nodes));
		num_clients_for_node_2=`expr $num_subs - $num_clients_for_node_1`;
		num_messages_per_client=`expr $num_messages / $num_subs`;
		
		scenario="Durable topics message size: $message_size nodes:$num_nodes topics:$num_topics subscribers per topic:${subs[$k]}";		
		logSub "$scenario"
		
		/usr/bin/expect << EOF
		spawn ./test_durable_topic_subscriber.sh
		
		expect "Number of topics:"
		send -- "$num_topics\n"
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
		sub_resultfile_name=$durable_topics_result_dir/$message_size\_test_subscriber_nodes_$num_nodes\_durable_topics_$num_topics\_${subs[$k]}\_result.txt;
		writeResult $sub_resultfile_name $num_messages;
		logSubTestEnd
	done
	if [ "$error" == "true" ];then
		break;
	fi
   done
   if [ "$error" == "true" ];then
	logSub "Jmeter did not stop, exiting";
	break;
   fi
done

