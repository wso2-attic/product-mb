#!/usr/bin/env bash

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

# set the topic pubs per topic
num_pub_sizes=${#pubs[@]};
last_num_pub_index=`expr $num_pub_sizes - 1`;

num_pub_tps=${#publisher_tps[@]};

expected_num_pub_tps=`expr $num_topic_sizes \\* $num_pub_sizes \\* $num_num_messages`;

if [ "$num_pub_tps" -ne "$expected_num_pub_tps" ]; then
  echo "Incorrect message tps size";
else
	for i in $(seq $starting_message_size_index $last_num_messages_index); do 
		message_size=${message_sizes[$i]};
		num_messages=`expr ${messages[$i]} / ${pubs[$k]}`;
		starting_j=0;
		if [ "$i" -eq "$starting_message_size_index" ]; then
		starting_j=$starting_topic_size_index;
		fi
	    for j in $(seq $starting_j $last_num_topic_index); do
		num_topics=${topics[$j]};
		starting_k=0;
		if [ "$i" -eq "$starting_message_size_index" ]; then
			if [ "$j" -eq "$starting_topic_size_index" ]; then
				starting_k=$starting_clients_per_topic_index;
			fi
		fi
		for k in $(seq $starting_k $last_num_pub_index); do
			num_pubs=`expr $num_topics \\* ${pubs[$k]}`; 
			num_clients_for_node_1=$((($num_pubs) / $num_nodes));
			num_clients_for_node_2=`expr $num_pubs - $num_clients_for_node_1`;
			num_messages_per_client=$((($num_messages) / $num_pubs));
			pub_tps_index=$(((($i) * $num_pub_sizes) + $j));	
			pub_tps_col=$(((($j) * $num_pub_sizes) + $k));
			elements_in_a_row=$((($num_pub_sizes) * $num_topic_sizes));			
			pub_tps_row=$((($i) * $elements_in_a_row));
			pub_tps_index=$(((pub_tps_row) + pub_tps_col));

			scenario="Durable topics message size:${message_size} nodes:${num_nodes} topics:${num_topics} publishers per topic:${pubs[$k]}";		
			logPub "$scenario"

			/usr/bin/expect << EOF
			spawn ./test_durable_topic_publisher.sh

			expect "Number of topics:"
			send -- "$num_topics\n"
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
			publishers_resultfile_name=$durable_topics_result_dir/$message_size\_test_publisher_nodes_$num_nodes\_durable_topics_$num_topics\_${pubs[$k]}\_result.txt;
			writeResult $publishers_resultfile_name $num_messages;
			logPubTestEnd
		done
		if [ "$error" == "true" ];then
			break;
		fi
	   done
	   if [ "$error" == "true" ];then
		logPub "Jmeter did not stop, exiting";
		break;
	   fi
	done

fi
