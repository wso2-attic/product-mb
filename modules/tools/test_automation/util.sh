#!/usr/bin/env bash

source ./automation_config.sh

#input parameters
#param1 name_of_the_result_text_file in format message_size_queues_result_.txt defines the test case

subscriber_result_file="subscriber_summary.txt";
publisher_result_file="publisher_summary.txt";

function writeResult {

	file_name=$1;
	num_messages=$2;
	percent_10_message_margin=`expr $2 / 10`;
	percent_90_message_margin=`expr $percent_10_message_margin \\* 9`;
	actual_percent_10_messages=$percent_10_message_margin;
	actual_percent_90_messages=$percent_10_message_margin;
	percent_10_time=0;
	percent_90_time=0;
	total_average=0;
	if [[ "$1" == *"subscriber"* ]]; then
  		file_name=$subscriber_result_file;
	else
		file_name=$publisher_result_file;
	fi
	if [ ! -f $file_name ]; then
    		touch $file_name;
		echo "No: of nodes",Message size,Type,"No: of queues/topic",Subscribers per topic,"90% message count","10% message count","90% time","10% time","10%-90% average","Complete average" >> $file_name;
	fi
	while IFS= read line
    	do
		if [[ "$line" == *"summary ="* ]]; then
			count=0;
			for word in $line
				do
			    	word_array[$count]=$word; 
			    	count=$((count+1))
        		done
			previous_message_count=$message_count;
			previous_time_in_sec=$time_in_sec;
			message_count=${word_array[2]}
		
			time_in_sec=${word_array[4]};
			length_time=`expr ${#time_in_sec} - 1`;
			time_in_sec=${time_in_sec:0:$length_time};
			if [ "$message_count" -gt "$percent_10_message_margin" ]; then
				if [ "$previous_message_count" -lt "$percent_10_message_margin" ]; then
					actual_percent_10_messages=$message_count;
					percent_10_time=$time_in_sec;
				fi
			fi
			if [ "$message_count" -gt "$percent_90_message_margin" ]; then
				if [ "$previous_message_count" -lt "$percent_90_message_margin" ]; then
					actual_percent_90_messages=$previous_message_count;
					percent_90_time=$previous_time_in_sec;
				fi
				if [ "$message_count" -eq "$num_messages" ]; then
					total_average=${word_array[6]};
					length_average=`expr ${#total_average} - 2`;
					total_average=${total_average:0:$length_average};
				fi
			fi
		fi
	done < $1

	type="Queue";
	queues=0;
	clients_per_queue=0;
	local num_nodes=25;
	local message_size=fsdfd;
	if [[ "$1" == *"queue"* ]]; then
		re="\/([0-9]+[KM]B).+_nodes_([0-9]+)_queues_([0-9]+)_";
		if [[ $1 =~ $re ]]; then
			queues=${BASH_REMATCH[3]}; 
			nodes=${BASH_REMATCH[2]}; 
			message_size=${BASH_REMATCH[1]};
			clients_per_queue=1;
		fi
	else
		if [[ "$1" == *"durable"* ]]; then
			type="Durable Topic";
		else
			type="Topic";
		fi
		re="\/([0-9]+[KM]B).+_nodes_([0-9]+)_.*topics_([0-9]+)_([0-9]+)";
		if [[ $1 =~ $re ]]; then
			queues=${BASH_REMATCH[3]}; 
			nodes=${BASH_REMATCH[2]}; 
			message_size=${BASH_REMATCH[1]};
			clients_per_queue=${BASH_REMATCH[4]};
		fi
	fi
	num_messages_10_90=`expr $actual_percent_90_messages - $actual_percent_10_messages`;
	duration_10_90=`expr ${percent_90_time%.*} - ${percent_10_time%.*}`;
	average_10_90=`expr $num_messages_10_90 / $duration_10_90`;
	echo -e $nodes,$message_size,$type,$queues,$clients_per_queue,$actual_percent_90_messages,$actual_percent_10_messages,$percent_90_time,$percent_10_time,$average_10_90,$total_average >> $file_name;
	
}

subscriber_log_file="subscriber.log";
publisher_log_file="publisher.log";

function logSub {
	log $subscriber_log_file "$1"
}

function logPub {
	log $publisher_log_file "$1"
}

function logSubTestEnd {
	log $subscriber_log_file
	log $subscriber_log_file "$finished_with_succsess_constant"
	log $subscriber_log_file
	log $subscriber_log_file "-----------------------------------------------------"
	log $subscriber_log_file
}

function logPubTestEnd {
	log $publisher_log_file
	log $publisher_log_file "$finished_with_succsess_constant"
	log $publisher_log_file
	log $publisher_log_file "-----------------------------------------------------"
	log $publisher_log_file
}

function log {
	echo "$2" >> $1;
	echo "$2";
}

finished_with_succsess_constant="finished scenario with succsess";
function waitForJmeterStop {
	error_state=$1;
	java_pid=$(pgrep jmeter);
	retries=0;
	while [ "$java_pid" != '' ];do
		if [ "$retries" -gt "$max_retry_attempts" ]; then
			eval error=true;
			break;
		fi
		echo "Waiting for jmeter to stop";
		sleep $retry_interval
		java_pid=$(pgrep "jmeter");
		retries=`expr $retries + 1`;
	done
}
