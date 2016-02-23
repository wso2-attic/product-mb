#!/usr/bin/env bash

#Jmeter summary outputs will be stored in the following directories
queue_result_dir="queues";
topic_result_dir="topics";
durable_topics_result_dir="durable_topics";

#As of now, the number of nodes is restriced to 2
num_nodes=2;



#queue configs
#Message sizes to be tested
#Make sure the data files with the metioned sizes exist in data/ directory
message_sizes=("1KB" "10KB" "100KB" "1MB");

#The aggregated number of messages the subscribers will receive for each of the above message size. There is a 1:1 mapping between the message size and the number of messages
messages=(1000000 1000000 1000000 1000000);

#The queue sizes to be tested. For each of the above message size, all of the queue sizes will be tested
queues=(5 10 20 50 100);

#Define the starting test case. Incase if the presvious tests run failed in the middle you can resume tests by setting the below parameters. An example configuration is explained below.
#Setting 
#	starting_message_size_index=1;
#	starting_queue_size_index=2; 
#will resume the tests from 10KB->20 queues scenario 
starting_message_size_index=0;
starting_queue_size_index=0;

#queue publisher tpss
#Should have the length of 'size_of_message_sizes * number_of_queue_sizes'
#Given in the format of publisher_tps=(message_size1_queue_size_1, message_size1_queue_size_2 message_size1_queue_size_3 message_size2_queue_size_1 message_size2_queue_size_2 message_size2_queue_size_3 ...) 
publisher_tps=(2000 1500 2000 2000 1000 5000 1000 1000 1000 500 120 120 120 120 100 12 12 12 12 10);



#topic configs
#Message sizes to be tested
#message_sizes=("1KB" "10KB" "100KB" "1MB");

#The aggregated number of messages the subscribers will receive for each of the above message size. There is a 1:1 mapping between the message size and the number of messages
#messages=(1000000 1000000 1000000 1000000);

#The topic sizes to be tested. For each of the above message size, all of the topic sizes will be tested
#topics=(5 10 20);

#Number of subscribers/publishers per topic for each of the topic size. For a single topic size, all of the below pub/sub sizes will be tested
#pubs=(1 5);
#subs=(1 5);

#Define the starting test case. Incase if the presvious tests run failed in the middle you can resume tests by setting the below parameters. An example configuration is explained below.
#Setting 
#	starting_message_size_index=1;
#	starting_topic_size_index=2; 
#	starting_clients_per_topic_index=1;
#will resume the tests from the scenario of messagesize:10KB number of topics:10 number of pub/sub per topic:5 
#starting_message_size_index=1;
#starting_topic_size_index=2;
#starting_clients_per_topic_index=1;

#Topic publisher tpss
##Should have the length of 'size_of_message_sizes * number_of_topic_sizes * number_pub/sub_sizes_per_topic'
#Given in the format of publisher_tps=(message_size1_topic_size_1_pub_size1 message_size1_topic_size_1_pub_size2 message_size1_topic_size_1_pub_size3 message_size1_topic_size_2_pub_size1 message_size1_topic_size_2_pub_size2 message_size1_topic_size_2_pub_size3 ...) 
#publisher_tps=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24);



#Configurations to wait untile jmeter is stopped to run the next test scenario
#Not in use, always set this false
pubSubInSameMachine=true;
#The time interval between the checks for jmeter run
retry_interval=60;
#maximum number of retries before exiting
max_retry_attempts=25;
#Maximum test run time for a test scenario. This defines how long the program will wait before starting the next test scenario. Therefore, it is important to set this with a sufficiently large value 
approximate_test_run_time=1800;

# If this is a mock run, jmeter will not be started. Rather the test resuslts from previous test runs will be taken for summary calculation
is_mock_run=false;
