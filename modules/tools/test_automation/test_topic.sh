#!/usr/bin/env bash

# Requires At least Bash 4 to run this 

source ./config.sh

declare -A publishers=()
declare -A subscribers=()
declare -A nodeUrls=()

total_publishers=0
total_subscribers=0

echo -n "Number of topics: "
read queues

echo -n "Number of nodes: "
read nodes

#echo -n "Number of publishers: "
#read total_publishers

#echo -n "Number of subcribers: "
#read total_subscribers

for i in $(seq 1 $nodes); do 
#     echo -n "Node $i url: "
#     read nodeUrls[$i]
    
    echo -e "\n=============== Node ${i} ================="
    echo -n "Number of publishers for node $i: "
    read publishers[$i]
    total_publishers=$((total_publishers + ${publishers[$i]}))

    echo -n "Number of subscribers for node $i: "
    read subscribers[$i]
    total_subscribers=$((total_subscribers + ${subscribers[$i]}))
done

echo

echo -n "Number of messages per subscriber: "
read messagesPerSubscriber

echo -n "Number of messages per publisher: "
read messagesPerPublisher

echo -n "Maximum publisher TPS: "
read maximumPublisherTPS

echo -n "Message size (1KB): "
read messageSize

# Start Subscriber script 
function startScript {
    maximumPublisherThroughput=$((maximumPublisherTPS * 60))

    cat > $1 <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="2.3">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="MB $2 Test" enabled="true">
      <stringProp name="TestPlan.comments"></stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
      <stringProp name="TestPlan.user_define_classpath"></stringProp>
    </TestPlan>
    <hashTree>
      <ConstantThroughputTimer guiclass="TestBeanGUI" testclass="ConstantThroughputTimer" testname="Constant Throughput Timer" enabled="true">
        <stringProp name="calcMode">all active threads</stringProp>
        <doubleProp>
          <name>throughput</name>
          <value>$maximumPublisherThroughput</value>
          <savedValue>0.0</savedValue>
        </doubleProp>
      </ConstantThroughputTimer>
      <hashTree/>
EOF
}

# Add subscribers
function addSubscribers {
    queueNumber=1
    aggregateSize=1
    aggregatedSamples=$(( messagesPerSubscriber / aggregateSize ))
    for node_i in $(seq 1 $nodes); do 
        for i in $(seq 1 ${subscribers[$node_i]}); do 
        
        cat >> $1 <<EOF

    <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Subscriber N${node}-${i}" enabled="true">
      <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
      <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
        <boolProp name="LoopController.continue_forever">false</boolProp>
        <intProp name="LoopController.loops">$aggregatedSamples</intProp>
      </elementProp>
      <stringProp name="ThreadGroup.num_threads">1</stringProp>
      <stringProp name="ThreadGroup.ramp_time">0</stringProp>
      <longProp name="ThreadGroup.start_time">1436164345000</longProp>
      <longProp name="ThreadGroup.end_time">1436164345000</longProp>
      <boolProp name="ThreadGroup.scheduler">false</boolProp>
      <stringProp name="ThreadGroup.duration"></stringProp>
      <stringProp name="ThreadGroup.delay">2</stringProp>
      <boolProp name="ThreadGroup.delayedStart">true</boolProp>
    </ThreadGroup>
    <hashTree>
      <SubscriberSampler guiclass="JMSSubscriberGui" testclass="SubscriberSampler" testname="JMS Subscriber ${i}" enabled="true">
        <stringProp name="jms.jndi_properties">false</stringProp>
        <stringProp name="jms.initial_context_factory">org.wso2.andes.jndi.PropertiesFileInitialContextFactory</stringProp>
        <stringProp name="jms.provider_url">$jndi_location</stringProp>
        <stringProp name="jms.connection_factory">TopicConnectionFactory${node_i}</stringProp>
        <stringProp name="jms.topic">MyTopic${queueNumber}</stringProp>
        <stringProp name="jms.security_principle"></stringProp>
        <stringProp name="jms.security_credentials"></stringProp>
        <boolProp name="jms.authenticate">false</boolProp>
        <stringProp name="jms.iterations">$aggregateSize</stringProp>
        <stringProp name="jms.read_response">true</stringProp>
        <stringProp name="jms.client_choice">jms_subscriber_receive</stringProp>
        <stringProp name="jms.timeout">300000</stringProp>
        <stringProp name="jms.durableSubscriptionId"></stringProp>
       </SubscriberSampler>
       </hashTree>
EOF

            queueNumber=$((((queueNumber) % queues) + 1))
        done
    done
}


# Add publisher
function addPublishers {
    queueNumber=1
    nodeNumber=1
    aggregateSize=1
    aggregatedSamples=$(( messagesPerPublisher / aggregateSize ))

    data_file="${script_directory}/data/${messageSize}.txt"

    for node_i in $(seq 1 $nodes); do 
        for i in $(seq 1 ${publishers[$node_i]}); do 
        
            cat >> $1 <<EOF
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Publisher N${node}-${i}" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">$aggregatedSamples</stringProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">1</stringProp>
        <stringProp name="ThreadGroup.ramp_time">0</stringProp>
        <longProp name="ThreadGroup.start_time">1436355944000</longProp>
        <longProp name="ThreadGroup.end_time">1436355944000</longProp>
        <boolProp name="ThreadGroup.scheduler">false</boolProp>
        <stringProp name="ThreadGroup.duration"></stringProp>
        <stringProp name="ThreadGroup.delay"></stringProp>
      </ThreadGroup>
      <hashTree>
        <PublisherSampler guiclass="JMSPublisherGui" testclass="PublisherSampler" testname="JMS Publisher N${node}-${i}" enabled="true">
          <stringProp name="jms.jndi_properties">false</stringProp>
          <stringProp name="jms.initial_context_factory">org.wso2.andes.jndi.PropertiesFileInitialContextFactory</stringProp>
          <stringProp name="jms.provider_url">$jndi_location</stringProp>
          <stringProp name="jms.connection_factory">TopicConnectionFactory${node_i}</stringProp>
          <stringProp name="jms.topic">MyTopic${queueNumber}</stringProp>
          <stringProp name="jms.security_principle"></stringProp>
          <stringProp name="jms.security_credentials"></stringProp>
          <stringProp name="jms.text_message"></stringProp>
          <stringProp name="jms.input_file">$data_file</stringProp>
          <stringProp name="jms.random_path"></stringProp>
          <stringProp name="jms.config_choice">jms_use_file</stringProp>
          <stringProp name="jms.config_msg_type">jms_text_message</stringProp>
          <stringProp name="jms.iterations">$aggregateSize</stringProp>
          <boolProp name="jms.authenticate">false</boolProp>
          <elementProp name="jms.jmsProperties" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
            <collectionProp name="Arguments.arguments"/>
          </elementProp>
        </PublisherSampler>
        <hashTree/>
      </hashTree>
EOF

            queueNumber=$((((queueNumber) % queues) + 1))
        done
    done
}


function endScript {
    cat >> $1 <<EOF
      <Summariser guiclass="SummariserGui" testclass="Summariser" testname="Generate Summary Results" enabled="true"/>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
EOF
}

# Generating Jmeter script for subscriber

startScript $subscriber_outfile_name "Subscriber"
addSubscribers $subscriber_outfile_name
endScript $subscriber_outfile_name

# Generating Jmeter script for Publisher

startScript $publisher_outfile_name "Publisher"
addPublishers $publisher_outfile_name
endScript $publisher_outfile_name

nohup $jmeterBinary -n -t $subscriber_outfile_name > /tmp/test_subscriber_result.txt &
sleep 10
nohup $jmeterBinary -n -t $publisher_outfile_name > /tmp/test_publisher_result.txt &

echo -e "\nJmeter Log output ...\n"
tail -f /tmp/test_subscriber_result.txt -f /tmp/test_publisher_result.txt
