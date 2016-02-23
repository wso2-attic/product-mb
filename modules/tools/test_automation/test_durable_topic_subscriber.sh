#!/usr/bin/env bash

# Requires At least Bash 4 to run this 

source ./config.sh
source ./automation_config.sh

declare -A subscribers=()
declare -A nodeUrls=()

total_subscribers=0

echo -n "Number of topics: "
read queues

echo -n "Number of nodes: "
read nodes

for i in $(seq 1 $nodes); do 

    echo -n "Number of subscribers for node $i: "
    read subscribers[$i]
    total_subscribers=$((total_subscribers + ${subscribers[$i]}))
done

echo -n "Number of messages per subscriber: "
read messagesPerSubscriber

echo -n "Message size (1KB): "
read messageSize

#set the jmeter.log output filepath.
subscribers_per_topic=`expr $total_subscribers / $queues`;
subscriber_resultfile_name=$durable_topics_result_dir/$messageSize\_test_subscriber_nodes_$nodes\_durable_topics_$queues\_$subscribers_per_topic\_result.txt;

# Start Subscriber script 
function startScript {
    maximumPublisherThroughput=$((50000 * 60))

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
    clientIDNumber=1
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
        <stringProp name="jms.durableSubscriptionId">test-client-${clientIDNumber}</stringProp>
       </SubscriberSampler>
       </hashTree>
EOF

            queueNumber=$((((queueNumber) % queues) + 1))
            clientIDNumber=$((clientIDNumber + 1))
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

if [ "$is_mock_run" == "false" ]; then
	nohup $jmeterBinary -n -t $subscriber_outfile_name > $subscriber_resultfile_name &

	echo -e "\nJmeter Log output ...\n"
	tail -f $subscriber_resultfile_name
fi
