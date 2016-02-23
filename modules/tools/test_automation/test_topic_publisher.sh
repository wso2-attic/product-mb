#!/usr/bin/env bash

# Requires At least Bash 4 to run this 

source ./config.sh
source ./automation_config.sh

declare -A publishers=()
declare -A nodeUrls=()

total_publishers=0

echo -n "Number of topics: "
read queues

echo -n "Number of nodes: "
read nodes

for i in $(seq 1 $nodes); do 

    echo -n "Number of publishers for node $i: "
    read publishers[$i]
    total_publishers=$((total_publishers + ${publishers[$i]}))

done

echo -n "Number of messages per publisher: "
read messagesPerPublisher

echo -n "Maximum publisher TPS: "
read maximumPublisherTPS

echo -n "Message size (1KB): "
read messageSize

#set the jmeter.log output filepath.
publishers_per_topic=`expr $total_publishers / $queues`;
publishers_resultfile_name=$topic_result_dir/$messageSize\_test_publisher_nodes_$nodes\_topics_$topics\_$publishers_per_topic\_result.txt;

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

# Generating Jmeter script for Publisher

startScript $publisher_outfile_name "Publisher"
addPublishers $publisher_outfile_name
endScript $publisher_outfile_name

if [ "$is_mock_run" == "false" ]; then
	nohup $jmeterBinary -n -t $publisher_outfile_name > $publishers_resultfile_name &

	echo -e "\nJmeter Log output ...\n"
	tail -f $publishers_resultfile_name
fi
