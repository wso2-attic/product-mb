#Bash scripts for performance test automation

This provides scripts for running a whole set of test cases for queues, topics and durable topics as well as scripts for running single test scenarios.

Following are the steps for running a whole set of test scenarios

Configuration:
- For basic confgurations, change the entries in 'config.sh'
- Modify the 'defaultjndi.properties' file to add jmeter configurations such as the node details and the jndi properties
- 'automation\_config.sh' in order to add configurations such as the test scenarios which are required for test automation

Running:
- For a queue scenario, run 'automate\_queue\_subscriber.sh' on the subscriber node and 'automate\_queue_publisher.sh' in the publisher node
- For a topic scenario, run 'automate\_topic\_subscriber.sh' on the subscriber node and 'automate\_topic_publisher.sh' in the publisher node
- For a durable topic scenario, run 'automate\_durable\_topic\_subscriber.sh' on the subscriber node and 'automate\_durable\_topic\_publisher.sh' in the publisher node

Follow the below steps inorder to run a single test case

- For basic confgurations, change the entries in 'config.sh'
- Modify the 'defaultjndi.properties' file to add jmeter configurations such as the node details and the jndi properties
- Run the test\_queue/topic/durable\_topic\_.sh in the client nodes and enter values when prompted

Note: Allocate around 4GB memory for jmeter in order to prevent it from going out of memory.
