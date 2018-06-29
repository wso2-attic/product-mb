## WSO2 MB 3.0.0 to 3.2.0 Migration Instructions

### Build instructions

Run mvn clean install from the migration tester tool folder. In the target folder the migration test tool zipped 
package will be created.


### Data migration tester tool

Migration tester tool is intended to be used to populate test data to WSO2 MB 3.0.0 and WSO2 MB 3.2.0
setups before and after migration. After that the tool can be used to test whether the migration was 
successful using the test data. After testing all the test data can be cleaned up using the tool itself.

Before running the data migration tester tool the test plan can be updated. Test plan can be
updated using the <MIGRATION_PACK_HOME>/tools/migration-tool-1.0-SNAPSHOT/conf/test-plan.xml file. 
This file contains the users, roles, subscriptions, topics and queues needed for migration testing.

Migration tester tool is executed using the executable migration-tester-tool-1.0-SNAPSHOT located at
<MIGRATION_PACK_HOME>/tools/migration-tool-1.0-SNAPSHOT directory. 

There are four commands to run this tool
     i. before-migration-setup  - all the users and roles are in the test plan are created in the broker node. 
                                  Message producers and consumers set under before-migration step in the 
                                  test-plan.xml is executed.
                                   
    ii. after-migration-setup   - Message producers and consumers set under before-migration step in 
                                  the test-plan.xml is executed.    

   iii. verify                  - Verifies whether all the roles, users, queues, topics and subscribers 
                                  and messages are properly migrated. 

    iv. cleanup                 - Removes all the resources created through the tester tool from the 
                                  MB server
                                  

#### Configurations
    
    a) <MIGRATION_PACK_HOME>/tools/migration-tool-1.0-SNAPSHOT/conf/jndi.properties file contains details required 
    to connect to the broker node to create queues and topics. Update this file according to the MB deployment.
        
    b) <MIGRATION_PACK_HOME>/tools/migration-tool-1.0-SNAPSHOT/conf/test-plan.xml contains information related to
        - management console url
        - admin user details
        - users to be created
        - role to be created 
        - queues and topics data population related information.
        
    c) Within test-plan.xml, client-trustore element refers to the client trustore used in WSO2 MB nodes. Depending
       on the deployment, point to the relevant client-trustore.jks file and update the password accordingly.
       You can find the client trustore in <MB_HOME>/repository/resources/security/ directory of the MB node.
    
    NOTE: queues and topics defined in test-plan.xml should be added to the jndi.properties file as well.

