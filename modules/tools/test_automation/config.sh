#!/usr/bin/env bash

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

jmeterBinary="/home/sasikala/Documents/tools/apache-jmeter-2.13/bin/jmeter"

subscriber_outfile_name="/tmp/test_subscriber.jmx"
publisher_outfile_name="/tmp/test_publisher.jmx"

jndi_location="${script_directory}/defaultjndi.properties"
