#!/bin/bash
#=============================================================================
# Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
#
#   WSO2 Inc. licenses this file to you under the Apache License,
#   Version 2.0 (the "License"); you may not use this file except
#   in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied. See the License for the
#   specific language governing permissions and limitations
#   under the License.
#=============================================================================
       
echo "Please Enter Your Message ID:"
read line
bit_string=$(echo "obase=2;$line" | bc)
length_bit_string=${#bit_string}
#---------------------------------------------------
#Check whether message id is valid or not
#---------------------------------------------------
if [ $length_bit_string -lt "19" ]
then
    echo "INVALID MESSAGE ID"
else
    end_index_timestamp=$( echo "$(($length_bit_string-18))")

    start_index_id=$( echo "$(($end_index_timestamp+1))")
    end_index_id=$( echo "$(($length_bit_string-10))")

    start_index_offset=$( echo "$(($end_index_id+1))")
    end_index_offset=$length_bit_string

    time_stamp_bits=$( echo $bit_string | cut -c1-"$end_index_timestamp")
    time_stamp_dec=$( echo "$((2#$time_stamp_bits))")
    unix_time_stamp=$( echo "$(($time_stamp_dec/1000))")

    time_stamp_offset=$( echo "$((41 * 365 * 24 * 60 * 60))")
    real_time_stamp=$( echo "$(($unix_time_stamp+$time_stamp_offset))")

    date_value=$( echo "$(date -d @$real_time_stamp)")

    node_id_bits=$( echo $bit_string | cut -c"$start_index_id"-"$end_index_id")
    node_id_dec=$( echo "$((2#$node_id_bits))")

    offset_bits=$( echo $bit_string | cut -c"$start_index_offset"-"$end_index_offset")
    offset_dec=$( echo "$((2#$offset_bits))")

    echo "DATE:   $date_value"
    echo "ID:     $node_id_dec"
    echo "OFF SET:$offset_dec"         
fi