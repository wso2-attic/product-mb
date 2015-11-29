
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
INPUT_FILES=PATH-TO-DATAFILES DIRECTORY/*          #Path for the directory where text files are stored/*
DATAFILES=PATH-TO-PROJECT-DIRECTORY-LOCATION/graph_generator/DataFiles/   #Path for the directory where data files should be stored/
interim_file1=PATH-TO-PROJECT-DIRECTORY-LOCATION/graph_generator/searchedC1.txts #New file path for the intermediate operations
#--------------------------------------------------------
# take action on each file and $f store current file name
#--------------------------------------------------------
for f in $INPUT_FILES
do 
    filename="${f##*/}"
    avg_time_length=0                #variable to keep length of the avg_time array
    sample_length=0
    data_file_path="$DATAFILES$filename"
    > $data_file_path
    index_num=$( echo $(expr index "$filename" .))          #get index of "."
    index_num=$((index_num-1))
    
    new_file_nam=$( echo $filename | cut -c1-"$index_num")  #get new name as path without extension
    > $interim_file1
    egrep "^summary =" $f > $interim_file1                   #get the files begin with summary and creare file searched.txt
    #-------------------------------------
    #read the file line by line
    #------------------------------------
    while IFS= read line
    do
       
        replaced_line="${line/\/s/''}"         #replace /s in each line    
        count=0
        for word in $replaced_line
        do
            word_array[$count]=$word; 
            count=$((count+1))
        done
        avg_time[$avg_time_length]=$word_array[6]};
        sample_num[$avg_time_length]=$avg_time_length;
       
        echo "$avg_time_length ${word_array[6]}" >> $data_file_path         #write graph data into file
        avg_time_length=$((avg_time_length+1))
    done <"$interim_file1"
        cat << __EOF | gnuplot
set xlabel "SAMPLE_NUMBER"
set ylabel "AVERAGE_VALUES"        
set terminal dumb
plot "$data_file_path" with lines title "Cumulative_Average"
__EOF
done

