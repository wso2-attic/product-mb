#!/bin/bash
#
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
 #   KIND, either express or implied.  See the License for the
 #   specific language governing permissions and limitations
 #   under the License.
#
FILES=~/Learning/FirstTask/graph_generator/Texts/*          #Path for the directory where text files are stored/*
DATAFILES=~/Learning/FirstTask/graph_generator/DataFiles/   #Path for the directory where data files should be stored/
new_file1=~/Learning/FirstTask/graph_generator/searchedC1.txts #New file path for the intermediate operations
for f in $FILES
do 

  # take action on each file. $f store current file name
  #just the name without path
  filename="${f##*/}"
  #variable to keep length of the avgTime array
  array_length=0
  array1_length=0
  
  #print the file name        
  newDnam="$DATAFILES$filename"
 
  > $newDnam
 #get index of "."
  val=$( echo $(expr index "$filename" .))
  val=$((val-1))
  #get new name as path without extension
  newnam=$( echo $filename | cut -c1-"$val")
    
   #get the files begin with summary and creare file searched.txt
        > $new_file1
        egrep "^summary =" $f > $new_file1
        
  #read the file line by line
        while IFS= read line
            do
                #replace /s in each line    
                newLine="${line/\/s/''}"
                #echo $newLine
                count=0
                
                    for word in $newLine
                        do
                        array[$count]=$word; 
                        count=$((count+1))
                    done
                avgTime[$array_length]=${array[6]};
                sampleNum[$array_length]=$array_length;
                #write graph data into file
                echo "$array_length ${array[6]}" >> $newDnam
                array_length=$((array_length+1))
               # echo "Value of 7th element in my array : ${array[6]} "
        done <"$new_file1"
        cat << __EOF | gnuplot
set xlabel "SAMPLE_NUMBER"
set ylabel "AVERAGE_VALUES"        
set terminal dumb
plot "$newDnam" with lines title "Cumulative_Average"
__EOF
done
