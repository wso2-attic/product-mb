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
FILES=PATH-TO-DATAFILES DIRECTORY/*          #Path for the directory where text files are stored/*
DATAFILES=PATH-TO-PROJECT-FOLDER/graph_generator/DataFiles/   #Path for the directory where data files should be stored/
GRAPHS=PATH-TO-PROJECT-FOLDER/graph_generator/Graphs/         #Path for the directory where .png files should be stored/

new_file=PATH-TO-PROJECT-FOLDER/graph_generator/searched.txt  #New file path for the intermediate operations
new_file1=PATH-TO-PROJECT-FOLDER/graph_generator/searched1.txts
for f in $FILES
do 

  # take action on each file. $f store current file name
  #just the name without path
  filename="${f##*/}"
  ins=INST
  #variable to keep length of the avgTime array
  array_length=0
  array1_length=0
  
  #print the file name        
  newDnam="$DATAFILES$filename"
  newDnam1="$DATAFILES$ins$filename"
 
  > $newDnam
  > $newDnam1
 #get index of "."
  val=$( echo $(expr index "$filename" .))
  val=$((val-1))
  #get new name as path without extension
  newnam=$( echo $filename | cut -c1-"$val")
  na1=$(echo '.png')
  
  #add extension .png 
  newPnam="$GRAPHS$newnam$na1"
  
   > $newPnam
   
   #get the files begin with summary and creare file searched.txt
        > $new_file
        egrep "^summary =" $f > $new_file
        
        > $new_file1
        egrep "^summary \+" $f > $new_file1
        #sed 's/^"summary ="/""/g' $new_file1 > $new_file1
        
        
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
        done <"$new_file"
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
                avgTime[$array1_length]=${array[6]};
                sampleNum[$array1_length]=$array1_length;
                #write graph data into file
                echo "$array1_length ${array[6]}" >> $newDnam1
                array1_length=$((array1_length+1))
               # echo "Value of 7th element in my array : ${array[6]} "
        done <"$new_file1"
        cat << __EOF | gnuplot
set xlabel "SAMPLE_NUMBER"
set ylabel "AVERAGE_VALUES"        
set term png font arial 14 size 1500,800
set output "$newPnam"
plot "$newDnam" with lines title "Cumulative_Average", \
"$newDnam1" with lines title "Instant_Average"
__EOF
done
