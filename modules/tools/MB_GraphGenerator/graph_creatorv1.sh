#!/bin/bash

#Path for the directory where text files are stored
FILES=~/Learning/FirstTask/MB_GraphGen/Texts/*
DataFiles=~/Learning/FirstTask/MB_GraphGen/DataFiles/
Graphs=~/Learning/FirstTask/MB_GraphGen/Graphs/

#New file path for the intermediate ops
newFile=~/Learning/FirstTask/MB_GraphGen/searched.txt
newFile1=~/Learning/FirstTask/MB_GraphGen/searched1.txts
for f in $FILES
do 

  # take action on each file. $f store current file name
  #just the name without path
  filename="${f##*/}"
  ins=INST
  #variable to keep length of the avgTime array
  len=0
  len1=0
  
  #print the file name        
  newDnam="$DataFiles$filename"
  newDnam1="$DataFiles$ins$filename"
 
  > $newDnam
  > $newDnam1
 #get index of "."
  val=$( echo $(expr index "$filename" .))
  val=$((val-1))
  #get new name as path without extension
  newnam=$( echo $filename | cut -c1-"$val")
  na1=$(echo '.png')
  
  #add extension .png 
  newPnam="$Graphs$newnam$na1"
  
   > $newPnam
   
   #get the files begin with summary and creare file searched.txt
        > $newFile
        egrep "^summary =" $f > $newFile
        
        > $newFile1
        egrep "^summary \+" $f > $newFile1
        #sed 's/^"summary ="/""/g' $newFile1 > $newFile1
        
        
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
                avgTime[$len]=${array[6]};
                sampleNum[$len]=$len;
                #write graph data into file
                echo "$len ${array[6]}" >> $newDnam
                len=$((len+1))
               # echo "Value of 7th element in my array : ${array[6]} "
        done <"$newFile"
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
                avgTime[$len1]=${array[6]};
                sampleNum[$len1]=$len1;
                #write graph data into file
                echo "$len1 ${array[6]}" >> $newDnam1
                len1=$((len1+1))
               # echo "Value of 7th element in my array : ${array[6]} "
        done <"$newFile1"
        cat << __EOF | gnuplot
set xlabel "SAMPLE_NUMBER"
set ylabel "AVERAGE_VALUES"        
set term png font arial 14 size 1500,800
set output "$newPnam"
plot "$newDnam" with lines title "Cumulative_Average", \
"$newDnam1" with lines title "Instant_Average"
__EOF
done
