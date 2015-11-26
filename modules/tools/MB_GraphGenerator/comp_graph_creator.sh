#!/bin/bash

#Path for the directory where text files are stored
FILES=~/Learning/FirstTask/MB_GraphGen/Texts/*
DataFiles=~/Learning/FirstTask/MB_GraphGen/DataFiles/

#New file path for the intermediate ops
newFile1=~/Learning/FirstTask/MB_GraphGen/searchedC1.txts
for f in $FILES
do 

  # take action on each file. $f store current file name
  #just the name without path
  filename="${f##*/}"
  #variable to keep length of the avgTime array
  len=0
  len1=0
  
  #print the file name        
  newDnam="$DataFiles$filename"
 
  > $newDnam
 #get index of "."
  val=$( echo $(expr index "$filename" .))
  val=$((val-1))
  #get new name as path without extension
  newnam=$( echo $filename | cut -c1-"$val")
    
   #get the files begin with summary and creare file searched.txt
        > $newFile1
        egrep "^summary =" $f > $newFile1
        
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
        done <"$newFile1"
        cat << __EOF | gnuplot
set xlabel "SAMPLE_NUMBER"
set ylabel "AVERAGE_VALUES"        
set terminal dumb
plot "$newDnam" with lines title "Cumulative_Average"
__EOF
done
