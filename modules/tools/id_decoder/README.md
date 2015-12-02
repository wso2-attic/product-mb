#id_decoder
This project can be used to decode Message Ids....
id_decode.sh can be used to give multiple message ids as input using file and write decoded details into a file.
single_id_decode.sh can be used to give single message id as input using terminal and get decoded details on to terminal itself.

Initialize these variables in id_decode.sh file

INPUT_FILES=PATH-TO-INPUT-FILES-DIRECTORY/*
DECRYPTED_ID_FILES=PATH-TO-DIRECTORY-WHERE-OUTPUT-SHOULD-BE-SAVED/

Add this project directory to your path....

Open command line in project directory....

  chmod +x id_decode.sh 
  chmod +x single_id_decode.sh
  
  ./id_decode.sh
  ./single_id_decode.sh
