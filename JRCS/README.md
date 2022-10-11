# JRCS
JRCS Assignment - Convert Avro File to CSV


This code can also be found at https://github.com/lowkeymonkey/JRCS.git 
This is a private repository, please request access.


Steps to execute file:

    1. Change directory to jrcs-avro-cv folder

If you have maven installed in your PC:

    2 A. Execute in command line: "mvn clean install".
        This will download dependencies in your local.
        Make sure you are connected to the internet.
      B. Change directory to target folder 

If you do not have maven installed in your PC:

    2. Change directory to output folder

    3. Execute command:
     java -jar jrcs-avro-cv-0.0.1-SNAPSHOT-jar-with-dependencies.jar [avro file location] [csv file name output]
     
     [avro file location] - this is the name and location of your avro file.
     [csv file name output] - this is the filename of the csv output, no need to place ".csv" extension
     
    Sample command:
    
    $ java -jar jrcs-avro-cv-0.0.1-SNAPSHOT-jar-with-dependencies.jar ../src/main/resources/02.avro finaloutput
    
    This will generate finaloutput.csv in your current directory
    
    