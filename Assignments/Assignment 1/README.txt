README.TXT
------------------------

This file lists the instructions to run the project BD_Assignment_1

----Author----
Sneha Bangar 

---Library used---
NA

----Class Details----

1. FileUpload.Java
   This is the main class which calls different functions uploadFileToServer,getUrl


----Run in Command Line----
Command line run format to run on hdfs -
hadoop jar BD_Assignment_1-0.0.1-SNAPSHOT.jar BigData.BD_Assignment_1.FileUpload  <destination path>

for example-
hadoop jar BD_Assignment_1-0.0.1-SNAPSHOT.jar BigData.BD_Assignment_1.FileUpload  hdfs://cshadoop1/user/ssb151030/

  
---Results----

After you run the program ,6 bz2 files given in the assignment will be downloaded on server and their uncompressed  versions (.txt) will also be saved on server.

Use  hdfs dfs -cat <filename>.txt command to see the any extracted file on server.
