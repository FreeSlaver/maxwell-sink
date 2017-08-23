# What is maxwell-sink

Maxwell-sink is an application based on **Kafka Connect** that consumes JSON messages produced by Maxwell.  
Filter and then Converter the message to SQL to be executed.  
It's used for synchronizing the data change between two or more MySQL clusters.  
    
# What is maxwell

Maxwell is an application that reads MySQL binlogs and writes row updates to Kafka as JSON.    
For more details,please visit [Maxwell's daemon](http://maxwells-daemon.io/)   
     
# How to use maxwell-sink

Just read the **maxwell-sink部署文档.docx** and modify the maxwell-sink.json to fit your needs.

   