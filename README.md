# spring-batch-demo
Sample spring batch app converts csv file to database data

- Database : MySQL

- CSV file is placed in classpath: src/main/resources folder

- Database schema and definitions are auto created at runtime since spring data jpa with hibernate is in place.

- A job builder factory that builds the job and a step builder factory that builds a step that has a csv flat file reader, data processor, and a writer to write the parsed data into mysql database. 

- A job bean that has a flow defined to execute the step bean(reader + processor + writer).

- Job completion listener is in place to notify if the job is over.
