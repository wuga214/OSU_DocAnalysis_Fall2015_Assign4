all:jarfile

spark_path = "../../spark/spark-1.4.0-bin-hadoop2.6"

class:*.java
	javac -cp ".:$(spark_path)/lib/spark-assembly-1.4.0-hadoop2.6.0.jar" *.java

jarfile:class
	cd ..; \
	jar cf a4example.jar a4example


clean:
	rm -f ../a4example.jar *.class
