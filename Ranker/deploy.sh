#Remove everything
echo "REMOVING EVERYTHING"
rm -rf /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps/master /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps/master.war
rm -rf /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps/worker /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps/worker.war


#Build master and first worker
echo "DEPLOYING MASTER AND FIRST WORKER"
ant clean
cp testConf/web-worker1.xml target/worker/WEB-INF/web.xml
ant all
mv master.war /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps
mv worker.war /home/aryaa/Downloads/apache-tomcat-7.0.61/webapps

