rm -rf /home/cis455/Downloads/apache-tomcat-6.0.43/webapps/worker /home/cis455/Downloads/apache-tomcat-6.0.43/webapps/*.war
echo "Removed from tomcat webapps" 
ant all
mv worker.war /home/cis455/Downloads/apache-tomcat-6.0.43/webapps
