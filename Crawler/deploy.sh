rm -rf /home/cis455/Downloads/apache-tomcat-6.0.43/webapps/servlet /home/cis455/Downloads/apache-tomcat-6.0.43/webapps/servlet.war
echo "Removed from tomcat webapps" 
ant servlet-war
echo "Stopping server"
/home/cis455/Downloads/apache-tomcat-6.0.43/bin/shutdown.sh
echo "Checking processes on 8080"
lsof -i:8080
read y
echo "Moving war to tomcat"
mv servlet.war /home/cis455/Downloads/apache-tomcat-6.0.43/webapps
echo "Starting server"
/home/cis455/Downloads/apache-tomcat-6.0.43/bin/startup.sh
