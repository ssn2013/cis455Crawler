rm -rf /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps/extract /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps/extract.war
echo "Removed from tomcat webapps" 
ant all
mv *.war /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps
