rm -rf /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps/search /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps/search.war
echo "Removed from tomcat webapps" 
ant clean
ant servlet-war
mv search.war /Users/Adi/Downloads/apache-tomcat-7.0.61/webapps
