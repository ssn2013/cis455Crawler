ant clean 
ant all
scp -i ~/.ec2/datformers.pem master.war ubuntu@ec2-52-7-79-117.compute-1.amazonaws.com:~
scp -i ~/.ec2/datformers.pem worker.war ubuntu@ec2-52-7-79-117.compute-1.amazonaws.com:~
scp -i ~/.ec2/datformers.pem worker.war ubuntu@ec2-52-6-216-134.compute-1.amazonaws.com:~
scp -i ~/.ec2/datformers.pem worker.war ubuntu@ec2-52-6-183-77.compute-1.amazonaws.com:~
scp -i ~/.ec2/datformers.pem worker.war ubuntu@ec2-52-7-23-58.compute-1.amazonaws.com:~
