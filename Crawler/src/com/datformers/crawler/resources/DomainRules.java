package com.datformers.crawler.resources;

import java.util.Date;

import com.datformers.crawler.info.RobotsTxtInfo;

/*
 * Class to store domain specifi dules
 */
public class DomainRules {
	private String domainName; //domain name
	private Date nextAllowedAccess; //next allowed date to access domain
	private RobotsTxtInfo robotsTxtInfo; //parsed version of robots.txt file
	/*
	 * Constructor
	 */
	public DomainRules(String name, RobotsTxtInfo robotsTxtInfo) {
		this.domainName = name;
		this.robotsTxtInfo = robotsTxtInfo;
		int delay = robotsTxtInfo.getCrawlDelay("cis455crawler"); //fetch delay depending on the crawler rules present
		if(delay<=0)
			delay = robotsTxtInfo.getCrawlDelay("*");
		Date date = new Date();
		date.setTime(date.getTime()+delay*1000); //find next allowed access time
		nextAllowedAccess = date;
	}
	public void setRobotsTxtInfo(RobotsTxtInfo robotsTxtInfo) {
		this.robotsTxtInfo = robotsTxtInfo;
	}
	public RobotsTxtInfo getRobotsTxtInfo() {
		return robotsTxtInfo;
	}
	public void setNextAccessTime() {
		int delay = robotsTxtInfo.getCrawlDelay("cis455crawler"); //fetch delay depending on the crawler rules present
		if(delay<=0)
			delay = robotsTxtInfo.getCrawlDelay("*");
		Date date = new Date();
		date.setTime(date.getTime()+delay*1000);
		nextAllowedAccess = date;
	}
	public Date getNextAccessTime() {
		return nextAllowedAccess;
	}
}
