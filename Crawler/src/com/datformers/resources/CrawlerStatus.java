package com.datformers.resources;

public class CrawlerStatus {
	private String ipAddress;
	private int port;
	private String status;
	private int totalProcessed;
	public int getTotalProcessed() {
		return totalProcessed;
	}
	public void setTotalProcessed(int totalProcessed) {
		this.totalProcessed = totalProcessed;
	}
	public String getIpPortString() {
		return ipAddress+":"+port;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
}
