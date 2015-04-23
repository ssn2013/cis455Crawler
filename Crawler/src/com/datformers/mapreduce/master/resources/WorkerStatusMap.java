package com.datformers.mapreduce.master.resources;

/*
 * Class representing data structure to store details of server sent using /workerstatus request
 */
public class WorkerStatusMap {
	private String IPPort; //ip+port string
	private String status; //status
	private String job; //name of job
	private int keysRead; //count of keys read
	private int keysWritten; //count of keys written
	public String getIPPort() {
		return IPPort;
	}
	public void setIPPort(String iPPort) {
		IPPort = iPPort;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getJob() {
		return job;
	}
	public void setJob(String job) {
		this.job = job;
	}
	public int getKeysRead() {
		return keysRead;
	}
	public void setKeysRead(int keysRead) {
		this.keysRead = keysRead;
	}
	public int getKeysWritten() {
		return keysWritten;
	}
	public void setKeysWritten(int keysWritten) {
		this.keysWritten = keysWritten;
	}
	
}
