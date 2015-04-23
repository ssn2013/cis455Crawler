package com.datformers.mapreduce.util;

import java.util.ArrayList;
import java.util.List;

/*
 * Class for acting as a data structure to hold job related details (submitted via the form)
 * This structure is used by both master and worker servlets to hold their data
 */
public class JobDetails {
	private String job;
	private String inputDir;
	private String outputDir;
	public int getNumMapThreads() {
		return numMapThreads;
	}
	public void setNumMapThreads(int numMapThreads) {
		this.numMapThreads = numMapThreads;
	}
	private int numMapThreads;
	private int numReduceThreads;
	private int numWorkers;
	private int keysWritten;
	private int keysRead;
	private List<String> workerDetails = new ArrayList<String>(); //List of ip:port values of workers
	public JobDetails() {
		keysRead = 0;
		keysWritten = 0;
	}
	public String getOutputDir() {
		return outputDir;
	}
	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}
	public int getNumReduceThreads() {
		return numReduceThreads;
	}
	public void setNumReduceThreads(int numReduceThreads) {
		this.numReduceThreads = numReduceThreads;
	}
	public boolean isMapPhase = true;
	public JobDetails(String job, String input, int numThreads, int numWorkers ) {
		this.job = job;
		this.inputDir = input;
		this.numMapThreads = numThreads;
		this.numWorkers = numWorkers;
		keysRead = 0;
		keysWritten = 0;
	}
	public String getJob() {
		return job;
	}
	public void setJob(String job) {
		this.job = job;
	}
	public String getInputDir() {
		return inputDir;
	}
	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}
	/*
	 * Method returns thread count, corresponding to the different phase (map vs reduce)
	 */
	public int getNumThreads() {
		if(isMapPhase)
			return numMapThreads;
		else 
			return numReduceThreads;
	}
	public void setNumThreads(int numThreads) {
		this.numMapThreads = numThreads;
	}
	public int getNumWorkers() {
		return numWorkers;
	}
	public void setNumWorkers(int numWorkers) {
		this.numWorkers = numWorkers;
	}
	public int getKeysWritten() {
		return keysWritten;
	}
	public int getKeysRead() {
		return keysRead;
	}
	public void addToKeysRead(int val) {
		keysRead+=val;
	}
	public void addToKeysWritten(int val) {
		keysWritten += val;
	}
	public void addWorkerDetails(String ipString) {
		workerDetails.add(ipString);
	}
	public List<String> getWorkerDetails() {
		return workerDetails;
	}
	public void resetKeys() { //to reset the count of keys when moving from map to reduce phase
		keysRead = 0;
		keysWritten = 0;
	}
}
