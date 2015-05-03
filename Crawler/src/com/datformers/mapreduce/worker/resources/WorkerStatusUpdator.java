package com.datformers.mapreduce.worker.resources;

import java.util.HashMap;
import java.util.Map;

import com.datformers.resources.HttpClient;
import com.datformers.crawler.XPathCrawler;
import com.datformers.crawler.XPathCrawlerThread;
import com.datformers.servlets.WorkerServlet;

/*
 * Class representing thread which updates Master every 10 seconds with status of worker
 */
public class WorkerStatusUpdator implements Runnable {

	private String masterIP;
	private String masterPort;
	private int workerPort;
	private WorkerServlet parent;

	public WorkerStatusUpdator(String ip, String port, int workerPort, WorkerServlet parent) {
		masterIP = ip;
		masterPort = port;
		this.workerPort = workerPort;
		this.parent = parent;
	}

	@Override
	public void run() {
		HttpClient client = new HttpClient();
		
		try {
			while (true) {
				// Code to send workerStatus updates
				Map<String, String> requestParameters = new HashMap<String, String>();
				requestParameters.put("port", "" + this.workerPort);
				requestParameters.put("status", parent.STATUS);
				requestParameters.put("job", parent.getPresentJobName());
				requestParameters.put("keysRead", "" + parent.getKeysRead());
				requestParameters.put("keysWritten", "" + parent.getKeysWritten());
				requestParameters.put("totalURLCount", "" + XPathCrawler.totalURLCount);
				String urlString = "http://" + masterIP + ":" + masterPort
						+ "/master/workerstatus";
				
				client.makeGetRequest(urlString, Integer.parseInt(masterPort.trim()), requestParameters);
				// Now Sleep
				
				Thread.sleep(10000);
				
			}
		} catch (InterruptedException ie) {
		
			ie.printStackTrace();
			System.out.println("Timed Updator of Worker interrupted: "
					+ ie.getMessage());
		}
	}

	/*
	 * Method to convert a map of parameters into the url string format
	 */
	private String getParameterString(Map<String, String> requestParameters) {
		if (requestParameters == null || requestParameters.isEmpty())
			return "";
		else {
			StringBuffer urlParams = null;
			for (String key : requestParameters.keySet()) {
				if (urlParams == null) {
					urlParams = new StringBuffer();
					urlParams.append("?" + key.trim() + "="
							+ requestParameters.get(key).trim());
				} else {
					urlParams.append("&" + key.trim() + "="
							+ requestParameters.get(key).trim());
				}
			}
			return urlParams.toString();
		}
	}

}
