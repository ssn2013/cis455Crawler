package edu.upenn.cis455.mapreduce.worker.resources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import edu.upenn.cis455.mapreduce.util.HttpClient;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

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
				requestParameters.put("status", parent.getState());
				requestParameters.put("job", parent.getPresentJobName());
				requestParameters.put("keysRead", "" + parent.getKeysRead());
				requestParameters.put("keysWritten", "" + parent.getKeysWritten());
				String urlString = "http://" + masterIP + ":" + masterPort
						+ "/pagerankmaster/workerstatus";

				client.makeRequest(urlString, Integer.parseInt(masterPort.trim()), requestParameters);
//				if(client.getResponseCode()==200)
//					System.out.println("WorkerStatusUpdator:run: Successful updation of master at: "+urlString);
//				else 
//					System.out.println("WorkerStatusUpdator:run: SUnsuccessful updation of master at: "+urlString+" returned: "+client.getResponseCode());

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
