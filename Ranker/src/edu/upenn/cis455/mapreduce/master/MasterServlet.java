package edu.upenn.cis455.mapreduce.master;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.master.resources.WorkerStatusMap;
import edu.upenn.cis455.mapreduce.util.HttpClient;
import edu.upenn.cis455.mapreduce.util.JobDetails;

/*
 * Master servlet acting as the master node
 * PLEASE NOTE: All URLs are relative to the Apache Tomcat server (i.e master would be http://<ip>:<port>/master/<path of request>
 */
public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	private Map<String, WorkerStatusMap> workerStatusMaps = new HashMap<String, WorkerStatusMap>(); // map
	// of
	// all
	// worker
	// node


	int countOfIterations = 0;
	private int totalNoOfIterations = 0;

	private JobDetails presentMapJob = null; // details of present running job
	private String job = null;
	private String inputDB = null;
	private String outputDB = null;
	private int numMapThreads = 10;
	private int numReduceThreads = 10;

//	HashMap<String, String> prevState = new HashMap<String, String>();
	
	@Override
	public void init(ServletConfig servletConfig) throws javax.servlet.ServletException {
		super.init(servletConfig);
		this.inputDB = servletConfig.getInitParameter("InputDB"); //fetch details of storage directory
		this.outputDB = servletConfig.getInitParameter("OutputDB");
		int iterations  = Integer.parseInt(servletConfig.getInitParameter("iterations"));
		this.totalNoOfIterations = iterations+1;
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		if (request.getServletPath().contains("workerstatus")) {
			processWorkerStatusRequest(request, response);

		} else if (request.getServletPath().contains("status")) {
			processStatusRequest(request, response);
		} else if (request.getServletPath().contains("pagerank")) {
			startPageRank();
			response.setStatus(200);
		} else {
			response.getWriter().println("<html><body><p>Hi this is the master node</p></body></html>");
		}

	}

	// Page Rank

	public void startPageRank() {
		String className = "";
		String inputDir = "";
		String outputDir = "";
		String databaseIO = "";
		int noMapThreads = numMapThreads;
		int noReduceThreads = numReduceThreads;
		JobDetails requestJob = new JobDetails();
		countOfIterations = 0;
		System.out.println("Master: Starting page rank");
		switch (countOfIterations) {
		case 0:
			System.out.println("Master: case 0");
			className = "edu.upenn.cis455.mapreduce.job.FindSinks";
			inputDir = inputDB;
			outputDir = "output0";
			databaseIO = "input";
			break;

		case 1:

			String job = "edu.upenn.cis455.mapreduce.job.RemoveSinks";
			className = job;
			inputDir = "output0";
			outputDir = "output1";
			break;
			
		default:
//			if(countOfIterations==totalNoOfIterations) {
//				className = "edu.upenn.cis455.mapreduce.job.Ranker";
//				inputDir = "output" + countOfIterations;
//				outputDir = outputDB;
//				databaseIO = "output";	
//			} else {
				className = "edu.upenn.cis455.mapreduce.job.Ranker";
				inputDir = "output" + (countOfIterations - 1);
				outputDir = "output" + countOfIterations;
//			}	
		}

		requestJob.setJob(className);
		requestJob.setInputDir(inputDir);
		requestJob.setOutputDir(outputDir);
		requestJob.setNumMapThreads(noMapThreads);
		requestJob.setNumReduceThreads(noReduceThreads);

		// Sending data to the worker
		StringBuffer dataToSend = new StringBuffer("job=" + className); //forming data for map
		dataToSend.append("&databaseIO="+databaseIO);
		dataToSend.append("&input=" + inputDir);
		dataToSend.append("&numThreads=" + noMapThreads);
		int i = 1;
		List<String> availableWorkers = new ArrayList<String>();
		for (String key : workerStatusMaps.keySet()) { // go through list of all
			// worker and look for
			// idle ones
			if (workerStatusMaps.get(key).getStatus().trim().equals("idle")) {
				availableWorkers.add(key);
				dataToSend.append("&worker" + i + "=" + key);
				i++;
				System.out.println("Master: adding to available worker: "+key);
			}
		}
		requestJob.setNumWorkers(availableWorkers.size()); // keep track of
		// worker working on
		// the job
		dataToSend.append("&numWorkers=" + availableWorkers.size());
		presentMapJob = requestJob; // if there are threads, set this to current
		// job
		String data = dataToSend.toString();
		for (String key : availableWorkers) {
			String urlString = "http://" + key.trim() + "/worker/runmap";
			HttpClient httpClient = new HttpClient();
			System.out.println("Master: making post request to URL: "+urlString+" Data: "+data);
			InputStream responseBody = httpClient.makePostRequest(urlString,
					Integer.parseInt(key.split(":")[1].trim()),
					"application/x-www-form-urlencoded", data);

		}

	}

	/*
	 * doPost method of servlet
	 * 
	 * @see
	 * javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest
	 * , javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) {

	}

	/*
	 * Method to handle /status GET request
	 */
	private void processStatusRequest(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			response.setContentType("text/html");
			StringBuffer stringBufferHTML = new StringBuffer();// forming
			// response body
			// i.e form
			stringBufferHTML
			.append("<html><body><h1>Server Status Page</h1></br><h2>Status of Worker</h2>");
			stringBufferHTML
			.append("<table><tr><th>IP:Port</th><th>Status</th><th>Job</th><th>Keys Read</th><th>Keys Written</th></tr>");
			for (String key : workerStatusMaps.keySet()) {
				stringBufferHTML.append("<tr>");
				WorkerStatusMap workerStatusMap = workerStatusMaps.get(key); // fetch
				// details
				// of
				// given
				// worker
				// from
				// maps
				if (workerStatusMap != null) { // display details in table
					stringBufferHTML.append("<td>"
							+ workerStatusMap.getIPPort() + "</td>");
					stringBufferHTML.append("<td>"
							+ workerStatusMap.getStatus() + "</td>");
					stringBufferHTML.append("<td>" + workerStatusMap.getJob()
							+ "</td>");
					stringBufferHTML.append("<td>"
							+ workerStatusMap.getKeysRead() + "</td>");
					stringBufferHTML.append("<td>"
							+ workerStatusMap.getKeysWritten() + "</td>");
				} else {
					stringBufferHTML
					.append("<tr><td>"
							+ workerStatusMap
							+ "</td><td colspan=\"4\">Error fetching object</td></tr>");
				}
				stringBufferHTML.append("</tr>");
			}
			// Create form for adding job
			stringBufferHTML
			.append("</table></body></html>");
			response.getWriter().println(stringBufferHTML.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Method to process /workerstatus POST requests
	 */
	private void processWorkerStatusRequest(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			// fetch all details from the requst and put into the map
			WorkerStatusMap workerStatusMap = new WorkerStatusMap();
			int port = Integer.parseInt(request.getParameter("port"));
			String status = request.getParameter("status").trim();
			workerStatusMap.setStatus(status);
			String job = request.getParameter("job").trim();
			workerStatusMap.setJob(job);
			int keysRead = Integer.parseInt(request.getParameter("keysRead"));
			workerStatusMap.setKeysRead(keysRead);
			int keysWritten = Integer.parseInt(request
					.getParameter("keysWritten"));
			workerStatusMap.setKeysWritten(keysWritten);
			String ipAddress = request.getHeader("X-FORWARDED-FOR");
			if (ipAddress == null) {
				ipAddress = request.getRemoteAddr();
			}
			workerStatusMap.setIPPort(ipAddress + ":" + port);

			workerStatusMaps.put(workerStatusMap.getIPPort(), workerStatusMap);
			// Check if All reduce are reached
//			int count = 0;
//			if (prevState.containsKey(ipAddress + ":" + port)) {
//				prevState.put(ipAddress + ":" + port, status);
//				for (Map.Entry<String, String> entry : prevState.entrySet()) {
//					String value = entry.getValue();
//					if (value.equals("idle")) {
//						count = count + 1;
//					}
//				}
//			}

			System.out.println("WORKER UPDATE:- Port: "
					+ port
					+ " Status: "
					+ status
					+ " Job: "
					+ job
					+ " keysRead: "
					+ keysRead
					+ " keysWritten: "
					+ keysWritten
					+ " ipAddress: "
					+ ipAddress
					+ " Put into map: "
					+ workerStatusMaps.get(workerStatusMap.getIPPort())
					.getJob());
			if (presentMapJob != null) {
				checkAndRunReduce(job);
			}
//			if (presentMapJob != null) {
//				checkAndRunNextIteration(job);
//			}
			
			// check if relevant threads are waiting and
			// run reduce
			/*if (count == 3) {
				prevState.clear();
				countOfIterations++;
				startPageRank();

			}*/
			response.setStatus(200);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void checkAndRunNextIteration(String jobName) {
		int count = 0;

		for (String key : workerStatusMaps.keySet()) { // for the given job,
			// the waiting workers
			WorkerStatusMap map = workerStatusMaps.get(key);
			if (map.getJob().equals(jobName)
					&& map.getStatus().equals("idle"))
				count++;
		}
		
		if(count == presentMapJob.getNumWorkers()) {
			countOfIterations++;
			if(countOfIterations>totalNoOfIterations)
				return;
			startPageRank();
		}
	}

	/*
	 * Method called at every /workerstatus request to check if all workers are
	 * "waiting" and run reduce
	 */
	private void checkAndRunReduce(String jobName) {
		int count = 0;

		for (String key : workerStatusMaps.keySet()) { // for the given job,
			// the waiting workers
			WorkerStatusMap map = workerStatusMaps.get(key);
			if (map.getJob().equals(jobName)
					&& map.getStatus().equals("waiting"))
				count++;
		}

		if (count == presentMapJob.getNumWorkers()) { 
			StringBuffer buf = new StringBuffer();
			buf.append("job=" + presentMapJob.getJob());
			buf.append("&output=" + presentMapJob.getOutputDir());
			buf.append("&numThreads=" + presentMapJob.getNumReduceThreads());
			String body = buf.toString();

			// for each given worker make /runreduce call
			for (String key : workerStatusMaps.keySet()) {
				HttpClient client = new HttpClient();
			//	prevState.put(key, "reduce");
				String url = "http://" + key + "/worker/runreduce";
				client.makePostRequest(url,
						Integer.parseInt(key.split(":")[1].trim()),
						"application/x-www-form-urlencoded", body);
			}
		}
	}

}