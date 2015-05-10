package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.datformers.storage.DBRankerWrapper;
import com.datformers.storage.DocRanksStore;

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
	
	int countOfIterations = 0;
	private int totalNoOfIterations = 0;
	int workerOutputWrittenCount = 0;
	
	HashMap<String, String> prevState = new HashMap<String, String>();

	private JobDetails presentMapJob = null; // details of present running job
	private String job = null;
	private String inputDB = null;
	private String outputDB = null;
	private int numMapThreads = 10;
	private int numReduceThreads = 10;
	private DBRankerWrapper wrapper;

	private int doPostCount = 0;
	
//	HashMap<String, String> prevState = new HashMap<String, String>();
	
	@Override
	public void init(ServletConfig servletConfig) throws javax.servlet.ServletException {
		
		System.out.println("INIT MASTER SErvelet");
		super.init(servletConfig);
		this.inputDB = servletConfig.getInitParameter("InputDB"); //fetch details of storage directory
		this.outputDB = servletConfig.getInitParameter("OutputDB");
		int iterations  = Integer.parseInt(servletConfig.getInitParameter("iterations"));
		this.totalNoOfIterations = iterations + 1;
		System.out.println("Starting with input db: "+this.inputDB);
		System.out.println("Starting with output db: "+this.outputDB);
		wrapper = new DBRankerWrapper(this.outputDB);
		wrapper.configure();
		//this.totalNoOfIterations = 3;
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		if (request.getServletPath().contains("workerstatus")) {
			processWorkerStatusRequest(request, response);

		} else if (request.getServletPath().contains("status")) {
			processStatusRequest(request, response);
		} else if (request.getServletPath().contains("pagerank")) {
			response.setStatus(200);
			startPageRank();
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
		switch (countOfIterations) {
		case 0:
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
			System.out.println("Master: Staring iteration: "+countOfIterations);
			className = "edu.upenn.cis455.mapreduce.job.Ranker";
			inputDir = "output" + (countOfIterations - 1);
			outputDir = "output" + countOfIterations;
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
			}
		}
		requestJob.setNumWorkers(availableWorkers.size()); // keep track of
		// worker working on
		// the job
		dataToSend.append("&numWorkers=" + availableWorkers.size());
		// job
		boolean firstTime = true;
		String data = dataToSend.toString();
		for (String key : availableWorkers) {
			String urlString = "http://" + key.trim() + "/worker/runmap";
			HttpClient httpClient = new HttpClient();
			InputStream responseBody = httpClient.makePostRequest(urlString,
					Integer.parseInt(key.split(":")[1].trim()),
					"application/x-www-form-urlencoded", data);
			if(firstTime) {
				firstTime = false;
				presentMapJob = requestJob; // if there are threads, set this to current
			}
		}

	}
	
	public void destroy() {
		wrapper.exit();
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

		if (request.getServletPath().contains("writetodb")) {
			doPostCount++;
			System.out.println("DOPOST COUNT: "+doPostCount);
			writeFinalOutputToDB(request, response);
		}

	}

	private synchronized void writeFinalOutputToDB(HttpServletRequest request,
			HttpServletResponse response) {
		System.out.println("Write to DB called");
		DocRanksStore entity = new DocRanksStore();

		BufferedReader reader = null;
		try {
			reader = request.getReader();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String line;
		try {
			while ((line = reader.readLine())!=null) {
				if (line.contains("$END")) {
					workerOutputWrittenCount++;		
					System.out.println("Reached END, count is now: "+workerOutputWrittenCount+" with line: "+line);
					if(workerOutputWrittenCount == workerStatusMaps.keySet().size()){
						wrapper.exit();	
						System.out.println("I am done writing!!!");
					}
				} else if(line !=null){
					String[] args = line.split("\t");
					String[] vals = args[1].split(" ");
					BigInteger docId = new BigInteger(args[0]);
					entity.setDocId(docId);
					entity.setRank(Double.parseDouble(vals[0]));
					wrapper.pageRankKey.put(entity);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

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
			

			System.out.println("WORKER UPDATE:- Port: "	+ port + " Status: "+ status+ " Job: "+ job+ " keysRead: "+ keysRead		+ " keysWritten: "
					+ keysWritten+ " ipAddress: "+ ipAddress+ " Put into map: "+ workerStatusMaps.get(workerStatusMap.getIPPort()).getJob());
			if (presentMapJob != null) {
				checkAndRunReduce(job);
			}
			//if (presentMapJob != null) {
				checkAndRunNextIteration(workerStatusMap);
			//}
			
//			if (count ==workerStatusMaps.keySet().size()) {
//				prevState.clear();
//				countOfIterations++;
//				checkAndRunNextIteration();
//				
//			}
			response.setStatus(200);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void checkAndRunNextIteration(WorkerStatusMap workerStatusMap) {
//		int count = 0;
//		for (String key : workerStatusMaps.keySet()) { // for the given job,
//			// the waiting workers
//			WorkerStatusMap map = workerStatusMaps.get(key);
//			if (map.getJob().equals(jobName) && map.getStatus().equals("idle"))
//				count++;
//		}
		
		int count = 0;
		if (prevState.containsKey(workerStatusMap.getIPPort())) {
			prevState.put(workerStatusMap.getIPPort(), workerStatusMap.getStatus());
			for (Map.Entry<String, String> entry : prevState.entrySet()) {
				String value = entry.getValue();
				if (value.equals("idle")) {
					count = count + 1;
				}
			}
		}
		
		if(count == workerStatusMaps.keySet().size()) {
			countOfIterations++;
			if(countOfIterations>totalNoOfIterations){
				if(countOfIterations==(totalNoOfIterations+1)) {
					String fileName = "output"+(countOfIterations-1);
					makeWriteToMeRequest(fileName);
					return;
				} else {
					presentMapJob = null;
					return;
				}
			}
			startPageRank();
		}
	}

	private void makeWriteToMeRequest(String fileName) {
		
		//System.out.println("Please Write to Me!!");
		for(String key: workerStatusMaps.keySet()) {
			WorkerStatusMap map = workerStatusMaps.get(key);
			HttpClient client = new HttpClient();
			String url = "http://" + key + "/worker/writeToMe";
			int port = Integer.parseInt(key.split(":")[1].trim());
			//System.out.println("Master making request to URL: "+url+" with file: "+fileName);
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", fileName);
			client.makeRequest(url, port , params);
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
				prevState.put(key, "reduce");
				String url = "http://" + key + "/worker/runreduce";
				client.makePostRequest(url,
						Integer.parseInt(key.split(":")[1].trim()),
						"application/x-www-form-urlencoded", body);
			}
		}
	}

}