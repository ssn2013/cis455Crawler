package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.util.HttpClient;
import edu.upenn.cis455.mapreduce.util.JobDetails;
import edu.upenn.cis455.mapreduce.worker.resources.FileManagement;
import edu.upenn.cis455.mapreduce.worker.resources.PushDataThread;
import edu.upenn.cis455.mapreduce.worker.resources.WorkerStatusUpdator;
import edu.upenn.cis455.mapreduce.worker.resources.WorkerThread;

/*
 * Class of Worker Servlet
 * PLEASE NOTE: All URLs are relative to the Apache Tomcat server (i.e worker would be http://<ip>:<port>/worker/<path of request>
 */
public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	public List<Thread> threadPool = new ArrayList<Thread>(); // list of threads
																// running
	public FileManagement fileManagementObject = null; // Object of
														// FileManagement class
														// which handles all
														// file related
														// operations
	private String masterIPPort;
	private int port;
	private String storageDir;
	private int countOfCompletedThreads = 0;
	private String status = "idle";
	private WorkerStatusUpdator wk;
	private Thread wkt;
	public static boolean cursorClosed = false;
	public static boolean outputDBClosed = false;
	

	// Job handling
	public JobDetails currentJob = null;
	private JobDetails pastJob = null;

	public void init(ServletConfig servletConfig)
			throws javax.servlet.ServletException {
		super.init(servletConfig);
		masterIPPort = servletConfig.getInitParameter("master"); // fetch
																	// details
																	// of master
		storageDir = servletConfig.getInitParameter("storagedir"); // fetch
																	// details
																	// of
																	// storage
																	// directory
		port = Integer.parseInt(servletConfig.getInitParameter("port")); // fetch
																			// port
																			// details
		String points[] = masterIPPort.split(":");
		wk = new WorkerStatusUpdator(points[0].trim(), points[1].trim(), port,
				this); // create thread to send update status calls to master
						// every 10 seconds
		wkt = new Thread(wk);
		wkt.start();
	}

	@Override
	public void destroy() {
		wkt.stop();
		for (Thread t : threadPool) {
			if (t != null)
				t.interrupt();
		}
	}
	/*
	 * doGet of Servlet
	 * 
	 * @see
	 * javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest
	 * , javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		if(request.getPathInfo()!=null && request.getPathInfo().contains("writeToMe")) {
			String fileName = request.getParameter("file");
			System.out.println("Worker received writeToMe with file: "+fileName);
			response.setStatus(200);
			writeOutputToMaster(fileName);
		} else {
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.println("<html><head><title>Worker</title></head>");
			out.println("<body>Hi, I am the worker!</body></html>");
		}
	}

	private void writeOutputToMaster(String fileName) {
		if(fileManagementObject==null) {
			fileManagementObject = new FileManagement(storageDir);
		}
		fileManagementObject.setToMasterReader(fileName);
		
		String dataToSend = null;
		
		//get data
		String urlString = "http://"+masterIPPort.trim()+"/master/writetodb";
		int masterPort = Integer.parseInt(masterIPPort.split(":")[1]);
		
		while((dataToSend = fileManagementObject.getDataFromFile())!=null) {
			HttpClient httpClient  = new HttpClient();
			System.out.println("Writing to URL: "+urlString);
			httpClient.makePostRequest(urlString, masterPort, "text/plain", dataToSend);
		}
		HttpClient httpClient  = new HttpClient();
		System.out.println("Writing to URL: "+urlString+" END from port: "+port);
		httpClient.makePostRequest(urlString, masterPort, "text/plain", "$END Worker on: "+port+"\n");
 	}

	/*
	 * doPost of servlet
	 * 
	 * @see
	 * javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest
	 * , javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		if (request.getPathInfo().contains("runmap")) {
			System.out.println("WORKER GOT MAP REQUEST");
			status = "mapping";
			processRunMap(request, response); // redirect to method handing map
												// calls
		} else if (request.getPathInfo().contains("runreduce")) {
			status = "reducing";
			processRunReduce(request, response); // redirect to method handling
													// reduce calls
		} else if (request.getPathInfo().contains("pushdata")) {
			System.out.println("WORKER GOT PUSHDATA REQUEST");
			processPushData(request, response); // redirect to method handling
												// pushdata calls
		}
	}

	/*
	 * Method to handle /pushdata requests
	 */
	private void processPushData(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			BufferedReader br = (BufferedReader) request.getReader();
			String line = null;
			fileManagementObject.writeToSpoolIn(br); // write to spool in folder
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Worker Servlet Exception in PushData request: "
					+ e.getMessage());
		}

	}

	/*
	 * Method to handle /runreduce calls
	 */
	private void processRunReduce(HttpServletRequest request,
			HttpServletResponse response) {
		String job = request.getParameter("job");
		String output = request.getParameter("output");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		
		if (currentJob.getJob().equals(job)) { // check if reduce is called for
												// current job
			currentJob.setNumReduceThreads(numThreads);
			currentJob.setOutputDir(output);
			try {
				fileManagementObject.setModeReduce(output);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			countOfCompletedThreads = 0;
			currentJob.resetKeys();
			// start threads
			
			System.out.println("Worker Starting reduce: "+currentJob.getJob());
			
			threadPool = new ArrayList<Thread>();
			for (int i = 0; i < numThreads; i++) {
				WorkerThread wt = new WorkerThread(job, fileManagementObject,
						this, false);
				Thread t = new Thread(wt);
				threadPool.add(t);
				t.start();
			}
		}
	}

	/*
	 * Method to handle /runmap calls
	 */
	private void processRunMap(HttpServletRequest request,
			HttpServletResponse response) {
		String job = request.getParameter("job");
		String input = request.getParameter("input");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
		String databaseIO = request.getParameter("databaseIO").trim();
		String logline = "\nJOB: " + job + " INPUT: " + input
				+ " NUM_THREADS: " + numThreads + " NUM_WORKERS: " + numWorkers;
		JobDetails jobDetails = new JobDetails(job, input, numThreads,
				numWorkers);
		
		for (int i = 1;; i++) {
			String workerValue = request.getParameter("worker" + i);
			if (workerValue == null)
				break;
			logline += " WORKER: " + i + ": " + workerValue;
			jobDetails.addWorkerDetails(workerValue);
		}
		startMapJob(jobDetails, databaseIO); // Method to handle map job
	}

	/*
	 * Method takes the given request parameters and starts map job on requested
	 * number of threads
	 */
	private void startMapJob(JobDetails jobDetails, String databaseIO) {
		
		System.out.println("Worker: Starting map: "+jobDetails.getJob());
		
		try {
			currentJob = jobDetails;
			fileManagementObject = new FileManagement(storageDir, currentJob.getInputDir(), currentJob.getNumWorkers(), databaseIO);

			// Instantiate a threadpool and run thread
			threadPool = new ArrayList<Thread>();
			currentJob.resetKeys();
			countOfCompletedThreads = 0;
			for (int i = 0; i < currentJob.getNumThreads(); i++) {
				WorkerThread worker = new WorkerThread(currentJob.getJob(),
						fileManagementObject, this, true);
				Thread t = new Thread(worker);
				threadPool.add(t);
				t.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Create a resource management object
	}

	/*
	 * Method called by threads on completion of task
	 */
	public synchronized void updateCompletion() {
		countOfCompletedThreads++;
		if (countOfCompletedThreads == currentJob.getNumThreads()) { // check
																		// count
																		// of
																		// threads
																		// against
																		// required
																		// number
																		// of
																		// threads
																		// to
																		// see
																		// if
																		// all
																		// threads
																		// finished
			
			if (status.equals("mapping")) { // on completions of map
				currentJob.isMapPhase = false;
				fileManagementObject.closeAllSpoolOut(); // close all references
															// to spool out
															// directory files
				new Thread(new PushDataThread(currentJob.getWorkerDetails(),
						this, fileManagementObject)).start(); // run push data
																// on another
																// thread
			} else if (status.equals("reducing")) { // on completion of reduce
				status = "idle"; // change status
				pastJob = currentJob; // past job keeps track of previous job
										// (for keysWritten)
			    fileManagementObject.closeReduceWriter();
				currentJob = null;
				//wrapper.exit();
			}
		}
	}

	/*
	 * Method called by thread handling push data to update status of worker as
	 * waiting
	 */
	public void updateStatusWaiting() {
		status = "waiting";

		Map<String, String> requestParameters = new HashMap<String, String>();
		requestParameters.put("port", "" + port);
		requestParameters.put("status", getState());
		requestParameters.put("job", getPresentJobName());
		requestParameters.put("keysRead", "" + getKeysRead());
		requestParameters.put("keysWritten", "" + getKeysWritten());
		String urlString = "http://" + masterIPPort + "/master/workerstatus";
		HttpClient client = new HttpClient();

		client.makeRequest(urlString,
				Integer.parseInt(masterIPPort.split(":")[1].trim()),
				requestParameters);
		if (client.getResponseCode() == 200)
			System.out
					.println("WorkerServlet:updateStatusWaiting: Successful updation of master at: "
							+ urlString);
		else
			System.out
					.println("WorkerServlet:updateStatusWaiting: Unsuccessful updation of master at: "
							+ urlString
							+ " returned: "
							+ client.getResponseCode());

	}

	/*
	 * Method called by thread to update count of keys read
	 */
	public synchronized void updateKeysRead(int count) {
		currentJob.addToKeysRead(count);
	}

	/*
	 * Method called by threads to update count of keys written
	 */
	public synchronized void updateKeyWritten(int count) {
		currentJob.addToKeysWritten(count);
	}

	/*
	 * Method returns count of keys read according to specifics of status
	 */
	public int getKeysRead() {
		if (status.equals("mapping") || status.equals("reducing"))
			return currentJob.getKeysRead();
		if (status.equals("waiting"))
			return currentJob.getKeysRead();
		if (status.endsWith("idle"))
			return 0;
		return -1;
	}

	/*
	 * Method calls count of keys written according to specifics of status
	 */
	public int getKeysWritten() {
		if (status.equals("mapping") || status.equals("reducing"))
			return currentJob.getKeysWritten();
		if (status.equals("waiting"))
			return currentJob.getKeysWritten();
		if (status.endsWith("idle")) {
			if (pastJob != null)
				return pastJob.getKeysWritten();
			else
				return 0;
		}
		return -1;
	}

	/*
	 * Method returns job name
	 */
	public String getPresentJobName() {
		if (currentJob == null)
			return "";
		else
			return currentJob.getJob();
	}

	/*
	 * Method returns current status of servlet
	 */
	public String getState() {
		return status;
	}
}
