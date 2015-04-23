package com.datformers.mapreduce.worker;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.datformers.crawler.XPathCrawler;
import com.datformers.mapreduce.util.HttpClient;
import com.datformers.mapreduce.util.JobDetails;
import com.datformers.mapreduce.worker.resources.FileManagement;
import com.datformers.mapreduce.worker.resources.PushDataThread;
import com.datformers.mapreduce.worker.resources.WorkerStatusUpdator;
import com.datformers.mapreduce.worker.resources.WorkerThread;
import com.datformers.storage.DBWrapper;

/*
 * Class of Worker Servlet
 * PLEASE NOTE: All URLs are relative to the Apache Tomcat server (i.e worker would be http://<ip>:<port>/worker/<path of request>
 */
public class WorkerServlet extends HttpServlet { 

	static final long serialVersionUID = 455555002;
	public List<Thread> threadPool = new ArrayList<Thread>(); //list of threads running
	public FileManagement fileManagementObject = null; //Object of FileManagement class which handles all file related operations
	private String masterIPPort;
	private int port;
	private String storageDir;
	private String dbDir;
	private int countOfCompletedThreads = 0;
	private String status = "idle";
	private WorkerStatusUpdator wk;
	private Thread wkt;
	public static String seedUrl[];

	//Job handling
	public JobDetails currentJob = null;
	private JobDetails pastJob = null;

	public void init(ServletConfig servletConfig) throws javax.servlet.ServletException {
		super.init(servletConfig);
		masterIPPort = servletConfig.getInitParameter("master"); //fetch details of master
		storageDir = servletConfig.getInitParameter("storagedir"); //fetch details of storage directory
		dbDir = servletConfig.getInitParameter("databasedir"); //fetch details of database directory
		DBWrapper.initialize(dbDir); //initialize DB environment
		port = Integer.parseInt(servletConfig.getInitParameter("port")); //fetch port details
		String points[] = masterIPPort.split(":");
		wk = new WorkerStatusUpdator(points[0].trim(), points[1].trim(), port, this); //create thread to send update status calls to master every 10 seconds
		wkt = new Thread(wk);
		wkt.start();
		System.out.println("SERVLET STARTED:");
	}
	
	@Override
	public void destroy() {
		for(Thread t: threadPool) {
			if(t!=null)
				t.interrupt();
		}
		wkt.interrupt();
		super.destroy();
	}

	/*
	 * doGet of Servlet
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
	{
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}

	/*
	 * doPost of servlet
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		System.out.println("WorkerServlet:doPost GOT: "+request.getPathInfo());
		if(request.getPathInfo().contains("runmap")) {
			status = "mapping";
			processRunMap(request, response); //redirect to method handing map calls
		} else if(request.getPathInfo().contains("runreduce")) {
			status = "reducing";
			processRunReduce(request, response); //redirect to method handling reduce calls
		} else if(request.getPathInfo().contains("startcrawl")) {
			status = "crawling";
			processRunCrawl(request, response); //redirect to method handling for crawl start	
		} else if(request.getPathInfo().contains("stopcrawl")) {
			processStopCrawl(request, response); //redirect to method handling crawl stop
			status = "idle";
		} else if(request.getPathInfo().contains("pushdata")) {
			processPushData(request, response); //redirect to method handling pushdata calls
		}
	}

	/*
	 * Method to handle /pushdata requests
	 */
	private void processPushData(HttpServletRequest request,
			HttpServletResponse response) {
		System.out.println("WorkerServlet:Processpushdata: Query from: "+request.getRemoteAddr()+" Port: "+request.getRemotePort());
		try {
			BufferedReader br = (BufferedReader)request.getReader();
			String line = null;
			fileManagementObject.writeToSpoolIn(br); //write to spool in folder
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Worker Servlet Exception in PushData request: "+e.getMessage());
		}
		
	}

	private void processStopCrawl(HttpServletRequest request,
			HttpServletResponse response) {
		XPathCrawler.SYSTEM_SHUTDOWN=true;
		
	}
	
	private void processRunCrawl(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			StringBuilder buffer = new StringBuilder();
			BufferedReader reader = request.getReader();
			String line;
			while ((line = reader.readLine()) != null) {
				buffer.append(line + "\n");
			}
			String body = buffer.toString();
			JSONObject obj;
			obj = new JSONObject(body);
			JSONArray url = obj.getJSONArray("urls");
			JSONArray crawlWorkers = obj.getJSONArray("crawler");
			String maxRequests=obj.getString("maxRequests");
					
			//start the crawling
			String delim=";;|;;";
			String args[]=new String[5];
			args[0]="";
			if (url != null) {
				seedUrl = new String[url.length()];
				for (int i = 0; i < url.length(); i++) {
					// seedUrl[i] = url.getJSONObject(i).getString("host");
					args[0]+= url.getJSONObject(i).toString()+delim;
				}
				//removing last occurrence of delim
				args[0]=args[0].substring(0,args[0].lastIndexOf(delim));
			}
			
			args[1]=storageDir;
			args[2]=""+1;
			args[3]=maxRequests;
			args[4]="";
			for (int i = 0; i < crawlWorkers.length(); i++) {
				// workers[i] = crawlWorkers.getJSONObject(i).getString("host");
				args[4]+=crawlWorkers.getJSONObject(i).toString()+delim;
			}
			
			//removing last occurrence of delim
			args[4]=args[4].substring(0,args[4].lastIndexOf(delim));
			
			CrawlerStartHelper myrunnable = new CrawlerStartHelper(args);
	     	new Thread(myrunnable).start();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		System.out.println("WorkerServlet:processRunReduce: Reduce request: \nJob: "+job+"\nInput: "+output+"\nNumThreads: "+numThreads);
		if(currentJob.getJob().equals(job)) { //check if reduce is called for current job
			currentJob.setNumReduceThreads(numThreads);
			currentJob.setOutputDir(output);
			fileManagementObject.setModeReduce(currentJob.getOutputDir());
			countOfCompletedThreads = 0;
			currentJob.resetKeys();
			
			//start threads
			threadPool = new ArrayList<Thread>();
			for(int i=0; i<numThreads; i++) {
				WorkerThread wt  = new WorkerThread(job, fileManagementObject, this, false);
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
		try {
			String job = request.getParameter("job");
			String input = request.getParameter("input");
			int numThreads = Integer.parseInt(request.getParameter("numThreads"));
			int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
			String logline = "\nJOB: "+job+" INPUT: "+input+" NUM_THREADS: "+numThreads+" NUM_WORKERS: "+numWorkers;
			JobDetails jobDetails = new JobDetails(job, input, numThreads, numWorkers);
			for(int i=1; ; i++) {
				String workerValue = request.getParameter("worker"+i);
				if(workerValue==null)
					break;
				logline+=" WORKER: "+i+": "+workerValue;
				jobDetails.addWorkerDetails(workerValue);
			}
			System.out.println("WorkerServlet:processRunMap: Got map request: "+logline);

			startMapJob(jobDetails); //Method to handle map job 
			response.setContentType("text/html");
			response.getWriter().println("<html><p>OK</p></html>");
		} catch (IOException e) {
			System.out.println("EXCEPTION: WorkerServlet:processRunMap: "+e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * Method takes the given request parameters and starts map job on requested number of threads
	 */
	private void startMapJob(JobDetails jobDetails) {
		//Create a resource management object
		currentJob = jobDetails;
		fileManagementObject = new FileManagement(storageDir, currentJob.getInputDir(), currentJob.getNumWorkers());

		//Instantiate a threadpool and run thread
		threadPool = new ArrayList<Thread>();
		currentJob.resetKeys();
		for(int i=0; i<currentJob.getNumThreads(); i++) {
			WorkerThread worker = new WorkerThread(currentJob.getJob(),fileManagementObject,this, true);
			Thread t = new Thread(worker);
			threadPool.add(t);
			t.start();
		}
	}

	/*
	 * Method called by threads on completion of task
	 */
	public synchronized void updateCompletion() {
		countOfCompletedThreads++;
		if(countOfCompletedThreads == currentJob.getNumThreads()) { //check count of threads against required number of threads to see if all threads finished
			System.out.println("System completed");
			if(status.equals("mapping")) { //on completions of map
				currentJob.isMapPhase = false;
				fileManagementObject.closeAllSpoolOut(); //close all references to spool out directory files
				new Thread(new PushDataThread(currentJob.getWorkerDetails(), this, fileManagementObject)).start(); //run push data on another thread
			} else if (status.equals("reducing")) { //on completion of reduce
				status = "idle"; //change status
				pastJob = currentJob; //past job keeps track of previous job (for keysWritten)
				fileManagementObject.closeReduceWriter();
				currentJob = null;
			}
		}
	}
	
	/*
	 * Method called by thread handling push data to update status of worker as waiting 
	 */
	public void updateStatusWaiting() {
		status = "waiting";

		Map<String, String> requestParameters = new HashMap<String, String>();
		requestParameters.put("port", ""+port);
		requestParameters.put("status",getState());
		requestParameters.put("job", getPresentJobName());
		requestParameters.put("keysRead", "" + getKeysRead());
		requestParameters.put("keysWritten", "" + getKeysWritten());
		String urlString = "http://" +masterIPPort + "/master/workerstatus";
		HttpClient client = new HttpClient();

		client.makeRequest(urlString, Integer.parseInt(masterIPPort.split(":")[1].trim()), requestParameters);
		if(client.getResponseCode()==200)
			System.out.println("WorkerServlet:updateStatusWaiting: Successful updation of master at: "+urlString);
		else 
			System.out.println("WorkerServlet:updateStatusWaiting: Unsuccessful updation of master at: "+urlString+" returned: "+client.getResponseCode());

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
		if(status.equals("mapping")||status.equals("reducing"))
			return currentJob.getKeysRead();
		if(status.equals("waiting"))
			return currentJob.getKeysRead();
		if(status.endsWith("idle"))
			return 0;
		return -1;
	}

	/*
	 * Method calls count of keys written according to specifics of status
	 */
	public int getKeysWritten() {
		if(status.equals("mapping")||status.equals("reducing"))
			return currentJob.getKeysWritten();
		if(status.equals("waiting"))
			return currentJob.getKeysWritten();
		if(status.endsWith("idle")) {
			if(pastJob!=null)
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
		if(currentJob==null)
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

class CrawlerStartHelper implements Runnable {
	private String args[];
	CrawlerStartHelper(String []ar) {
	args=ar;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			XPathCrawler.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}

