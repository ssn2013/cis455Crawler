package com.datformers.servlets;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;
import org.json.JSONArray;
import org.json.JSONObject;

import com.datformers.crawler.XPathCrawler;
import com.datformers.crawler.resources.OutgoingMap;
import com.datformers.crawler.resources.URLQueue;
import com.datformers.mapreduce.util.JobDetails;
import com.datformers.mapreduce.worker.resources.FileManagement;
import com.datformers.mapreduce.worker.resources.WorkerStatusUpdator;
import com.datformers.mapreduce.worker.resources.WorkerThread;
import com.datformers.resources.HttpClient;
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
	static String storageDir;
	private String dbDir;
	private int countOfCompletedThreads = 0;
	public static String STATUS = "idle";
	private WorkerStatusUpdator wk;
	private Thread wkt;
	public static String seedUrl[];
	public String otherWorkers[];

	//Job handling
	public JobDetails currentJob = null;
	private JobDetails pastJob = null;

	public void init(ServletConfig servletConfig) throws javax.servlet.ServletException {
//		super.init(servletConfig);
		masterIPPort = servletConfig.getInitParameter("master"); //fetch details of master
		storageDir = servletConfig.getInitParameter("storagedir"); //fetch details of storage directory
		if(storageDir.endsWith("/")) storageDir=storageDir.substring(0,storageDir.length()-1);   //the store dir WLL not have / at end!! 
		dbDir = servletConfig.getInitParameter("databasedir"); //fetch details of database directory
		DBWrapper.initialize(dbDir); //initialize DB environment
//		DBWrapper.close();
		port = Integer.parseInt(servletConfig.getInitParameter("port")); //fetch port details
		String points[] = masterIPPort.split(":");
		wk = new WorkerStatusUpdator(points[0].trim(), points[1].trim(), port, this); //create thread to send update status calls to master every 10 seconds
		wkt = new Thread(wk);
		wkt.start();
		System.out.println("SERVLET STARTED:");
		
		
	}
	
	@Override
	public void destroy() {
		System.out.println("this is stopping");
//		wkt.stop();
		wkt.interrupt();
		DBWrapper.close();
		
		for(Thread t: threadPool) {
			if(t!=null)
				t.interrupt();
		}
		for(Thread t: XPathCrawler.subThreads) {
			if(t!=null)
				t.interrupt();
		}
//		
		
//		
//		super.destroy();
	}

	/*
	 * doGet of Servlet
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
	{
		if(request.getPathInfo().contains("stopcrawl")) {
			processStopCrawl(request, response); //redirect to method handling crawl stop
			STATUS = "idle";
		} else if (request.getPathInfo().contains("checkpoint")) {
			STATUS = "checkpointing";
			processCreateCheckpoint(request, response); //redirect to method checkpointing
		} else {
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.println("<html><head><title>Worker</title></head>");
			out.println("<body>Hi, I am the worker!</body></html>");
		}

	}

	/*
	 * doPost of servlet
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		System.out.println("WorkerServlet:doPost GOT: "+request.getPathInfo());
		if(request.getPathInfo().contains("startcrawl")) {
			STATUS = "crawling";
			processRunCrawl(request, response); //redirect to method handling for crawl start	
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
		System.out.println("GOT REQUEST TO STOP CRAWLER");
		STATUS = "idle";
		XPathCrawler.STOP_CRAWLER=true;
		
	}

	private void processCreateCheckpoint(HttpServletRequest request,
			HttpServletResponse response) {
		//XPathCrawler.STOP_CRAWLER=true;
		
		System.out.println("GOT REQUEST TO START CHECKPONITING: ");
		if(OutgoingMap.getInstance()==null) {
			System.out.println("Outgoing Map not existent");
			response.setStatus(500);
			return;
		}
		
		
			//Create a separate thread to parallely run checkpoint
			System.out.println("WORKER: Checkpointing Part");
			CheckPointThread ct=new CheckPointThread(this);
			new Thread(ct).start();
			
			
		response.setStatus(200);
	}
	
	private void processRunCrawl(HttpServletRequest request,
			HttpServletResponse response) {
		System.out.println("AND WE GOT CRAWL FROM MASTER");
		try {
			//save the WorkerServlet object in XPathCrawler
			XPathCrawler.setWorkerServletOb(this);
			StringBuilder buffer = new StringBuilder();
			BufferedReader reader = request.getReader();
			String line = null;
			System.out.println("WORSER SERVLET: REQUEST: "+request.getContentType());
			while ((line = reader.readLine()) != null) {
				System.out.println("WORKER SERVLET: REQUEST BODY LINE GOT: "+line);
				buffer.append(line + "\n");
			}
			String body = buffer.toString();
			JSONObject obj;
			System.out.println("GOT BODY FROM MASTER FOR RUN: "+body);
			obj = new JSONObject(body);
			JSONArray url = obj.getJSONArray("urls");
			JSONArray crawlWorkers = obj.getJSONArray("crawler");
			String maxRequests=obj.getString("maxRequests");
					
			//start the crawling
			String args[]=new String[3];
			if (url.length()>0) {
				seedUrl = new String[url.length()];
				for (int i = 0; i < url.length(); i++) {
					// seedUrl[i] = url.getJSONObject(i).getString("host");
					seedUrl[i]= url.getString(i).toString();
//					args[0]+= url.getString(i).toString()+delim;
				}
				//removing last occurrence of delim
			} else {
				seedUrl = null;
			}
			otherWorkers = new String [crawlWorkers.length()];
			args[0]=storageDir;
			args[1]=""+5;
			args[2]=maxRequests;
			for (int i = 0; i < crawlWorkers.length(); i++) {
				// workers[i] = crawlWorkers.getJSONObject(i).getString("host");
				otherWorkers[i]=crawlWorkers.getString(i).toString();
			}
			
			countOfCompletedThreads = 0;
			CrawlerStartHelper myrunnable = new CrawlerStartHelper(args,seedUrl,otherWorkers);
	     	new Thread(myrunnable).start();
			
	     	response.setContentType("text/plain");
	     	String responseSTUFF = "HELLO";
	     	response.setContentLength(responseSTUFF.getBytes().length);
	     	response.getWriter().println(responseSTUFF);
	     	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/*
	 * Method called by threads on completion of task
	 */
	public synchronized void updateCompletion() {
		countOfCompletedThreads++;
		System.out.println("THREAD REPORTED COMPLETED: TOTAL COUNT: "+countOfCompletedThreads+" REQUIRED COUNT: "+otherWorkers.length+" reported by thread: "+Thread.currentThread().getName());
		if(countOfCompletedThreads == otherWorkers.length) { //check count of threads against required number of threads to see if all threads finished
			System.out.println("System completed");
			STATUS="idle";
			updateStatusToMaster();
		}
	}
	
	/*
	 * Method called by thread handling push data to update status of worker as waiting 
	 */
	public void updateStatusToMaster() {

		Map<String, String> requestParameters = new HashMap<String, String>();
		requestParameters.put("port", ""+port);
		requestParameters.put("status",getState());
		requestParameters.put("job", getPresentJobName());
		requestParameters.put("keysRead", "" + getKeysRead());
		requestParameters.put("keysWritten", "" + getKeysWritten());
		requestParameters.put("totalURLCount", "" + XPathCrawler.totalURLCount);
		String urlString = "http://" +masterIPPort + "/master/workerstatus";
		HttpClient client = new HttpClient();

		client.makeGetRequest(urlString, Integer.parseInt(masterIPPort.split(":")[1].trim()), requestParameters);
		if(client.getResponseStatusCode()==200)
			System.out.println("WorkerServlet:updateStatusWaiting: Successful updation of master at: "+urlString);
		else 
			System.out.println("WorkerServlet:updateStatusWaiting: Unsuccessful updation of master at: "+urlString+" returned: "+client.getResponseStatusCode());

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
		if(STATUS.equals("mapping")||STATUS.equals("reducing"))
			return currentJob.getKeysRead();
		if(STATUS.equals("waiting"))
			return currentJob.getKeysRead();
		if(STATUS.endsWith("idle"))
			return 0;
		return -1;
	}

	/*
	 * Method calls count of keys written according to specifics of status
	 */
	public int getKeysWritten() {
		if(STATUS.equals("mapping")||STATUS.equals("reducing"))
			return currentJob.getKeysWritten();
		if(STATUS.equals("waiting"))
			return currentJob.getKeysWritten();
		if(STATUS.endsWith("idle")) {
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
		return STATUS;
	}
}

class CrawlerStartHelper implements Runnable {
	private String args[];
	private String urls[];
	private String workers[];
	CrawlerStartHelper(String []ar,String []u,String []w) {
	urls=u;
	args=ar;
	workers=w;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			XPathCrawler.start(args,urls,workers);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}

class CheckPointThread implements Runnable {
	private WorkerServlet ws;
	
	CheckPointThread(WorkerServlet w) {
	ws=w;
	
	}
	/*
	 * 
	 * This function is used to delete the directory
	 */
	public boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		// The directory is now empty or this is a file so delete it
		return dir.delete();
	}

	/*
	 * 
	 * This function is used to create the directory
	 */
	public void createDir(String dir) {

		File f = new File(dir);
		if (f.exists()) {
			deleteDir(f);
		}
		if (!f.mkdir()) {
			System.out.println(dir);
			System.out.println("Error in creating Dir!!");
			return;
		}

	}

	@Override
	public void run() {
//		String spoolIn = ws.storageDir + "/spool_in";
//		createDir(spoolIn);
		System.out.println("RUN FOR CHECKPOINTING THREAD");
		ws.fileManagementObject = new FileManagement(ws.storageDir, null, 0);
		OutgoingMap map=OutgoingMap.getInstance();
		URLQueue queue = URLQueue.getInstance();
		ws.threadPool = new ArrayList<Thread>();
		System.out.println("Threadpool created");
		for(int i=0;i<ws.otherWorkers.length;i++) {
			System.out.println("other work: "+ws.otherWorkers[i]);
			if(i==0) {
				if(queue.isEmpty()) {
					System.out.println("the queue is empty");
					ws.updateCompletion();
					continue;
				}
				WorkerThread worker = new WorkerThread(queue.getQueue(),ws.fileManagementObject,ws,ws.storageDir+"/spoolIn/"+ws.otherWorkers[i]);
				Thread t = new Thread(worker);
				ws.threadPool.add(t);
				t.start();
				
			}
			else {
				if(map.getQueueAtIndex(i).isEmpty()) {
					ws.updateCompletion();
					continue;
				}
				WorkerThread worker = new WorkerThread(map.getQueueAtIndex(i),ws.fileManagementObject,ws,ws.storageDir+"/spoolOut/"+ws.otherWorkers[i]);
				Thread t = new Thread(worker);
				ws.threadPool.add(t);
				t.start();
			}
			
		}
		
	}
	
	
}

