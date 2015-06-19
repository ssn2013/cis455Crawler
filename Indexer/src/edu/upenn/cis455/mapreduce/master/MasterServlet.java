package edu.upenn.cis455.mapreduce.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.sleepycat.je.rep.utilint.DbNullNode;

import storage.DBIndexerWrapper;
import storage.WriteToDocEntity;
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
																									// nodes
																									// and
																									// their
																									// informations
	private JobDetails presentMapJob; // details of present running job
	private String job = "edu.upenn.cis455.mapreduce.job.Indexer";
	private String inputDB = "/Users/Adi/Documents/workspace/Indexer/database";
	private String outputDB = "database_output";
	private int numMapThreads = 10;
	private int numReduceThreads = 10;
	/*
	 * Remove all this
	 */
	/*
	String con1 = "<p><b>China</b> (<a href=\"/wiki/Simplified_Chinese_characters\" title=\"Simplified Chinese characters\">"
			+ "simplified Chinese</a>: <span lang=\"zh-Hans\">中国</span>; "
			+ "<a href=\"/wiki/Traditional_Chinese_characters\" title=\"Traditional Chinese characters\">"
			+ "traditional Chinese</a>: <span lang=\"zh-Hant\">中國</span>; <a href=\"/wiki/Pinyin\" title=\"Pinyin\">pinyin</a>: <span lang=\"zh-Latn-pinyin\">Zhōngguó</span>), officially the"
			+ " <b>People's Republic of China</b> (<b>PRC</b>), is a <a href=\"/wiki/Sovereign_state\" title=\"Sovereign state\">sovereign state</a>"
			+ " located in <a href=\"/wiki/East_Asia\" title=\"East Asia\">East Asia</a>.";
	String con2 = "<div class=\"fc-item__standfirst\"><p>A frightened buffalo storms into a primary school in China, and chases students across a playground</p></div>"
			+ "<aside class=\"fc-item__meta js-item__meta\">"
			+ "<time class=\"fc-item__timestamp\" datetime=\"2015-04-16T18:57:39+0000\" data-timestamp=\"1429210659000\" data-relativeformat=\"short\">";
	
	String con3 = "<p>The <b>Internet</b> is a global system of interconnected <a href=\"/wiki/Computer_network\" title=\"Computer network\">computer networks</a> that use the standard <a href=\"/wiki/Internet_protocol_suite\" "
			+ "title=\"Internet protocol suite\">Internet protocol suite</a> (TCP/IP) to link several billion devices worldwide. It is a <i>network of networks</i><sup id=\"cite_ref-1\""
			+ " class=\"reference\"><a href=\"#cite_note-1\"><span>[</span>1<span>]</span></a></sup> that consists of millions of private, public, academic, business, and government "
			+ "networks of local to global scope, linked by a broad array of electronic, wireless, and optical networking technologies. The Internet carries an extensive range of information resources and services, such as the inter-linked <a href=\"/wiki/Hypertext\" title=\"Hypertext\">hypertext</a> documents and <a href=\"/wiki/Web_application\" title=\"Web application\">"
			+ "applications</a> of the <a href=\"/wiki/World_Wide_Web\" title=\"World Wide We\">World Wide Web</a> (WWW), the <a href=\"/wiki/Information_infrastructure\" title=\"Information infrastructure\">infrastructure</a> to support <a href=\"/wiki/Email\" title=\"Email\">email</a>, and <a href=\"/wiki/Peer-to-peer\" title=\"Peer-to-peer\">peer-to-peer</a> networks for "
			+ "<a href=\"/wiki/File_sharing\" title=\"File sharing\">file sharing</a>"
			+ "and <a href=\"/wiki/Voice_over_IP\" title=\"Voice over IP\">telephony</a>.</p>";
	
	*/

	/*
	 * doGet method of servlet
	 * 
	 * @see
	 * javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest
	 * , javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		
		System.out.println("MasterServlet:doGet: Got GET request "
				+ request.getServletPath());
		if (request.getServletPath().contains("workerstatus")) { // redirect worker
																// status calls
			processWorkerStatusRequest(request, response);
		} else if (request.getServletPath().contains("index")) {
			/*
			 * Remove from here
			 */
			/*DBIndexerWrapper wrapper = new DBIndexerWrapper(getServletContext()
					.getInitParameter("BDBstore"));
			wrapper.configure();
			WriteToDocEntity obj = new WriteToDocEntity();
			obj.writeToDb(con1, 1, "http://en.wikipedia.org/wiki/China",
					wrapper);
			obj.writeToDb(con2, 2, "http://www.theguardian.com/world/china",
					wrapper);
			obj.writeToDb(con1, 3, "http://english.gov.cn/", wrapper);
			obj.writeToDb(con3, 4, "http://en.wikipedia.org/wiki/Internet",
					wrapper);
			obj.writeToDb(con3, 5,
					"http://www.rcn.com/philadelphia/high-speed-internet",
					wrapper);
			obj.writeToDb(con3, 6, "https://www.isc.org/", wrapper);
			obj.writeToDb(con3, 7, "https://www.isc.org/mission/", wrapper);
			obj.writeToDb(con3, 8,
					"http://en.wikipedia.org/wiki/Internet_Systems_Consortium",
					wrapper);
			obj.writeToDb(con3, 9,
					"http://en.wikipedia.org/wiki/Internet_OS", wrapper);
			wrapper.exit();*/
			/*
			 * Remove till here
			 */
			processFormSubmissionPost();
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
		// System.out.println("Post request received: "+request.getPathInfo());
		// if(request.getPathInfo().contains("status")) { //redirect status
		// calls (from the form) to corresponding method
		// processFormSubmissionPost(request, response);
		// }
	}

	/*
	 * Method handling /status POST request from the form
	 */
	private void processFormSubmissionPost() {
		// Getting all values from form
		String className = job;
		String inputDir = inputDB;
		String outputDir = outputDB;
		int noMapThreads = numMapThreads;
		int noReduceThreads = numReduceThreads;
		JobDetails requestJob = new JobDetails(); // temporary object to store
													// details fetched from the
													// form
		requestJob.setJob(className);
		requestJob.setInputDir(inputDir);
		requestJob.setOutputDir(outputDir);
		requestJob.setNumMapThreads(noMapThreads);
		requestJob.setNumReduceThreads(noReduceThreads);

		// Sending data to the worker
		StringBuffer dataToSend = new StringBuffer("job=" + className); // forming
																		// the
																		// data
																		// part
																		// for
																		// /runmap
		dataToSend.append("&input=" + inputDir);
		dataToSend.append("&numThreads=" + noMapThreads);
		int i = 1;
		List<String> availableWorkers = new ArrayList<String>();
		// NEW CODE
		PrintWriter writer = null;
		try {
			writer = new PrintWriter("/Users/Adi/Documents/workspace/SearchEngine/workers.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		//
		for (String key : workerStatusMaps.keySet()) { // go through list of all
														// worker and look for
														// idle ones
			if (workerStatusMaps.get(key).getStatus().trim().equals("idle")) {
				availableWorkers.add(key);
				// WRITE KEY TO FILE (WORKER LIST)
				writer.println(key);
				dataToSend.append("&worker" + i + "=" + key);
				i++;
			}
		}
		writer.close();
		requestJob.setNumWorkers(availableWorkers.size()); // keep track of
															// worker working on
															// the job
		dataToSend.append("&numWorkers=" + availableWorkers.size());
		presentMapJob = requestJob; // if there are threads, set this to current
									// job
		String data = dataToSend.toString();
		System.out
				.println("MasterServlet:processFormSubmissionPost: Data being sent: "
						+ data);
		for (String key : availableWorkers) {
			String urlString = "http://" + key.trim() + "/worker/runmap";
			System.out.println("URL STRING: " + urlString);
			// Custom httpclient
			HttpClient httpClient = new HttpClient();
			InputStream responseBody = httpClient.makePostRequest(urlString,
					Integer.parseInt(key.split(":")[1].trim()),
					"application/x-www-form-urlencoded", data);

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
					.append("</table><br/><h2>Submit a job</h2><form method=\"post\" action=\"status\">"
							+ "<table><tr><td>Class: </td><td><input type=\"text\" name=\"class\"></td></tr>"
							+ "<tr><td>Input Dir:</td><td><input type=\"text\" name=\"inputDir\"></td></tr>"
							+ "<tr><td>Output Dir:</td><td><input type=\"text\" name=\"outputDir\"></td></tr>"
							+ "<tr><td>Number of Map Threads:</td><td><input type=\"text\" name=\"noMapThreads\"></td></tr>"
							+ "<tr><td>No of Reduce Threads:</td><td><input type=\"text\" name=\"noReduceThreads\"></td></tr>"
							+ "<tr><td colspan=\"2\"><input type=\"submit\" value=\"Run\"></td></tr></table></form>"
							+ "<br/><p>Coded by: Sruthi Nair (sruthin@seas.upenn.edu)</p>");
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
			System.out.println("STATUS: ---- "+status);
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
			workerStatusMaps.put(workerStatusMap.getIPPort(), workerStatusMap); // put
																				// details
																				// into
																				// the
																				// server's
																				// map
																				// of
																				// all
																				// workers'
																				// details
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
			if(presentMapJob != null){
				checkAndRunReduce(job);
			}// check if relevant threads are waiting and
									// run reduce
			response.setStatus(200);
		} catch (Exception e) {
			System.out
					.println("EXCEPTION: MasterServlet:processWorkerStatusRequest "
							+ e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * Method called at every /workerstatus request to check if all workers are
	 * "waiting" and run reduce
	 */
	private void checkAndRunReduce(String jobName) {
		int count = 0;
		
		for (String key : workerStatusMaps.keySet()) { // for the given job,
			System.out.println(key);											// take a count of all
														// the waiting workers
			WorkerStatusMap map = workerStatusMaps.get(key);
			if (map.getJob().equals(jobName)
					&& map.getStatus().equals("waiting"))
				count++;
		}

		if (count == presentMapJob.getNumWorkers()) { // if the count equals the
														// number of workers
														// initially assigned to
														// the job, start reduce
														// phase
			// make the body String
			StringBuffer buf = new StringBuffer();
			buf.append("job=" + presentMapJob.getJob());
			buf.append("&output=" + presentMapJob.getOutputDir());
			buf.append("&numThreads=" + presentMapJob.getNumReduceThreads());
			String body = buf.toString();

			// for each given worker make /runreduce call
			for (String key : workerStatusMaps.keySet()) {
				HttpClient client = new HttpClient();
				String url = "http://" + key + "/worker/runreduce";
				client.makePostRequest(url,
						Integer.parseInt(key.split(":")[1].trim()),
						"application/x-www-form-urlencoded", body);
			}
		}
	}
}