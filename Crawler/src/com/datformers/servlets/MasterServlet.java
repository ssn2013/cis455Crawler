package com.datformers.servlets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

import com.datformers.resources.CrawlerStatus;
import com.datformers.resources.HttpClient;

public class MasterServlet extends HttpServlet{
	private Map<String, CrawlerStatus> crawlerStatusMap = new HashMap<String, CrawlerStatus>();
	private String seedFileName;
	private int maxRequestsPerCrawler;
	private int maxRequests;
	public BigInteger hashRange[];
	public String workers[];
	private String crawlerFilePath="/mnt/crawlers";
	private String crawl_status ="idle";
	public void init(ServletConfig servletConfig) throws javax.servlet.ServletException {
		super.init(servletConfig);
		seedFileName = getServletConfig().getInitParameter("SeedURlFile");
		maxRequestsPerCrawler = Integer.parseInt(getServletConfig().getInitParameter("MaxRequestsPerCrawler"));
		maxRequests = Integer.parseInt(getServletConfig().getInitParameter("TotalMaxRequests"));
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException  {
		try{
			if(request.getPathInfo()!=null&&request.getPathInfo().contains("workerstatus")) {
				
				String ipAddress = request.getHeader("X-FORWARDED-FOR");
				if (ipAddress == null) {  
					ipAddress = request.getRemoteAddr();  
				}
				int port = Integer.parseInt(request.getParameter("port").trim());
				int totalProcessed  = Integer.parseInt(request.getParameter("totalURLCount").trim());
				String status = request.getParameter("status");
				//System.out.println("port: "+port+" status: "+status+" ipaddress: "+ipAddress);
				
				//set crawler status
				CrawlerStatus crawlerStatus = new CrawlerStatus();
				crawlerStatus.setIpAddress(ipAddress);
				crawlerStatus.setPort(port);
				crawlerStatus.setStatus(status);
				crawlerStatus.setTotalProcessed(totalProcessed);

				System.out.println("\n\nIPAddres: "+crawlerStatus.getIpAddress()
						+"\nPORT: "+crawlerStatus.getPort()
						+"\nSTATUS: "+crawlerStatus.getStatus());

				//add to map
				crawlerStatusMap.put(crawlerStatus.getIpPortString(), crawlerStatus);
				
				//System.out.println("MASTER STATUS: "+crawl_status);
				//Check if time for checkpointing
				if(crawl_status.equals("crawling")&&checkForCheckpoiting())
					callForCheckpoint();
				if(checkForCompletion())
					stopCrawling();
				if(crawl_status.equals("checkpoint")&&checkForCheckpoitingCompletion())
					makeCrawlRequests(true);
			} else if(request.getPathInfo()!=null&&request.getPathInfo().contains("startCrawling")) {
				doHashDiv(false);
				//Make crawl requests to all crawlers
				makeCrawlRequests(true);
			} else if(request.getPathInfo()!=null&&request.getPathInfo().contains("resumeCrawling")) {
				doHashDiv(true);
				//Make crawl requests to all crawlers
				makeCrawlRequests(false);
			} else if(request.getPathInfo()!=null&&request.getPathInfo().contains("stopCrawling")) {
				//Stop all crawling();
				stopCrawling();
			} else {
				StringBuffer htmlBuffer = new StringBuffer("<html><body>");
				htmlBuffer.append("<form method=\"get\" action=\"master/startCrawling\"><input type=\"submit\" value=\"Start Crawling\"></form><br/>");
				htmlBuffer.append("<form method=\"get\" action=\"master/stopCrawling\"><input type=\"submit\" value=\"Stop Crawling\"></form>");
				htmlBuffer.append("<form method=\"get\" action=\"master/resumeCrawling\"><input type=\"submit\" value=\"Resume Crawling\"></form>");
				int sum = 0;
				for(String key: crawlerStatusMap.keySet()) {
					
					sum+=crawlerStatusMap.get(key).getTotalProcessed();
				}
				
				htmlBuffer.append("<p>Total Requests Processed Till now="+sum+"</p>");
				htmlBuffer.append("<p>Total Crawler nodes"+crawlerStatusMap.size()+"</p>");
				htmlBuffer.append("</body></html>");
				response.getWriter().println(htmlBuffer.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private boolean checkForCheckpoitingCompletion() {
		int count = 0;
		for(String key: crawlerStatusMap.keySet()) {
			if(crawlerStatusMap.get(key).getStatus().equals("idle"))
				count++;
		}
		if(count==crawlerStatusMap.keySet().size())
			return true;
		else 
			return false;
	}

	private boolean checkForCompletion() {
		int sum = 0;
		for(String key: crawlerStatusMap.keySet()) {
			sum+=crawlerStatusMap.get(key).getTotalProcessed();
		}
		int emptied = 0;
		for(String key: crawlerStatusMap.keySet()) {
			if(crawlerStatusMap.get(key).getStatus().equals("queue_emptied"))
				emptied++;
		}
		
		if(sum>=maxRequests || emptied==crawlerStatusMap.keySet().size())
			return true;
		else
			return false;
	}

	private void callForCheckpoint() {
		for(String key: crawlerStatusMap.keySet()) {
			HttpClient client = new HttpClient();
			System.out.println("starting checkpoinint!!");
//			System.out.println("SENDING: STOP CRAWL TO: "+"http://"+key+"/crawler/checkpoint"
//					+"\nPORT: "+ crawlerStatusMap.get(key).getPort());
			client.makeGetRequest("http://"+key+"/crawler/checkpoint", crawlerStatusMap.get(key).getPort(), new HashMap<String, String>());
		}
		crawl_status = "checkpoint";
	}

	private boolean checkForCheckpoiting() {
		int countOfDoneWorkers = 0;
		int countOfEmptyQueueWorkers = 0;
		for(String key: crawlerStatusMap.keySet()) {
			if(crawlerStatusMap.get(key).getStatus().equals("done")) 
				countOfDoneWorkers++;
			else if (crawlerStatusMap.get(key).getStatus().equals("queue_emptied"))
				countOfEmptyQueueWorkers++;
		}
//		System.out.println("CHECK FOR CHECKPOINTING: COUNT "+countOfDoneWorkers+" VALUE: "+(crawlerStatusMap.keySet().size()==countOfDoneWorkers));
		if((countOfDoneWorkers+countOfEmptyQueueWorkers) == crawlerStatusMap.keySet().size())
			return true;
		else 
			return false;
	}

	private void stopCrawling() {
		//Make request to stop crawling
		for(String key: crawlerStatusMap.keySet()) {
			HttpClient client = new HttpClient();
//
//			System.out.println("SENDING: STOP CRAWL TO: "+"http://"+key+"/crawler/stopcrawl"
//					+"\nPORT: "+ crawlerStatusMap.get(key).getPort());
			client.makeGetRequest("http://"+key+"/crawler/stopcrawl", crawlerStatusMap.get(key).getPort(), new HashMap<String, String>());
		}
		crawl_status = "idle";
	}

	public void createCrawlerFiles() {
		File f=new File(crawlerFilePath);
		workers=crawlerStatusMap.keySet().toArray(new String[crawlerStatusMap.keySet().size()]);
		try {
			f.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(
					new FileWriter(f,true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=0;i<workers.length;i++) {
			out.println(workers[i]);
		}
		out.close();
	}
	public void readCrawlerFiles() {
		File f=new File(crawlerFilePath);
		if(f.exists()) {
			String line;String craw="";
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
				while ((line = br.readLine()) != null) {
					if(craw.equals("")) craw=line;
					else craw=craw+";;;"+line;
				}
				workers=craw.split(";;;");
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			System.out.println("Crawlers file does not exists!!");
		}
	}
	public void doHashDiv(boolean readFromFile) {
		 
		if(!readFromFile) createCrawlerFiles();
		else readCrawlerFiles();
		
		if(hashRange!=null) return;
				hashRange=new BigInteger[crawlerStatusMap.keySet().size()];
		BigInteger range = new BigInteger(
				"ffffffffffffffffffffffffffffffffffffffff", 16)
				.divide(new BigInteger("" + workers.length));
		hashRange[0] = range;
		for (int i = 1; i < workers.length; i++) {

			BigInteger temp = new BigInteger("" + hashRange[i - 1]).add(range);

			hashRange[i] = temp;

		}

	}
	public static BigInteger convertToBigInt(byte[] data) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = (data[i] >>> 4) & 0x0F;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return new BigInteger(buf.toString(), 16);
	}

	/*
	 * 
	 * This function is used to calculate the SHA1 hash
	 */
	public static BigInteger SHA1(String text) {
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("SHA-1");

			byte[] sha1hash = new byte[40];
			md.update(text.getBytes("iso-8859-1"), 0, text.length());
			sha1hash = md.digest();
			// String string=convertToHex(sha1hash);
			return convertToBigInt(sha1hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		// return new BigInteger(sha1hash);
	}

	public int findIndex(String key) {
		BigInteger b = null;
		b = SHA1(key);
		//System.out.println("key:"+key+"="+b);
		int index = 0;
		for (int i = 0; i < hashRange.length; i++) {
			if (b.compareTo(hashRange[i]) <= 0) {
				index = i;
				break;
			}
		}
		return index;
	}
	
	private void makeCrawlRequests(boolean readFromFile) {
		//Read seed URLs
		FileReader fileReader;
		try {
			Map<String, ArrayList<String>> crawlerToUrlMap = new HashMap<String, ArrayList<String>>();
			if(readFromFile) {
				fileReader = new FileReader(new File(seedFileName));
				BufferedReader br = new BufferedReader(fileReader);
				List<String> seedUrls = new ArrayList<String>();
				String line = null;
				while((line = br.readLine())!=null) {
					seedUrls.add(line);
				}

				//Divide URLS among crawlers
				//TODO: implement URL hashing
				//int urlsPerCrawlerCount = seedUrls.size()/crawlerStatusMap.keySet().size();
				//int ind = 0;String last;
				for(String key: crawlerStatusMap.keySet()) {
					ArrayList<String> urls = new ArrayList<String>();
					crawlerToUrlMap.put(key, urls);
				}
				
				String[] crawlers = workers;
				
				for(String url:seedUrls) {
					int index=findIndex(url);
					
					crawlerToUrlMap.get(crawlers[index]).add(url);
					
				}
			}		
			//Form Json object for the request
			JSONArray crawlerList = new JSONArray(crawlerStatusMap.keySet().toArray(new String[crawlerStatusMap.keySet().size()]));			

			//Send seed URLs to each crawler
			for(String key: crawlerStatusMap.keySet()) {
				HttpClient client = new HttpClient();
				JSONObject requestObject = new JSONObject();
				if(readFromFile)
					requestObject.put("urls", new JSONArray(crawlerToUrlMap.get(key).toArray()));
				else
					requestObject.put("urls", new JSONArray());
				requestObject.put("maxRequests", maxRequestsPerCrawler);
				
				//And here were are making life more complicated
				String[] ipSet = new String[crawlerStatusMap.keySet().size()];
				int i=1;
				for(String key2: workers) {
					if(key.equals(key2))
						continue;
					ipSet[i++] = key2;
				}
				ipSet[0] = key;
				requestObject.put("crawler", new JSONArray(workers));
				requestObject.put("self", key);

//				System.out.println("SENDING: TO:"+"http://"+key+"/crawler/startcrawl"
//						+"\nPORT: "+crawlerStatusMap.get(key).getPort()
//						+"\nCONTENT TYPE: "+"application/json"
//						+"\nBODY STRING: "+requestObject.toString());

				client.makePostRequest("http://"+key+"/crawler/startcrawl", crawlerStatusMap.get(key).getPort(), "application/json", requestObject.toString());
			}
			crawl_status = "crawling";
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException {
		
	}
}
