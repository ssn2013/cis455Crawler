package com.datformers.servlets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
				System.out.println("port: "+port+" status: "+status+" ipaddress: "+ipAddress);
				
				//set crawler status
				CrawlerStatus crawlerStatus = new CrawlerStatus();
				crawlerStatus.setIpAddress(ipAddress);
				crawlerStatus.setPort(port);
				crawlerStatus.setStatus(status);
				crawlerStatus.setTotalProcessed(totalProcessed);

				System.out.println("IPAddres: "+crawlerStatus.getIpAddress()
						+"\nPORT: "+crawlerStatus.getPort()
						+"\nSTATUS: "+crawlerStatus.getStatus());

				//add to map
				crawlerStatusMap.put(crawlerStatus.getIpPortString(), crawlerStatus);
				
				//Check if time for checkpointing
				if(checkForCheckpoiting())
					callForCheckpoint();
				if(checkForCompletion())
					stopCrawling();
				if(checkForCheckpoitingCompletion())
					makeCrawlRequests(false);
			} else if(request.getPathInfo()!=null&&request.getPathInfo().contains("startCrawling")) {
				//Make crawl requests to all crawlers
				makeCrawlRequests(true);
			} else if(request.getPathInfo()!=null&&request.getPathInfo().contains("stopCrawling")) {
				//Stop all crawling();
				stopCrawling();
			} else {
				StringBuffer htmlBuffer = new StringBuffer("<html><body>");
				htmlBuffer.append("<form method=\"get\" action=\"master/startCrawling\"><input type=\"submit\" value=\"Start Crawling\"></form><br/>");
				htmlBuffer.append("<form method=\"get\" action=\"master/stopCrawling\"><input type=\"submit\" value=\"Stop Crawling\"></form>");
				htmlBuffer.append("</body></html>");
				response.getWriter().println(htmlBuffer.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private boolean checkForCheckpoitingCompletion() {
		if(crawl_status.endsWith("crawling")) {
			int count = 0;
			for(String key: crawlerStatusMap.keySet()) {
				if(crawlerStatusMap.get(key).getStatus().equals("idle"))
					count++;
			}
			if(count==crawlerStatusMap.keySet().size())
				return true;
			else 
				return false;
		} else {
			return false;
		}
	}

	private boolean checkForCompletion() {
		int sum = 0;
		for(String key: crawlerStatusMap.keySet()) {
			sum+=crawlerStatusMap.get(key).getTotalProcessed();
		}
		if(sum==maxRequests)
			return true;
		else
			return false;
	}

	private void callForCheckpoint() {
		for(String key: crawlerStatusMap.keySet()) {
			HttpClient client = new HttpClient();

			System.out.println("SENDING: STOP CRAWL TO: "+"http://"+key+"/crawler/checkpoint"
					+"\nPORT: "+ crawlerStatusMap.get(key).getPort());
			client.makeGetRequest("http://"+key+"/crawler/checkpoint", crawlerStatusMap.get(key).getPort(), new HashMap<String, String>());
		}
		crawl_status = "checkpoint";
	}

	private boolean checkForCheckpoiting() {
		int countOfDoneWorkers = 0;
		for(String key: crawlerStatusMap.keySet()) {
			if(crawlerStatusMap.get(key).getStatus().equals("done")) 
				countOfDoneWorkers++;
		}
		System.out.println("CHECK FOR CHECKPOINTING: COUNT "+countOfDoneWorkers+" VALUE: "+(crawlerStatusMap.keySet().size()==countOfDoneWorkers));
		if(countOfDoneWorkers == crawlerStatusMap.keySet().size())
			return true;
		else 
			return false;
	}

	private void stopCrawling() {
		//Make request to stop crawling
		for(String key: crawlerStatusMap.keySet()) {
			HttpClient client = new HttpClient();

			System.out.println("SENDING: STOP CRAWL TO: "+"http://"+key+"/crawler/stopcrawl"
					+"\nPORT: "+ crawlerStatusMap.get(key).getPort());
			client.makeGetRequest("http://"+key+"/crawler/stopcrawl", crawlerStatusMap.get(key).getPort(), new HashMap<String, String>());
		}
		crawl_status = "idle";
	}

	private void makeCrawlRequests(boolean readFromFile) {
		//Read seed URLs
		FileReader fileReader;
		try {
			Map<String, String[]> crawlerToUrlMap = new HashMap<String, String[]>();
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
				int urlsPerCrawlerCount = seedUrls.size()/crawlerStatusMap.keySet().size();
				int ind = 0;
				for(String key: crawlerStatusMap.keySet()) {
					String urls[] = new String[urlsPerCrawlerCount];
					for(int i = 0; i<urlsPerCrawlerCount; i++) {
						urls[i] = seedUrls.get(ind++);
					}
					crawlerToUrlMap.put(key, urls);
				}
			} 

			//Form Json object for the request
			JSONArray crawlerList = new JSONArray(crawlerStatusMap.keySet().toArray(new String[crawlerStatusMap.keySet().size()]));			

			//Send seed URLs to each crawler
			for(String key: crawlerStatusMap.keySet()) {
				HttpClient client = new HttpClient();
				JSONObject requestObject = new JSONObject();
				if(readFromFile)
					requestObject.put("urls", new JSONArray(crawlerToUrlMap.get(key)));
				requestObject.put("maxRequests", maxRequestsPerCrawler);
				
				//And here were are making life more complicated
				String[] ipSet = new String[crawlerStatusMap.keySet().size()];
				int i=1;
				for(String key2: crawlerStatusMap.keySet()) {
					if(key.equals(key2))
						continue;
					ipSet[i++] = key2;
				}
				ipSet[0] = key;
				requestObject.put("crawler", new JSONArray(ipSet));

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
