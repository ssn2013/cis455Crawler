package com.datformers.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;
import org.xml.sax.SAXException;

import com.sleepycat.persist.PrimaryIndex;

import com.datformers.crawler.info.RobotsTxtInfo;
import com.datformers.crawler.resources.DomainRules;
import com.datformers.crawler.resources.ResourceManagement;
import com.datformers.crawler.resources.URLQueue;
import com.datformers.storage.DBWrapper;
import com.datformers.storage.ParsedDocument;

/*
 * Class of individual crawler threads that actually process the file
 */
public class XPathCrawlerThread implements Runnable{
	private ResourceManagement resourceManagement;
	public String domain; 
	public String url;
	public boolean newResource = false;
	private XPathCrawler parent = null;
	private static String allowedMimeTypes[] = {"text/html","text/xml", "application/xml","application/atom+xml",
		"application/dash+xml", "application/rdf+xml", "application/rss+xml",
		"application/soap+xml", "application/xhtml+xml", "application/xop+xml",
		"application/smil+xml", "image/svg+xml", "message/imdn+xml", "model/x3d+xml",
		"application/vnd.mozilla.xul+xml", "application/vnd.google-earth.kml+xml"}; //list of allowed MIME types 
	private boolean isHttps = false; 
	private DBWrapper wrapper = null;
	public XPathCrawlerThread(XPathCrawler parent) {
		this.parent = parent;
		wrapper = new DBWrapper();
	}
	/*
	 * Method to extract domain from a URL string
	 */
	private String getDomain(String url) {
		String host = url.substring(url.indexOf('/')+2); //remove protocol part
		if(host.contains("/"))
			host = host.substring(0, host.indexOf('/'));//remove any additional paths 
		return host;
	}
	/*
	 * Thread's run method. 
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		URLQueue queue = URLQueue.getInstance();
		try {
			while(!XPathCrawler.SYSTEM_SHUTDOWN) { //run as long as system is not supposed to shutdown
				String url = queue.getUrl();
				if(url==null) 
					return;
				System.out.println(Thread.currentThread().getName()+" FETCHED URL FROM QUEUE: "+url);
				this.domain = getDomain(url);
				this.url = url;
				if(parent.getRulesForDomain(domain)==null) //A url is unlikely to have domain specific rules if it's fetched the first time
					newResource = true;
				else 
					newResource = false;
				executeTask(); //task of the thread
			}
		} catch (InterruptedException e) {
			System.out.println("Crawler thread got interrupted :(");
			e.printStackTrace();
		}
	}
	/*
	 * Method of the tasks of a crawler thread
	 */
	public void executeTask() {
		try {
			boolean httpsFlag = false;
			if(url.startsWith("https")) //Check if url is HTTPS
				httpsFlag = true;

			resourceManagement = new ResourceManagement(url, httpsFlag); 
			resourceManagement.addRequestHeader("User-Agent", "cis455crawler"); //User-agent header for requests
			resourceManagement.addRequestHeader("Connection", "close"); //Connection close header to make the communications quick

			if(newResource) {  //For a new URL, fetch and parse robots.txt
				if(parent.getRulesForDomain(getDomain(url))==null) {
					RobotsTxtInfo r = fetchAndParseRobots();
					if(r==null)
						return;
					else
						parent.setRobotsTxtInfo(getDomain(url), r); //add parsed robots.txt to domain specific rules
				}
			}
			//validate crawling with robots.txt rules
			if(!isCrawlingAllowed(parent.getRulesForDomain(getDomain(url))))
				return;

			//check if in DB, if so add last modified date
			PrimaryIndex<String, ParsedDocument> indexDocuments = wrapper.getStore().getPrimaryIndex(String.class, ParsedDocument.class);
			ParsedDocument gotDoc = indexDocuments.get(url);
			String checkDate = null;
			if(gotDoc!=null) {
				SimpleDateFormat format1 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
				TimeZone gmtTime = TimeZone.getTimeZone("GMT");
				format1.setTimeZone(gmtTime);
				checkDate = format1.format(gotDoc.getLastAccessedDate());
			}
			
			resourceManagement.addRequestHeader("If-Modified-Since", checkDate);
			//make head request  
			resourceManagement.makeHeadRequest();
			if(resourceManagement.getResponseStatus()==304) //Checking unmodified date
				return;

			//Check mime type conforms
			String mimeType = resourceManagement.getResponseHeader("Content-Type");
			boolean isAllowedType = false;
			for(String allowedType: allowedMimeTypes) {
				if(mimeType.toLowerCase().contains(allowedType)) {
					isAllowedType = true;
					break;
				}
			}
			if(!isAllowedType) {
				System.out.println("Not allowed type: "+mimeType);
				return;
			}

			//maximimum size check
			if(resourceManagement.getResponseHeader("Content-Length")==null ) {
				System.out.println("Invalid or missing header for Content-length");
				return;
			}
			int sizeOfFile = Integer.parseInt(resourceManagement.getResponseHeader("Content-Length"));
			if(sizeOfFile>parent.MAX_SIZE) {
				System.out.println("Above maximum allowed size: "+sizeOfFile);
				return;
			}

			//make get request
			InputStream bodyStream = resourceManagement.makeGetRequest();

			//parse File
			Document doc = null;
			try {
				doc = parseDOM(bodyStream);
			} catch (ParserConfigurationException | SAXException | IOException e) {
				e.printStackTrace();
				System.out.println("Parsing threw error: "+e.getMessage());
			}

			//extract links
			if(doc==null) {
				System.out.println("Some issue occurred in parsing, document object is null");
				return;
			}

			//Save Parsed file to database
			writeFileToDatabase();

			if(!resourceManagement.isHtml) //Links are not extracted from files which are not HTML
				return;
			ArrayList<String> extractedUrls = extractLinks(doc);
			URLQueue queue = URLQueue.getInstance(); //url queue
			for(String str: extractedUrls) {
				System.out.println(Thread.currentThread().getName()+" Pushing to Queue: "+str);
				queue.add(str); //add extracted links
			} 
		} catch (Exception e) {
			System.out.println("ERROR IN TASK: "+e.getMessage());
			e.printStackTrace();
			System.out.println("Continuing with next task");
			return;
		}
	}
	/*
	 * Method to handle writing to database
	 */
	private void writeFileToDatabase() {
		ParsedDocument document = new ParsedDocument();
		document.setUrl(url);
		document.setDocumentContents(resourceManagement.getBody());
		document.setLastAccessedDate(new Date());
		PrimaryIndex<String, ParsedDocument> indexDoc = wrapper.getStore().getPrimaryIndex(String.class, ParsedDocument.class);
		indexDoc.put(document);
		XPathCrawler.addCounter(); //increase counter of files succesfully fetched and parsed
		if(!newResource) { //update next allowed access time for domain
			DomainRules domainRules = parent.getRulesForDomain(getDomain(url));
			domainRules.setNextAccessTime();
		}
	}
	/*
	 * Method to extract links
	 */
	public ArrayList<String> extractLinks(Document doc) {
		ArrayList<String> extractedUrls = new ArrayList<String>();
		NodeList links=doc.getElementsByTagName("link"); //look for link tags
		for(int i=0; i<links.getLength();  i++) {
			NamedNodeMap attributes=links.item(i).getAttributes();
			Node href=attributes.getNamedItem("href"); //extract absolute or relative url from href tag
			Node rel=attributes.getNamedItem("rel");
			if(!href.getNodeValue().startsWith("http")) { //for absolute URLS, for the full URL of the new resource
				String protocol = "http://";
				if(isHttps)
					protocol = "https://";
				//String subUrl = protocol+getDomain(url)+href.getNodeValue();
				String subUrl = null;
				if(url.endsWith(".html")||url.endsWith(".htm")) {
					subUrl = protocol+getDomain(url)+"/"+href.getNodeValue();
				} else {
					if(url.endsWith("/")) 
						subUrl = url+href.getNodeValue(); //append href's value to the end
					else
						subUrl = url+"/"+href.getNodeValue(); //append href's value to the end
				}
				extractedUrls.add(subUrl);
			} else if( href.getNodeValue().contains(":") || href.getNodeValue().contains("#") || href.getNodeValue().toLowerCase().contains("javascript")) {
				System.out.println("Removing possibility of matching non link cases");
				continue;
			} else {
				extractedUrls.add(href.getNodeValue());
			}
		}
		NodeList a=doc.getElementsByTagName("a"); //above steps for the <a> tag as well
		for(int i=0; i<a.getLength();  i++) {
			NamedNodeMap attributes=a.item(i).getAttributes();
			Node href=attributes.getNamedItem("href");
			Node rel=attributes.getNamedItem("rel");
			if(!href.getNodeValue().startsWith("http")) {
				String protocol = "http://";
				if(isHttps)
					protocol = "https://";
				String toAdd = "/"+href.getNodeValue();
				if(href.getNodeValue().startsWith("/"))
					toAdd = href.getNodeValue();
				//String subUrl = protocol+getDomain(url)+href.getNodeValue();
				String subUrl = null;
				if(url.endsWith(".html")||url.endsWith(".htm")) {
					subUrl = protocol+getDomain(url)+"/"+href.getNodeValue();
				} else {
					if(url.endsWith("/")) 
						subUrl = url+href.getNodeValue();
					else
						subUrl = url+"/"+href.getNodeValue();
				}
				extractedUrls.add(subUrl);
			} else if( href.getNodeValue().contains(":") || href.getNodeValue().contains("#") || href.getNodeValue().toLowerCase().contains("javascript")) { //href values can contain other things apart from URLs
				System.out.println("Removing possibility of matching non link cases");
				continue;
			} else { //for href values with absolute URL
				extractedUrls.add(href.getNodeValue());
			}
		}
		return extractedUrls;
	}
	/*
	 *Method to determine if crawling is permitted given domain specific rules
	 */
	private boolean isCrawlingAllowed(DomainRules rulesForDomain) {
		if(rulesForDomain==null)
			return false;
		ArrayList<String> disallowedLinks = null;
		disallowedLinks = rulesForDomain.getRobotsTxtInfo().getDisallowedLinks("cis455crawler"); //Get rules for cis455crawler first
		if(disallowedLinks == null)
			disallowedLinks = rulesForDomain.getRobotsTxtInfo().getDisallowedLinks("*"); //If matches to crawler can't be found, get rules for all
		if(disallowedLinks==null|| disallowedLinks.size()==0) {
			System.out.println("Coudn't get disallowed links for *");
			return false;
		}
		if(disallowedLinks.get(0).equalsIgnoreCase("/")) {
			System.out.println("All crawlers banned");
			return false;
		}
		for(String str: disallowedLinks) {
			if(str.contains("*"))
				break;
			else {
				if(url.contains(str)) {
					System.out.println("Url matched disallowed pattern: "+str);
					return false;
				}
			}
		}
		
		//Testing time
		//System.out.println("Domain TIME: "+rulesForDomain.getNextAccessTime());
		if(!newResource) { //for an already encoutered domain
			Date date = new Date();
			Date then = rulesForDomain.getNextAccessTime(); //get last accessed time
			if(date.before(then)) {
				URLQueue instance  = URLQueue.getInstance();
				instance.add(url); //Add URL back to queue, Time limits on the domain not expired
				return false;
			}
		}
		return true;
	}
	/*
	 * Method to fetch and parse robots.txt file
	 */
	public RobotsTxtInfo fetchAndParseRobots() {
		String protocol = "http://";
		isHttps  = false;
		if(url.startsWith("https")) {
			protocol = "https://";
			isHttps = true;
		}
		String robotsUrl = protocol+getDomain(url)+"/robots.txt";
		ResourceManagement resourceManagement = new ResourceManagement(robotsUrl, isHttps);
		resourceManagement.addRequestHeader("User-Agent", "cis455crawler");
		resourceManagement.addRequestHeader("Connection", "close");
		resourceManagement.makeGetRequest();
		if(resourceManagement.getResponseStatus()!=200)
			return null;
		RobotsTxtInfo robotsTxtInfo = new RobotsTxtInfo();
		InputStream robotsStream = resourceManagement.makeGetRequest();
		BufferedReader br = new BufferedReader(new InputStreamReader(robotsStream));
		String line = null;
		String presentUserAgent = null;
		try { //parse through robots.txt
			while((line = br.readLine())!=null) {
				if(line.contains(":")) {
					String[] elements = line.split(":");
					String key = elements[0].trim();
					String value = "";
					if(elements.length>1)
						value = elements[1].trim();
					if(key.equalsIgnoreCase("Allow"))
						robotsTxtInfo.addAllowedLink(presentUserAgent, value);
					else if(key.equalsIgnoreCase("Disallow"))
						robotsTxtInfo.addDisallowedLink(presentUserAgent, value);
					else if(key.equalsIgnoreCase("User-agent")) {
						presentUserAgent = value;
						robotsTxtInfo.addUserAgent(presentUserAgent);
					} else if(key.equalsIgnoreCase("Crawl-delay"))
						robotsTxtInfo.addCrawlDelay(presentUserAgent, Integer.parseInt(value));
					else if(key.equalsIgnoreCase("Sitemap"))
						robotsTxtInfo.addSitemapLink(value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return robotsTxtInfo;
	}
	/*
	 * Method to obtain a DOM object given 
	 */
	public Document parseDOM(InputStream is) throws ParserConfigurationException, SAXException, IOException {
		Document document = null;
		if(resourceManagement.isHtml) { //html
			Tidy tidy=new Tidy();
			tidy.setXHTML(true);
			tidy.setTidyMark(false);
			tidy.setShowWarnings(false);
			tidy.setQuiet(true);
			document = tidy.parseDOM(is, null); //Jtidy used to parse HTML
		} else { //xml
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		    DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
		    document = documentBuilder.parse(is); 
		}
	    return document;
	}
}
