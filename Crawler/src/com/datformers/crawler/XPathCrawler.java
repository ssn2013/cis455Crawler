package com.datformers.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datformers.crawler.info.RobotsTxtInfo;
import com.datformers.crawler.resources.DomainRules;
import com.datformers.crawler.resources.URLQueue;
import com.datformers.crawler.resources.OutgoingMap;
import com.datformers.servlets.WorkerServlet;
import com.datformers.storage.DBWrapper;
import com.datformers.storage.ParsedDocument;
import com.datformers.storage.VisitedURLStore;
import com.sleepycat.persist.PrimaryIndex;

/*
 * Main Crawler class
 */
public class XPathCrawler {

	public static String STARTING_URLS[] = null;
	public static String STORE_DIRECTORY = null;
	public static String CRAWLERS[] = null;
	public static int MAX_SIZE = -1;
	private static int MAX_PAGES = -1;
	private static int NO_OF_THREADS = 2;
	public HashMap<String, DomainRules> domainToRulesMapping = null;
	public static List<Thread> subThreads = new ArrayList<Thread>();
	private static XPathCrawler crawler = null;
	public static int count = 0;
	public static int totalURLCount = 0;
	public static int selfIndex = -1;
	public static boolean SYSTEM_SHUTDOWN = false;
	public static WorkerServlet ws = null;
	public static boolean STOP_CRAWLER = false;

	public static XPathCrawler getInstance() {
		return crawler;
	}

	/*
	 * Function to get the domain of a given URL
	 */
	public String getDomain(String url) {
		String host = url.substring(url.indexOf('/') + 2); // remove protocol
															// part
		if (host.contains("/"))
			host = host.substring(0, host.indexOf('/'));// remove any additional
														// paths
		return host;
	}

	public static void setWorkerServletOb(WorkerServlet w) {
		ws = w;
	}

	/*
	 * Function to get domain specific rules for a given URL
	 */
	public DomainRules getRulesForDomain(String domain) {
		return domainToRulesMapping.get(domain);
	}

	/*
	 * Function to add the parsed Robots.txt file to the domain specific rules
	 */
	public void setRobotsTxtInfo(String domain, RobotsTxtInfo robotsTxtInfo) {
		domainToRulesMapping
				.put(domain, new DomainRules(domain, robotsTxtInfo));
	}

	/*
	 * The task of the crawler
	 */
	public boolean getUrlFromFile() {
		System.out.println("reading from feerer");
		URLQueue queue = URLQueue.getInstance();
		String spoolIn = STORE_DIRECTORY + "/spoolIn";
		spoolIn = spoolIn.replace("//", "/");
		// get urls from spool in?
		File f = new File(spoolIn);
		if (!f.exists())
			return false;
		File[] files = f.listFiles();
		String line;
		if(files.length<1) return false;
		for (File file : files) {
			// check if file is the merged one!!
			if (file.getAbsolutePath().contains("merged"))
				continue;
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				while ((line = br.readLine()) != null) {
					queue.add(line);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return true;
	}

	public boolean executeTask() {
		domainToRulesMapping = new HashMap<String, DomainRules>();
		String delim = ";;|;;";
		URLQueue queue = URLQueue.getInstance(); // instance of the queue of
		XPathCrawlerThread.visitedURL.clear();// URLs
		String keys[] = CRAWLERS;
		OutgoingMap.createInstance(keys); // create a separate output set of
											// URLs for each worker
		// have to check if we have to resume operation or use seed urls
		if (STARTING_URLS != null) {
			DBWrapper d=new DBWrapper();
			d.truncateVisitedStore();
			for (String url : STARTING_URLS) {
				queue.add(url); // this crawler class just enqueues the first
								// URL and the threads handle the rest
			}
		} else
			if(!getUrlFromFile()) {
				System.out.println("No more data to crawl");
				return false;
			}
		return true;
	}

	public static void start(String args[], String urls[], String workers[]) {
		try {
			SYSTEM_SHUTDOWN = false;
			count = 0;
			STOP_CRAWLER = false;

			if (args.length < 3) {
//				System.out
//						.println("3 Arguments: 1. Starting URL, 2. DB directory, 3. Maximum size required 4.Num Pages 5.Crawlers");
				return;
			}
			STARTING_URLS = urls; // starting URL
			STORE_DIRECTORY = args[0]; // director of database environment
			CRAWLERS = workers;
			MAX_SIZE = Integer.parseInt(args[1]); // maximum allowed size for a
													// fetched file
			MAX_SIZE *= 1024 * 1024;
			if (args.length > 2)
				MAX_PAGES = Integer.parseInt(args[2]); // limit on maximum
														// documents to fetch
			crawler = new XPathCrawlerFactory().getCrawler();

			boolean shouldCrawl=crawler.executeTask(); // start the crawler task
			if(shouldCrawl) {
			for (int i = 0; i < NO_OF_THREADS; i++) { // create and executing
														// threads
				XPathCrawlerThread thread = new XPathCrawlerThread(crawler);
				Thread t = new Thread(thread);
//				System.out.println("ADDING THREAD: " + t.getName());
				crawler.addThread(t);
				t.start();
			}
			crawler.checkForClose(); // crawler then checks for the condition in
			} else {
				WorkerServlet.STATUS = "queue_emptied";	
			
			}							// for which it would stop
//			System.out.println("Store visited crawl data");
			saveVisitedToDB();
//			System.out.println("crawling ended");
			DBWrapper.commit();
			
			ws.updateStatusToMaster();
		} catch (Exception e) {
			DBWrapper.close();
			throw e;
		}
	}

	private boolean checkForClose() {
		boolean killTime = false;

		while (!killTime) {
			if (MAX_PAGES != -1 && count > MAX_PAGES) { // close if the number
														// of processed URLs
														// exceed the number of
														// files allowed

				killTime = true;
				break;
			}
			if (STOP_CRAWLER) {

				killTime = true;
				break;
			}
			/*
			 * This conditions checks for the case where the crawler is done i.e
			 * the queue is empty and all the threads are waiting on the queue
			 */
			if (URLQueue.getInstance().isEmpty() && count > 0) { // if queue is
																	// empty
																	// after
																	// some
																	// amount of
																	// processing
				boolean kill = true;
				for (Thread t : subThreads) {
					if (t.isAlive() && (t.getState() != Thread.State.WAITING)) { // if
																					// any
																					// thread
																					// is
																					// still
																					// working
																					// don't
																					// start
																					// shutdown
																					// process
						kill = false;
						break;
					}
				}
				if (kill == true) {
					killTime = true;
					break;
				}
			}
		}
		if (killTime = true) {
			// kill All threads or else wait for them to complete
			SYSTEM_SHUTDOWN = true; // state used by the threads to determine if
									// they should stop running

			for (Thread t : subThreads) {
				if (t.getState() == Thread.State.WAITING)
					t.interrupt(); // interrupt all waiting threads
				else
					try {
						t.join(); // in case any are still processing wait for
									// them
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		}

		return killTime;
	}

	public void addThread(Thread t) {
		subThreads.add(t);
	}

	public static synchronized void addCounter() {
		totalURLCount++;
		count++; // counter incremented by threads to keep track of files
					// processed
	}

	public static void saveVisitedToDB() {
		DBWrapper wrapper = new DBWrapper();
		PrimaryIndex<BigInteger, VisitedURLStore> indexDoc = wrapper.getStore()
				.getPrimaryIndex(BigInteger.class, VisitedURLStore.class);

		for (String url : XPathCrawlerThread.visitedURL) {
			VisitedURLStore v = new VisitedURLStore();
			BigInteger hashUrl = SHA1(url);
			v.setUrl(hashUrl);
			indexDoc.put(v);
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

}
