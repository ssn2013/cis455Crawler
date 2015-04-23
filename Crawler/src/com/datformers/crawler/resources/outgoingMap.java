package com.datformers.crawler.resources;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datformers.crawler.XPathCrawler;



/*
 * Class implementing the HashMap of URLs
 */
public class outgoingMap {
	//private HashMap<String,Set<String>> outgoingCrawlQueue= new HashMap<String,Set<String>>();
	private List<Set<String>> outgoingCrawlQueue;
	public BigInteger hashRange[];
	private static outgoingMap queue = null;
	
	private outgoingMap() {

	}
	public void doHashDiv(String []workers) {
		BigInteger range = new BigInteger(
				"ffffffffffffffffffffffffffffffffffffffff", 16)
				.divide(new BigInteger("" + workers.length));
		hashRange[0] = range;
		for (int i = 1; i < workers.length; i++) {

			BigInteger temp = new BigInteger("" + hashRange[i - 1]).add(range);

			hashRange[i] = temp;

		}

	}
	
	public static void createInstance(String []keys) {
		queue=new outgoingMap();
		queue.outgoingCrawlQueue= new ArrayList<Set<String>>(keys.length);
		for(int i=0;i<keys.length;i++) {
			try {
				if(keys[i].contains(InetAddress.getLocalHost().toString())) {
					XPathCrawler.selfIndex=i;
					continue;
				}
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Set<String> tmp=new HashSet<String>();
			queue.outgoingCrawlQueue.add(tmp);
		}
		queue.doHashDiv(keys);
	}
	public static outgoingMap getInstance() { 
		return queue;
	}
	/*
	 * Method to enqueue URL
	 */
	public void add(int key,String str) {
		Set<String> tmp=outgoingCrawlQueue.get(key);
		synchronized (tmp) {
			outgoingCrawlQueue.get(key).add(str);	
		}
		
		//notify();
	}
	/*
	 * Method to dequeue URL
	 */
//	public synchronized String getUrl(String key) throws InterruptedException {
//		while(urlQueue.isEmpty()) {
//			wait();
//		}
//		return urlQueue.remove();
//	}
	/*
	 * Method to determine if the queue is empty
	 */
	public synchronized boolean isEmpty() {
		return outgoingCrawlQueue.isEmpty();
	}
}
