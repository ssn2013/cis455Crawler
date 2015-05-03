package com.datformers.crawler.resources;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datformers.crawler.XPathCrawler;



/*
 * Class implementing the HashMap of URLs
 */
public class OutgoingMap {
	private List<Set<String>> outgoingCrawlQueue;
	public BigInteger hashRange[];
	private static OutgoingMap queue = null;
	
	private OutgoingMap() {

	}
	public int size() {
		return outgoingCrawlQueue.size();
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
		queue=new OutgoingMap();
		queue.outgoingCrawlQueue= new ArrayList<Set<String>>(keys.length);
		
		for(int i=0;i<keys.length;i++) {
			
				if(keys[i].equals(XPathCrawler.selfAddress)) {
					XPathCrawler.selfIndex=i;
					
				}
			
			Set<String> tmp=new HashSet<String>();
			queue.outgoingCrawlQueue.add(tmp);
		}
		queue.hashRange=new BigInteger[keys.length];
		queue.doHashDiv(keys);
	}
	public static OutgoingMap getInstance() { 
		return queue;
	}
	public ArrayList<String> getQueueAtIndex(int index) { 
		
		return new ArrayList<String>(queue.outgoingCrawlQueue.get(index));
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
	 * Method to determine if the queue is empty
	 */
	public synchronized boolean isEmpty() {
		return outgoingCrawlQueue.isEmpty();
	}
}
