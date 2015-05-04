package com.datformers.crawler.resources;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/*
 * Class implementing the Queue of URLs
 */
public class URLQueue {
	private Queue<String> urlQueue = new LinkedList<String>();
	private static URLQueue queue = null;
	
	private URLQueue() {
		
	}
	public int size() {
		return urlQueue.size();
	}
	public static URLQueue getInstance() { //singleton instance of queue
		if(queue == null) {
			queue = new URLQueue();
		}
		return queue;
	}
	
	/*
	 * Method to enqueue URL
	 */
	public synchronized void add(String str) {
		urlQueue.add(str);
		notify();
	}
	/*
	 * Method to dequeue URL
	 */
	public synchronized String getUrl() throws InterruptedException {
		while(urlQueue.isEmpty()) {
			wait();
		}
		return urlQueue.remove();
	}
	public ArrayList<String> getQueue() {
		return new ArrayList<String>(queue.urlQueue);
	}
	/*
	 * Method to determine if the queue is empty
	 */
	public synchronized boolean isEmpty() {
		return urlQueue.isEmpty();
	}
	public void clear() {
		urlQueue.clear();
	}
}
