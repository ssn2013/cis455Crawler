package com.datformers.mapreduce.worker.resources;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import com.datformers.crawler.resources.URLQueue;
import com.datformers.mapreduce.Job;
import com.datformers.mapreduce.worker.resources.FileManagement;
import com.datformers.resources.HttpClient;
import com.datformers.servlets.WorkerServlet;

/*
 * Class representing threads used for map and reduce. Class is also the Context used by Jobs
 */
public class WorkerThread implements  Runnable{

	private FileManagement fileManagement;
	private WorkerServlet parent;
	private ArrayList<String> queue = null;
	private String hostname=null;
	private String hostnameIp=null;

	public WorkerThread(ArrayList<String> queue, FileManagement fileManagement, WorkerServlet parent,String hostname, String hostnameIp) {
		this.queue=queue;
		//		for(String str: queue) {
		//			System.out.println("Queue to save contains: "+str);
		//		}
		this.fileManagement = fileManagement;
		this.parent = parent;
		this.hostname=hostname;
		this.hostnameIp=hostnameIp;
	}

	@Override
	public void run() {
		//System.out.println("Thread started running for writing to workers: "+Thread.currentThread().getName());
		PrintWriter out=null;
		URL hostUrl;
		HttpClient client = new HttpClient();
		File hostfile=new File(hostname);
		if(hostfile.exists()) {
			//System.out.println("Trying to delete old file: "+hostfile.delete());
			hostfile.delete();
			hostfile=new File(hostname);
		}
		try {
			hostfile.createNewFile();
			out = new PrintWriter(new BufferedWriter(
					new FileWriter(hostfile,
							true)));
		} catch (IOException e) {
			System.out.println("Exception occurred in fetching file name");
			e.printStackTrace();
		}


		String urlToSend="";
		for(String url:queue) {

			out.println(url);
			//send the file to the crawler
			if(hostnameIp!=null) {
				urlToSend+=url + "\n";
				if(urlToSend.getBytes().length>(1.5*1024*1024)) {
					//this packet will not be sent out!		
					//System.out.println("Byte count exceeded, making a call");
					String urlString = "http://"+hostnameIp+"/crawler/pushdata";
					try {
						//System.out.println("URL: "+urlString+" Data being sent: "+urlToSend);
						hostUrl = new URL(urlString);
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					client.makePostRequest(urlString, Integer.parseInt(hostname.split(":")[1]), "text/plain", urlToSend);
					//System.out.println("PushDataThread:run: Made push requet to: "+hostname);
					urlToSend="";
				}	
			}
		}
		if(hostnameIp!=null) {
			if(urlToSend.length()>0) {
				//System.out.println("Last chance to send");
				String urlString = "http://"+hostnameIp+"/crawler/pushdata";
				try {
					//System.out.println("URL: "+urlString+" Data being sent: "+urlToSend);
					hostUrl = new URL(urlString);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				client.makePostRequest(urlString, Integer.parseInt(hostname.split(":")[1]), "text/plain", urlToSend);
				//System.out.println("PushDataThread:run: Made push requet to: "+hostname);
				urlToSend="";
			}
		}

		//change state and call it quits
		out.close();
		URLQueue q=URLQueue.getInstance();
		q.clear();
		
			
		//System.out.println("Before writing to update completion: ");
		parent.updateCompletion();

	}



	/*
	 * Write method called by job
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */

	/*
	 * Main for testing purposes. Irrelevant here.
	 */
	public static void main(String args[]) throws Exception {
		String name="edu.upenn.cis455.mapreduce.job.DummyJob";
		Class jobClass = Class.forName(name);
		Job job = (Job)jobClass.newInstance();
		//System.out.println("GOT THIS FAR: ");

		jobClass = ClassLoader.getSystemClassLoader().loadClass(name);
		job = (Job) jobClass.newInstance();
		System.out.println("GOT THIS FAR AS WELL");
	}

}
