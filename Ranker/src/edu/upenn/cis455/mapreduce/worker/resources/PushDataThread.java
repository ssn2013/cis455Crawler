package edu.upenn.cis455.mapreduce.worker.resources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


import edu.upenn.cis455.mapreduce.util.HttpClient;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

/*
 * Class represeting the thread handling pushdata calls
 */
public class PushDataThread implements Runnable{

	private FileManagement fileManagement;
	
	private List<String> threadIPString = new ArrayList<String>();
	private WorkerServlet parent;
	private String spoolOutDir;
	
	public PushDataThread(List<String> threadIps, WorkerServlet parent, FileManagement parentObj) {
		this.threadIPString = threadIps;
		this.parent = parent;
		this.fileManagement = parentObj;
	}
	
	@Override
	public void run() {
//		System.out.println("PushDataThread:run: start");
		int index = 0;
		URL url;
		try {
			for(String ipAddrStr: threadIPString) {
				int counter = 0;
				String urlString = "http://"+ipAddrStr+"/worker/pushdata";
				url = new URL(urlString);
				fileManagement.setSpoolOutFileReaderForWorker(index);
				String dataToSent = null;
				while((dataToSent = fileManagement.getSpoolOutChunkForWorker())!=null){
//					System.out.println("Chunked Requesting FROM FILE: "+index+" Making Request: "+urlString);
					HttpClient client = new HttpClient();
					client.makePostRequest(urlString, Integer.parseInt(ipAddrStr.split(":")[1]), "text/plain", dataToSent);
					int successStory = client.getResponseCode();
					counter++;
				}
				System.out.println("PushDataThread:run: Made push request to: "+ipAddrStr+" number of times: "+counter);
				index++;
			}	
//			System.out.println("PushDataThread:run: updating Parent");
			parent.updateStatusWaiting();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("EXCEPTION PushDataThread:run: "+e.getMessage());
		}
	}


}
