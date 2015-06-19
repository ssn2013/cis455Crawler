package FrontEnd;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HttpsURLConnection;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;




public class topSearch extends HttpServlet{

	public static BigInteger[] range;
	public static HashMap<String, Integer> stopWords = new HashMap<String, Integer>();
	public String workersFile=null;

	
	public String doSpellCheck(String query) {
		try {
			query=URLEncoder.encode(query, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		HttpsURLConnection urlConn;
		URL mUrl;
		try {
			String hostname="https://montanaflynn-spellcheck.p.mashape.com/check/?text="+query;
			//mUrl = new URL("https://"+hostname+"/check/?text="+query);
			mUrl=new URL(hostname);
			//System.out.println(mUrl.toString());
			urlConn = (HttpsURLConnection) mUrl.openConnection();
			urlConn.setConnectTimeout(2000);
			urlConn.setRequestProperty("X-Mashape-Key", "d1yeX0CPEemsh4ZoJKqrHmsgkEn6p15LvyOjsn31nIMSKh6ZDt");
			urlConn.setRequestProperty("Accept","application/json");

			urlConn.connect();

			InputStream connectionInputStream = urlConn.getInputStream(); //get input stream

			if(connectionInputStream==null)
				System.out.println("CONNECTION INPUT STREAM NULL");

			//receiving response
			BufferedReader reader = new BufferedReader(new InputStreamReader(connectionInputStream));
			String line = null;
			StringBuilder buffer = new StringBuilder();
			while ((line = reader.readLine()) != null) {
				buffer.append(line + "\n");
			}
			String body = buffer.toString();
			reader.close();
			//parsing JSON object
			JSONObject obj;
			obj = new JSONObject(body);
			return obj.getString("suggestion").toLowerCase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{	
		
		try {
			InputStream body = request.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(body));
	        StringBuilder out = new StringBuilder();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            out.append(line);
	        }
	        String result = out.toString();
	       // System.out.println("OUTPUT -- "+result);
	       
	        String[] res = result.split("\\$");
	        result = "";
	        for(int i=0;i<res.length;i++){
	        	result+=res[i].split("#")[0]+"$";
	        }
	        result = result.substring(0, result.length()-1);
	        //System.out.println("RESULT -- "+result);
	        reader.close();
	        response.setContentType("text/plain");
	        PrintWriter resp = response.getWriter();
	        resp.write(result);
	        resp.close();
	        
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
	
		stopWords.put("a", 1);
		stopWords.put("an", 1);
		stopWords.put("and", 1);
		stopWords.put("are", 1);
		stopWords.put("as", 1);
		stopWords.put("at", 1);
		stopWords.put("be", 1);
		stopWords.put("by", 1);
		stopWords.put("for", 1);
		stopWords.put("from", 1);
		stopWords.put("has", 1);
		stopWords.put("he", 1);
		stopWords.put("in", 1);
		stopWords.put("is", 1);
		stopWords.put("it", 1);
		stopWords.put("its", 1);
		stopWords.put("of", 1);
		stopWords.put("on", 1);
		stopWords.put("that", 1);
		stopWords.put("the", 1);
		stopWords.put("to", 1);
		stopWords.put("was", 1);
		stopWords.put("were", 1);
		stopWords.put("will", 1);
		stopWords.put("with", 1);
		
		System.out.println("search:Starting top search");
		HashMap<String, Integer> topURLs = new HashMap<String, Integer>();
		HashMap<String, Double> URLRank = new HashMap<String, Double>();
		String query = request.getParameter("query");
		query = query.toLowerCase();
		String spellCheck = doSpellCheck(query);
		System.out.println("search:Spell check done!!");
		String wrongInput = "false";
		if(spellCheck!=null){
			if(!query.equals(spellCheck)){
				query = spellCheck;
				wrongInput = "true";
			}
		}
		query = query.toLowerCase();
		HashMap<String, ArrayList<String>> workers = new HashMap<String, ArrayList<String>>();
		ArrayList<String> workersList = new ArrayList<String>();
		
		if(workersFile==null) {
			workersFile = getInitParameter("indexWorkersFile"); //fetch details of master
		}
		
		try {
			BufferedReader br = new BufferedReader(
					new InputStreamReader(
							new FileInputStream(
									workersFile),
							"Cp1252"));
			String line;
			while ((line = br.readLine()) != null) {
				workers.put(line, new ArrayList<String>());
				workersList.add(line);
			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	
		

		// REMOVE STOP WORDS
		String[] queryArr = query.split(" ");
		List<String> searchList = new ArrayList<String>();
		for (String token : queryArr) {
			if (!stopWords.containsKey(token)) {
				searchList.add(token);
			}
		}
		
		
//		// GENERATE WORKER AND THE WORDS HE HAS TO HANDLE
//		for (int i = 0; i < searchList.size(); i++) {
//			BigInteger keyHash;
//			try {
//				keyHash = SHA1(searchList.get(i));
//				//for (int j = 0; j < range.length; j++) {
//				//	if (keyHash.compareTo(range[j]) < 1) {
//						ArrayList<String> temp = workers.get(workersList.get(Integer.parseInt(keyHash.mod(new BigInteger(""+workersList.size())).toString())));
//						temp.add(searchList.get(i));
//						workers.put(workersList.get(Integer.parseInt(keyHash.mod(new BigInteger(""+workersList.size())).toString())), temp);
//						//break;
//					//}
//				//}
//			} catch (NoSuchAlgorithmException e) {
//				e.printStackTrace();
//			} catch (UnsupportedEncodingException e) {
//				e.printStackTrace();
//			}
//			
//		}
		for(int i=0;i<searchList.size();i++){
			if((int)searchList.get(i).charAt(0) >= 97 && (int)searchList.get(i).charAt(0) <= 102){
				ArrayList<String> temp = workers.get(workersList.get(0));
				temp.add(searchList.get(i));
				workers.put(workersList.get(0), temp);	
			}else if((int)searchList.get(i).charAt(0) >= 103 && (int)searchList.get(i).charAt(0) <= 108){
				ArrayList<String> temp = workers.get(workersList.get(1));
				temp.add(searchList.get(i));
				workers.put(workersList.get(1), temp);	
			}else if((int)searchList.get(i).charAt(0) >= 109 && (int)searchList.get(i).charAt(0) <= 115){
				ArrayList<String> temp = workers.get(workersList.get(2));
				temp.add(searchList.get(i));
				workers.put(workersList.get(2), temp);	
			}else if((int)searchList.get(i).charAt(0) >= 116 && (int)searchList.get(i).charAt(0) <= 122){
				ArrayList<String> temp = workers.get(workersList.get(3));
				temp.add(searchList.get(i));
				workers.put(workersList.get(3), temp);	
			}else{
				ArrayList<String> temp = workers.get(workersList.get(0));
				temp.add(searchList.get(i));
				workers.put(workersList.get(0), temp);	
			}
		}
		
		
		
		// REMOVE ALL THE WORKERS WHO DONT HAVE ANYTHING TO HANDLE
		HashMap<String, ArrayList<String>> toSearchWorkers = new HashMap<String, ArrayList<String>>();
		Iterator it = workers.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pair = (Map.Entry) it.next(); 
			ArrayList<String> temp = (ArrayList<String>) pair.getValue();
			if(!temp.isEmpty()){
				toSearchWorkers.put((String)pair.getKey(), temp);
			}
		}
		//System.out.println(toSearchWorkers.toString());
		/*
		 *  TO CHANGE
		 */
		
		JSONObject result = new JSONObject();
		JSONArray mainJSONArray = new JSONArray();
		System.out.println("search:Sending to extract!!");
		it = toSearchWorkers.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry worker = (Map.Entry) it.next(); 
			
			// SEND HTTP REQUEST TO THE CORRESPONDING WORKER AND GET RESULTS
			String words = "";
			ArrayList<String> temp = (ArrayList<String>) worker.getValue();
			for(int i=0;i< temp.size();i++){
				words+=temp.get(i)+"$";
			}
			words = words.substring(0, words.length()-1);
			
			HttpURLConnection connection = null;  
			  try {
			    String url = "http://"+worker.getKey().toString()+"/extract/ext?words="+words;
			    URL request_url = new URL(url);
			    connection= (HttpURLConnection)request_url.openConnection();
			    
			    connection.setConnectTimeout(100000);
			    connection.setReadTimeout(100000);
			     
			    InputStream is = connection.getInputStream();
			    BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			    StringBuilder response1 = new StringBuilder();  
			    String line;
			    while((line = rd.readLine()) != null) {
			      response1.append(line);
			    }
			    rd.close();
			    JSONObject intermediate = new JSONObject(response1.toString());
			    
			    
			    JSONArray getArray = intermediate.getJSONArray("search");
			    for(int i = 0; i < getArray.length(); i++)
			    {
			          JSONObject object = getArray.getJSONObject(i);
			          mainJSONArray.put(object);
			    }
			    
			    
			  } catch (Exception e) {
			    e.printStackTrace();
			  } finally {
			    if(connection != null) {
			      connection.disconnect(); 
			    }
			  }
			
		}
		try {
			System.out.println("search:ready to send for page rank");
			result.put("search_results", mainJSONArray);
			//System.out.println(result.toString());
			
			// GETTING THE TOP URLs
//			JSONArray getArray = result.getJSONArray("search_results");
//		    for(int i = 0; i < getArray.length(); i++)
//		    {
//		          JSONObject object = getArray.getJSONObject(i);
//		          String url = object.get("docID").toString();
//		          if(topURLs.containsKey(url)){
//		        	  int count = topURLs.get(url);
//		        	  topURLs.put(url, count+1);
//		          }else{
//		        	  topURLs.put(url, 1);
//		          }
//		    }
//			LinkedHashMap<String, Integer> topSorted = sortHash(topURLs, false);
//			it = topSorted.entrySet().iterator();
//			String pageRankList = "";
//			int limit = 0;
//			while(it.hasNext()){
//				if(limit == 100){
//					break;
//				}
//				limit++;
//				Map.Entry pair = (Map.Entry) it.next();
//				pageRankList+=pair.getKey().toString()+"$";
//			}
//			System.out.println("INPUT TO PAGE RANK -- "+pageRankList);
//			
			JSONArray res = result .getJSONArray("search_results");

			if(res.length() == 0){
				response.setContentType("text/plain");
			    PrintWriter out = response.getWriter();
			    response.setStatus(200);
			    out.write("Sorry$"+wrongInput+"$"+query);
			    out.close();
				
				
			    return;
			}

			for(int i=0;i<res.length();i++){
				JSONObject obj  = res.getJSONObject(i);
				topURLs.put(obj.getString("docID"), Integer.parseInt(obj.getString("count")));
				URLRank.put(obj.getString("docID"), Double.parseDouble(obj.getString("rank")));
			}
			
			LinkedHashMap<String, Integer> topSorted = sortHash(topURLs, false);
			it = topSorted.entrySet().iterator();
			String pageRankList = "";
			int limit = 0;
			while(it.hasNext()){
				if(limit == 100){
					break;
				}
				limit++;
				Map.Entry pair = (Map.Entry) it.next();
				pageRankList+=pair.getKey().toString()+"#"+URLRank.get(pair.getKey())+"$";
			}
			
			pageRankList = pageRankList.substring(0, pageRankList.length()-1);
			// SEND THE PAGE RANK LIST TO PAGE RANKER AND GET THE RESPONSE
			HttpURLConnection urlConn;
			//URL mUrl = new URL("http://158.130.104.153:8080/extract/ext");
			//URL mUrl = new URL("http://158.130.104.153:8080/pagerank/fetchranks");
/* main thing */ //URL mUrl = new URL("http://52.7.79.117:8080/pagerankmaster/fetchranks");
			URL mUrl = new URL("http://127.0.0.1:8080/search/search");
			urlConn = (HttpURLConnection) mUrl.openConnection();
			urlConn.addRequestProperty("Content-Type", "text/plain" + "POST");
			urlConn.setRequestProperty("Content-Length", Integer.toString(pageRankList.length()));
			urlConn.setDoOutput(true);
			urlConn.getOutputStream().write(pageRankList.getBytes("UTF8"));
			
			
			InputStream is = urlConn.getInputStream();
		    BufferedReader rd = new BufferedReader(new InputStreamReader(is));
		    StringBuilder response1 = new StringBuilder();  
		    String line;
		    while((line = rd.readLine()) != null) {
		      response1.append(line);
		    }
		    rd.close();
			//
		    String finalResult[] = response1.toString().split("\\$");
		    //System.out.println("OUTPUT FROM PAGE RANK -- "+response1.toString());
		    System.out.println("search:done with page rank printing results");
		    // FINAL RESULTS
		    JSONObject displayResult = new JSONObject();
		    JSONArray ja = new JSONArray();
		    
		    JSONArray display = result.getJSONArray("search_results");
		    for(int i=0;i<finalResult.length;i++){
		    	for(int j = 0; j < display.length(); j++)
		    	{
		    		JSONObject object = display.getJSONObject(j);  
		    		if(object.get("docID").toString().equals(finalResult[i])){
		    			//System.out.println(object.get("docID")+"--"+object.get("url")+"--"+object.get("snippet"));
		    			JSONObject res1 = new JSONObject();
		    			res1.put("url", object.get("url"));
		    			res1.put("snippet", object.get("snippet"));
		    			res1.put("title", object.get("title"));
		    			ja.put(res1);
		    			break;
		    		}
		    		
		    	}
		    }
		    displayResult.put("result", ja);
		    displayResult.put("spellCheck", wrongInput);
		    displayResult.put("corrected", query);
		    response.setContentType("text/plain");
		    PrintWriter out = response.getWriter();
		    out.write(displayResult.toString());
		    out.close();
			
			
			
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		

	}

	
	/////////// FUNCTIONS 
	
	
	public static BigInteger SHA1(String text) throws NoSuchAlgorithmException,
			UnsupportedEncodingException {
		MessageDigest md;
		md = MessageDigest.getInstance("SHA-1");
		byte[] sha1hash = new byte[40];
		md.update(text.getBytes("iso-8859-1"), 0, text.length());
		sha1hash = md.digest();

		return convertToBigInt(sha1hash);
	}

	// Function to convert the Hash to Big Integer
	private static BigInteger convertToBigInt(byte[] data) {
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
	
	public static LinkedHashMap<String, Integer> sortHash(Map<String, Integer> unsortMap, final boolean order) {
		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(
				unsortMap.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
	
	
}
