package fetch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import storage.OutputDBWrapper;
import storage.WordIndexEntity;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.PrimaryIndex;


public class Ext extends HttpServlet{
	public BigInteger hashRange[];
	public String workers[];
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String queryString;
	public String indexDb;
	public String crawlerWorkers;
	public PrimaryIndex<String, WordIndexEntity> pi; 
	public OutputDBWrapper wrapper;
	
	@Override
	public void init() {
		indexDb = getInitParameter("indexDb"); 
		crawlerWorkers = getInitParameter("crawlerWorkers");
		wrapper = new OutputDBWrapper(indexDb);
		wrapper.configure();
		pi = wrapper.indexKey;
	}
	
	@Override
	public void destroy() {
		wrapper.exit();
	}
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response){
		System.out.println("extract:starting extract");
		if(request.getServletPath().contains("ex")){
				

			HashMap<BigInteger,Integer> intermatchedDocIds = new HashMap<BigInteger,Integer>(); 
			HashMap<BigInteger,Double> interrankDocIds = new HashMap<BigInteger,Double>();
			
			
			HashMap<BigInteger,Integer> matchedDocIds = new HashMap<BigInteger,Integer>(); 
			HashMap<BigInteger,Double> rankDocIds = new HashMap<BigInteger,Double>();
			
			// NEW CODE
			HashMap<String,ArrayList<BigInteger>> fullList = new HashMap<String,ArrayList<BigInteger>>();
			HashMap<String,ArrayList<Double>> fullListRanks = new HashMap<String,ArrayList<Double>>();
			
			HashSet<BigInteger> intersection = new HashSet<BigInteger>();
			//
			
			String[] query = request.getParameter("words").split("\\$");
			
			// NEW CODE
			for(int i=0;i<query.length;i++){
				fullList.put(query[i],new ArrayList<BigInteger>());
			}
			//
			
			queryString="";
			for(String word:query) {
				if(queryString.equals("")) queryString=word;
				else queryString+=";;;;;"+word;
			}
			HashSet<String> wordSet = new HashSet<String>();
			for(int i=0;i<query.length;i++){
				wordSet.add(query[i]);
			}
			
			System.out.println("extract:reading from index db");
			response.setContentType("text/plain");
			EntityCursor<WordIndexEntity> pi_cursor = pi.entities();
			
			Iterator<WordIndexEntity> docIterator = pi_cursor.iterator();
			
			while(docIterator.hasNext()){
				WordIndexEntity index = docIterator.next();
				
				if(wordSet.contains(index.getWord())){
					ArrayList<BigInteger> temp = (ArrayList<BigInteger>) index.getDocIds();
					ArrayList<Double> ranks = (ArrayList<Double>) index.getRank();
					
					// NEW CODE
					fullList.put(index.getWord(), temp);
					fullListRanks.put(index.getWord(), ranks);
					//
				}
			}
			ArrayList<BigInteger> fullCombined = new ArrayList<BigInteger>();
			ArrayList<Double> fullCombinedRank = new ArrayList<Double>();
			
			Iterator it = fullList.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        fullCombined.addAll((ArrayList<BigInteger>)pair.getValue());
		        fullCombinedRank.addAll(fullListRanks.get((String)pair.getKey()));
		    }
			
			ArrayList<BigInteger> inter = new ArrayList<BigInteger>();
			it = fullList.entrySet().iterator();
			int skip = 0;
			while(it.hasNext()){
				Map.Entry pair = (Map.Entry)it.next();
				if(skip == 0){
					inter = (ArrayList<BigInteger>)pair.getValue();
					skip ++;
				}else{
					inter.retainAll((ArrayList<BigInteger>)pair.getValue());
				}
			}
			
			HashSet interSet = new HashSet<BigInteger>(inter);
			
			for(int i=0;i<fullCombined.size();i++){
				if(intermatchedDocIds.containsKey(fullCombined.get(i))) {
					if(interSet.contains(fullCombined.get(i))){
						intermatchedDocIds.put(fullCombined.get(i),intermatchedDocIds.get(fullCombined.get(i))+100);
					}else{
						intermatchedDocIds.put(fullCombined.get(i),intermatchedDocIds.get(fullCombined.get(i))+1);
					}	
				}
				else{
					if(interSet.contains(fullCombined.get(i))){
						intermatchedDocIds.put(fullCombined.get(i),100);
					}else{
						intermatchedDocIds.put(fullCombined.get(i),1);
					}	
				}
				interrankDocIds.put(fullCombined.get(i),fullCombinedRank.get(i));
				
			}
			
			LinkedHashMap<BigInteger, Integer> topSorted = sortHash(intermatchedDocIds, false);
			it = topSorted.entrySet().iterator();
			int limit =0;
			while(it.hasNext()){
				if(limit == 100)
					break;
				limit ++;
				Map.Entry pair = (Map.Entry) it.next();
				matchedDocIds.put((BigInteger)pair.getKey(), (Integer)pair.getValue());
				rankDocIds.put((BigInteger)pair.getKey(),interrankDocIds.get((BigInteger)pair.getKey()));
			}
			
			pi_cursor.close();
			System.out.println("NEW CODE");
			System.out.println("extract:read from index db");
			Set<BigInteger> listofIds = matchedDocIds.keySet();
			
			// THIS INFO HAS TO COME FROM CRAWLER DB
			
			JSONObject mainObj = new JSONObject();
			JSONArray ja = new JSONArray();
			
			doHashDiv();
			
			HashMap<Integer, String> docs=new HashMap<Integer,String>();
			for(int k=0;k<workers.length;k++) {
				docs.put(k, new String("ids="));
			}
			
			//fetching contents for each doc id
			try {
				for(BigInteger b:listofIds) { 
					
					int index=findIndex(b);
					String params=docs.get(index);
					if(params.equals("ids=")) {
						params+=b.toString();
					}
					else params+=";"+b.toString();
					docs.put(index,params);
					}
				
				for(int k=0;k<workers.length;k++) {
					if(docs.get(k).equals("ids=")) continue;
				HashMap<BigInteger,HashMap<String,String>> results=fetchDocId(workers[k],docs.get(k));
				
				for(BigInteger b:listofIds) {
					
				if(!results.containsKey(b)) continue;	
				JSONObject jo = new JSONObject();
								
				jo.put("docID", b.toString());
				jo.put("url", results.get(b).get("url"));
				jo.put("title", results.get(b).get("title"));
				jo.put("snippet", results.get(b).get("snippet"));
				jo.put("count", matchedDocIds.get(b).toString());
				jo.put("rank", ""+rankDocIds.get(b));
				ja.put(jo);
					
				}
				}
				mainObj.put("search", ja);
			} catch (JSONException e1) {
				e1.printStackTrace();
			}
							
			
			response.setContentType("text/plain");
			try {
				PrintWriter output = response.getWriter();
				output.write(mainObj.toString());
				output.flush();
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public HashMap<BigInteger,HashMap<String,String>> fetchDocId(String hostname,String params) {
		
		HttpURLConnection urlConn;
		URL mUrl;
		HashMap<BigInteger,HashMap<String,String>> retVal=new HashMap<BigInteger,HashMap<String,String>>();
		try {
			System.out.println("extract:sent request to fetch docs");
			mUrl = new URL("http://"+hostname+"/extract/fetchdocs?"+params+"&query="+queryString);
			urlConn = (HttpURLConnection) mUrl.openConnection();
			urlConn.setConnectTimeout(100000);
		    urlConn.setReadTimeout(100000);
			urlConn.setRequestMethod("GET");
			urlConn.setDoOutput(false);
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
			JSONArray results = obj.getJSONArray("results");
			
		    for (int i = 0 ; i < results.length(); i++) {
		        JSONObject item = results.getJSONObject(i);
		        BigInteger id=new BigInteger(item.getString("docID"));
		        HashMap<String,String> otherValues=new HashMap<String,String>();
		        otherValues.put("url",item.getString("url"));
		        otherValues.put("title",item.getString("title"));
		        otherValues.put("snippet",item.getString("snippet")); 
		        retVal.put(id, otherValues);
		    }
		    System.out.println("extract:send fetch results to ext");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		return retVal;	
	}
	
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response){
		try {
			InputStream body = request.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(body));
	        StringBuilder out = new StringBuilder();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            out.append(line);
	        }
	        String result = out.toString();
	        //System.out.println("OUTPUT -- "+result);
	       
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
	public void doHashDiv() {
		if(hashRange!=null) return;
		File f=new File(crawlerWorkers);
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
			System.out.println("Problem file does not exist!!");
		}
		
		hashRange=new BigInteger[workers.length];
		BigInteger range = new BigInteger(
				"ffffffffffffffffffffffffffffffffffffffff", 16)
				.divide(new BigInteger("" + workers.length));
		hashRange[0] = range;
		for (int i = 1; i < workers.length; i++) {

			BigInteger temp = new BigInteger("" + hashRange[i - 1]).add(range);

			hashRange[i] = temp;

		}

	}
//	

	public int findIndex(BigInteger key) {
		
		//System.out.println("key:"+key+"="+b);
		int index = 0;
		for (int i = 0; i < hashRange.length; i++) {
			if (key.compareTo(hashRange[i]) <= 0) {
				index = i;
				break;
			}
		}
		return index;
	}
	
	public static LinkedHashMap<BigInteger, Integer> sortHash(Map<BigInteger, Integer> unsortMap, final boolean order) {
		List<Entry<BigInteger, Integer>> list = new LinkedList<Entry<BigInteger, Integer>>(
				unsortMap.entrySet());
		Collections.sort(list, new Comparator<Entry<BigInteger, Integer>>() {
			public int compare(Entry<BigInteger, Integer> o1,
					Entry<BigInteger, Integer> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		LinkedHashMap<BigInteger, Integer> sortedMap = new LinkedHashMap<BigInteger, Integer>();
		for (Entry<BigInteger, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
}
