package edu.upenn.cis455.mapreduce.worker;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.ObjectInputStream.GetField;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.math.*;

import storage.WordIndexEntity;


class Reduce{
	public String key;
	public String[] value;
}

public class Reducer {
	public static int lineNumber = 0;
	public static void main(String args[]){
		Reduce res;
		while(true){
			res = getGroup();
			if(res == null){
				break;
			}
//			System.out.print(res.key + "---");
//			for(int i=0;i<res.value.length;i++){
//				System.out.print(res.value[i]+"----");
//			}
//			System.out.println();
			String[] values = res.value;
			Map<String, Double> unsortMap = new HashMap<String, Double>();
			for (int i = 0; i < values.length; i++) {
				//System.out.println(key +"----"+values[i]);
				String[] fields = values[i].split(" ");
				// listDocIds.add(Long.parseLong(fields[0]));
				double tf = 0.5 + (0.5 * Integer.parseInt(fields[1]) / Integer
						.parseInt(fields[4]));
				double tmp = (100000 / values.length);
				double idf = Math.log(tmp);
				// rank = 0.5 *tf-idf + 0.3 *isTitle + 0.2 *meta
				double rank = 0.5 * (tf * idf) + 0.3 * Integer.parseInt(fields[2])
						+ 0.2 * Integer.parseInt(fields[3]);
				unsortMap.put(fields[0], rank);

			}
			Map<String, Double> sortedMapDes = sortReduce(unsortMap, false);
			
			
			
			List<BigInteger> listDocIds = new ArrayList<BigInteger>();
			List<Double> ranks = new ArrayList<Double>();
			// Write to output
			Map<String, Double> sortedMapAsc = (Map<String, Double>) sortedMapDes;
			for (java.util.Map.Entry<String, Double> entry : sortedMapAsc
					.entrySet()) {
				ranks.add(entry.getValue());
				listDocIds.add(new BigInteger(entry.getKey()));

			}
			System.out.println(res.key+"--"+listDocIds+"--"+ranks);
			//System.out.println(key+"--"+listDocIds+"---"+ranks);
			//WordIndexEntity entity = new WordIndexEntity();
			//entity.setWord(key);
			//entity.setDocIds(listDocIds);
			//entity.setRank(ranks);
			//WorkerServlet.wrapperOutput.indexKey.put(entity);
			
		}
	}
	
	private static Map<String, Double> sortReduce(
			Map<String, Double> unsortMap, final boolean order) {

		List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(
				unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Entry<String, Double> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
	
	public synchronized static Reduce getGroup(){
		String key;
		FileInputStream fstream;
		BufferedReader br = null;
		
		try {
				fstream = new FileInputStream("/Users/Adi/Documents/workspace/Indexer/storage/spoolIn/sorted");	
				br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;
				for(int i=0;i<lineNumber;i++){
					strLine = br.readLine();
				}
				ArrayList<String> temp = new ArrayList<String>();
				String t = br.readLine();
				if(t==null){
					return null;
				}
				temp.add(t);
				key = temp.get(0).split("\t")[0];
				lineNumber++;
				while ((strLine = br.readLine()) != null){
						if(strLine.split("\t")[0].equals(key)){
							lineNumber++;
							temp.add(strLine);
						}else{
							break;
						}
				}
				Reduce obj = new Reduce();
				
				String[] val = new String[temp.size()];
				for(int i=0;i<temp.size();i++){
					val[i]=temp.get(i).split("\t")[1];
					
					
				}
				obj.key = key;
				obj.value = val;
				br.close();
				return obj;
	
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
}
