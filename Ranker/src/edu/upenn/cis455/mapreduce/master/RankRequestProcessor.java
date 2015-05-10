package edu.upenn.cis455.mapreduce.master;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.datformers.storage.DBIndexerWrapper;
import com.datformers.storage.DBRankerWrapper;
import com.datformers.storage.DocRanksStore;
import com.datformers.storage.ParsedDocument;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.PrimaryIndex;

public class RankRequestProcessor {
	
	private DBRankerWrapper wrapper = null;
	
	public RankRequestProcessor(DBRankerWrapper wrapperInput) {
		wrapper = wrapperInput;
	}

	public List<String> getRankedDocs(Map<String, String> request) {
		Set<String> docIds = request.keySet();
		Map<String, Double> pageRank = new HashMap<String, Double>();
		TreeMap<Double, LinkedList<String>> finalRank = new TreeMap<Double, LinkedList<String>>();
		
		PrimaryIndex<BigInteger,DocRanksStore> pageRankKey = wrapper.pageRankKey;
//		Transaction txn = null;
//		txn = wrapper.myEnv.beginTransaction(null, null);
		EntityCursor<DocRanksStore> cursor = pageRankKey.entities();
		Iterator<DocRanksStore> inputIterator = cursor.iterator();
		
		boolean first  = true;
		double max = 0;
		double average = 0;;
		int count = 0;
		while(inputIterator.hasNext()) {
			DocRanksStore doc = inputIterator.next();
			if(docIds.contains(""+doc.getDocId())) {
				pageRank.put(""+doc.getDocId(), doc.getRank());
				if(first) {
					average = doc.getRank();
					max = doc.getRank();
				} else {
					average += doc.getRank();
					if(max < doc.getRank())
						max = doc.getRank();
				}
			}
			count++;
		}
		average = average/count;
		average = (average+max)/2;
		
		cursor.close();
		
		//Calculation of page rank
		Set<String> pageRankedDocs = pageRank.keySet();
		for(String docId: request.keySet()) {
			System.out.print("Key: "+docId);
			double rank = Double.parseDouble(request.get(docId));
			if(pageRankedDocs.contains(docId)) { //calculate rank
				rank = rank * pageRank.get(docId);
				System.out.println(" PageRank: "+pageRank.get(docId)+" FinalRank: "+rank);
			} else {
				rank = rank * average;
				System.out.println(" Average: "+average+" FinalRank: "+rank);
			}
			
			//Add to treemap
			if(finalRank.containsKey(rank)) {
				finalRank.get(rank).add(docId);
			} else {
				LinkedList<String> temp = new LinkedList<String>();
				temp.add(docId);
				finalRank.put(rank, temp);
			}
		}
		
//		List<String> dummyReturns = new ArrayList<String>();
//		dummyReturns.add("a");
//		dummyReturns.add("b");
//		dummyReturns.add("c");
		
		List<String> resultSet = new ArrayList<String>();
		for(double finRank: finalRank.keySet()) {
			List<String> docs = finalRank.get(finRank);
			for(String str: docs) {
				resultSet.add(str);
			}
		}
		return resultSet;
	}

}
