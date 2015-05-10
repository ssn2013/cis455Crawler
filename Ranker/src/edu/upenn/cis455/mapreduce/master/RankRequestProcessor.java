package edu.upenn.cis455.mapreduce.master;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		for(String key: request.keySet()) 
			System.out.println("Key: "+key+" Value: "+request.get(key));
		Set<String> docIds = request.keySet();
		Map<String, Double> pageRank = new HashMap<String, Double>();
		
		PrimaryIndex<BigInteger,DocRanksStore> pageRankKey = wrapper.pageRankKey;
		Transaction txn = null;
		txn = wrapper.myEnv.beginTransaction(null, null);
		EntityCursor<DocRanksStore> cursor = pageRankKey.entities(txn, null);
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
		}
		average = average/count;
		
									
		
		List<String> dummyReturns = new ArrayList<String>();
		dummyReturns.add("a");
		dummyReturns.add("b");
		dummyReturns.add("c");
		return dummyReturns;
	}

}
