package edu.upenn.cis455.mapreduce.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;
import edu.upenn.cis455.mapreduce.worker.resources.FileManagement;

/*
 * Word count job.
 */
public class Indexer implements Job {

	public HashMap<String, Integer> stopWords = new HashMap<String, Integer>();

	public Indexer(){
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
	}
	// Sort the hashmap and get the max frequency value
	private int sortHash(Map<String, Integer> unsortMap, final boolean order) {
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

		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap.entrySet().iterator().next().getValue();
	}

	public void map(String key, List<String> value, Context context) {
		boolean isMeta = false;
		boolean isTitle = false;
		boolean isScript = false;
		boolean isStyle = false;
		Set<String> titleValues = new HashSet<String>();
		Set<String> metaValues = new HashSet<String>();

		for (int i = 0; i < value.size(); i++) {
			String cur = value.get(i);
			if (cur.contains("<title")) {
				isTitle = true;
				continue;
			}
			if (cur.contains("</title>")) {
				isTitle = false;
				continue;
			}
			if (isTitle == true) {
				titleValues.add(cur.toLowerCase());
			}
			if (cur.contains("<meta")) {
				if (cur.contains("name=\"Description\"")
						|| cur.contains("name=\"description\"")) {

					if (cur.contains("content=")) {
						String desc = cur.split("content=")[1];
						List<String> val = FileManagement.lemmatize(desc
								.toLowerCase());
						for (int j = 0; j < val.size(); j++) {
							metaValues.add(val.get(j));
						}
					} else if (cur.contains("Content=")) {
						String desc = cur.split("Content=")[1];
						List<String> val = FileManagement.lemmatize(desc
								.toLowerCase());
						for (int j = 0; j < val.size(); j++) {
							metaValues.add(val.get(j));
						}
					}
				}
				if (cur.contains("name=\"Keywords\"")
						|| cur.contains("name=\"keywords\"")) {
					if (cur.contains("content=")) {
						String desc = cur.split("content=")[1];
						desc = desc.replace(',', ' ');
						List<String> val = FileManagement.lemmatize(desc.toLowerCase());
						for (int j = 0; j < val.size(); j++) {
							metaValues.add(val.get(j));
						}
					} else if (cur.contains("Content=")) {
						String desc = cur.split("Content=")[1];
						desc = desc.replace(',', ' ');
						List<String> val = FileManagement.lemmatize(desc.toLowerCase());
						for (int j = 0; j < val.size(); j++) {
							metaValues.add(val.get(j));
						}
					}
				}
			}
		}

		HashMap<String, Integer> termFreq = new HashMap<String, Integer>();
		Set<String> unique = new HashSet<String>(value);
		for (String word : unique) {
			termFreq.put(word, Collections.frequency(value, word));
		}
		int maxFreq = sortHash(termFreq, false);

		for (int i = 0; i < value.size(); i++) {

			String current = value.get(i);

			if (current.contains("<script")) {
				isScript = true;
				continue;
			}
			if (current.contains("</script>")) {
				isScript = false;
				continue;
			}
			if (isScript == true) {
				continue;
			}
			
			// NEW CODE
			
			if(current.contains("<style")){
				isStyle = true;
				continue;
			}
			if(current.contains("</style>")){
				isStyle = true;
				continue;
			}
			if (isStyle == true){
				continue;
			}
			// END OF NEW CODE
			else if (current.startsWith("<") && current.endsWith(">")) {
				continue;
			} else if (Pattern.matches("\\p{Punct}", value.get(i))) {
				continue;
			} else if (stopWords.containsKey(value.get(i))) {
				continue;
			} else {
				if (unique.contains(current)) {
					if (titleValues.contains(current)
							&& metaValues.contains(current))
						context.write(current,
								key + " " + termFreq.get(current) + " " + "1"
										+ " " + "1" + " " + maxFreq);
					else if (titleValues.contains(current)) {
						context.write(current,
								key + " " + termFreq.get(current) + " " + "1"
										+ " " + "0" + " " + maxFreq);
					} else if (metaValues.contains(current)) {
						context.write(current,
								key + " " + termFreq.get(current) + " " + "0"
										+ " " + "1" + " " + maxFreq);
					} else {
						context.write(current,
								key + " " + termFreq.get(current) + " " + "0"
										+ " " + "0" + " " + maxFreq);
					}
					unique.remove(current);
				}
			}
		}
	}

	public void reduce(String key, String[] values, Context context) {
		// records are in given order docId,Tf,isTitle,isMeta,MaxFreq
		System.out.println("IN REDUCE FUNCTION");
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
		context.write(key, sortedMapDes);
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

}
