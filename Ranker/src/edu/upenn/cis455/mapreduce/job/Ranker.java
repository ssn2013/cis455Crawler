package edu.upenn.cis455.mapreduce.job;

import java.math.BigInteger;
import java.security.MessageDigest;
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
public class Ranker implements Job {

	public HashMap<String, Integer> stopWords = new HashMap<String, Integer>();
	int iterationNo = 0;

	public Ranker() {

		

	}

	// Sort the hashmap and get the max frequency value
	public void map(String key, List<String> value, Context context) {

		if (iterationNo == 0) {
			// Write into a file in the following format: D1 + "/t" + rank + " "
			// + List of Outlinks
			float rank = 1;
			String val = String.valueOf(rank);
			for (String items : value) {
				BigInteger hashedVal = SHA1(items);
				val = val + " " + String.valueOf(hashedVal);
			}
			context.write(key, val);
		} else {

			// Write into a file in the following format: D1 + "/t" + rank + " "
			// + List of Inlinks
			float divFactor = value.size() - 1;
			String val = "" + Float.parseFloat(value.get(0))/divFactor;
			String original = "";
			for (int i = 1; i < value.size(); i++) {	
				val = val + " " + key;
				original = original + value.get(i);
				context.write(value.get(i), val);
			}
			context.write(key, original);
			

		}

	}

	public void reduce(String key, String[] values, Context context) {
		// records are in given order docId,Tf,isTitle,isMeta,MaxFreq
		System.out.println("IN REDUCE FUNCTION");
		Map<String, Double> unsortMap = new HashMap<String, Double>();
		for (int i = 0; i < values.length; i++) {
			// System.out.println(key +"----"+values[i]);
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

		// context.write(key, sortedMapDes);
	}

	public static BigInteger convertToBigInt(byte[] data) {
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

	/*
	 * 
	 * This function is used to calculate the SHA1 hash
	 */
	public static BigInteger SHA1(String text) {
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("SHA-1");

			byte[] sha1hash = new byte[40];
			md.update(text.getBytes("iso-8859-1"), 0, text.length());
			sha1hash = md.digest();
			// String string=convertToHex(sha1hash);
			return convertToBigInt(sha1hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		// return new BigInteger(sha1hash);
	}

}
