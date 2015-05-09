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

	public Ranker() {

	}

	// Sort the hashmap and get the max frequency value
	public void map(String key, List<String> value, Context context) {
		
		double divFactor = value.size() - 1;
		
		if(divFactor == 0) {
			return;
		}
		
		String intermediateRank = "" + Double.parseDouble(value.get(0)) / divFactor;
		String original = "";
		String val = "";
		for (int i = 1; i < value.size(); i++) {
			val = intermediateRank + " " + key;
//			System.out.println("Ranker Map writing key: "+value.get(i)+" value: "+val);
			context.write(value.get(i), val);
			original = original + " " + value.get(i);

		}
//		System.out.println("Ranker Map writing: key: "+key+" value: "+original.trim());
		context.write(key, original.trim());

	}

	public void reduce(String key, String[] values, Context context) {

//		System.out.print("Ranker Reduce input key: "+key+"  ");
//		for(String str: values) {
//			System.out.print(" value: "+str);
//		}
//		System.out.println();
		
		double sum = 0;
		String outLinks = "";
		for (int i = 0; i < values.length; i++) {
			String[] fields = values[i].split(" ");
			//if(fields[0].matches("[+-]?\\d*(\\.\\d+)?")){
			if(fields[0].contains(".")) {
				sum = sum + Float.parseFloat(fields[0]);
			}
			else{
				if(outLinks.isEmpty()) 
					outLinks += values[i].trim();
				else 
					outLinks += " "+values[i].trim();
//				//for(String links:values){
//					outLinks +=  values[i];
//				//}
			}
		}
		
		double finalRank = (1 - 0.85) + 0.85 * sum;
		String output = ""+finalRank+" "+outLinks.trim();
//		System.out.println("Reduce output: key: "+key+" value: "+finalRank+" "+outLinks.trim());
		
		if(output.contains("nfinity")) {
			System.out.print("Ranker Reduce input key: "+key+"  ");
			for(String str: values) {
				System.out.print(" value: "+str);
			}
			System.out.println();
		}
		
		context.write(key, output);
	}

}
