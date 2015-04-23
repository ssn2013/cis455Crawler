package com.datformers.mapreduce.job;

import com.datformers.mapreduce.Context;
import com.datformers.mapreduce.Job;

/*
 * Word count job.
 */
public class WordCount implements Job {

	public void map(String key, String value, Context context)
	{
		String parts[] = value.split("\\s+");//split on white spaces to get word tokens
		for(int i=0; i<parts.length; i++) {
			context.write(parts[i], ""+1); //output token, 1 as key-value pair
		}
	}

	public void reduce(String key, String[] values, Context context)
	{
		int sum = 0;
		for(int i=0; i<values.length; i++) {
			sum += Integer.parseInt(values[i].trim()); //sum over all the counts
		}
		context.write(key, ""+sum);
	}

}
