package com.datformers.mapreduce.job;

import java.util.Arrays;

import com.datformers.mapreduce.Context;
import com.datformers.mapreduce.Job;

/*
 * A dummy job class created initially for testing purposes which just directly write whatever key value pairs it gets
 */
public class DummyJob implements Job {

	@Override
	public void map(String key, String value, Context context) {
		context.write(key, value); //write key-value
	}

	@Override
	public void reduce(String key, String[] values, Context context) {
		context.write(key, Arrays.toString(values));//write key and concatenation of all values
	}

}
