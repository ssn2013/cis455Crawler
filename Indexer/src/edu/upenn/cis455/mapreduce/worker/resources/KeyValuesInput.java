package edu.upenn.cis455.mapreduce.worker.resources;

import java.util.ArrayList;

/*
 * Class representing key values[] pair given as input to reduce phase
 */
public class KeyValuesInput {
	private String key;
	private ArrayList<String> value = new ArrayList<String>(); //values[] initially stores as ArrayList for convenience of adding values
	public KeyValuesInput() {
		
	}
	public KeyValuesInput(String key, String value) {
		this.key = key;
		this.value.add(value);
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String[] getValues() { //conversion from ArrayList to String[] is done 
		String values[] = new String[value.size()];
		for(int i=0; i<value.size(); i++)
			values[i] = value.get(i);
		return values;
	}
	public void addValue(String value) {
		this.value.add(value);
	}
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer("KEY: "+key+" :-");
		for(int i=0; i<value.size(); i++)
			buf.append(" "+value.get(i));
		return buf.toString();
	}
}
