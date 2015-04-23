package com.datformers.mapreduce.worker.resources;

/*
 * Class representing a data structure to store key-value pairs for Map phase
 */
public class KeyValueInput {
	private String key;
	private String value;
	public KeyValueInput(String key, String value) {
		this.key = key;
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}
