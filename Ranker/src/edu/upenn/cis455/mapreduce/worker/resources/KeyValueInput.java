package edu.upenn.cis455.mapreduce.worker.resources;

import java.util.List;

/*
 * Class representing a data structure to store key-value pairs for Map phase
 */
public class KeyValueInput {
	private String key;
	private List<String> value;
	public KeyValueInput(String key, List<String> value) {
		this.key = key;
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public List<String> getValue() {
		return value;
	}
	public void setValue(List<String> value) {
		this.value = value;
	}
}
