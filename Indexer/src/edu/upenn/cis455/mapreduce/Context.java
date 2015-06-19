package edu.upenn.cis455.mapreduce;

public interface Context {

  void write(String key, Object value);
  
}
