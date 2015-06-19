package edu.upenn.cis455.mapreduce;

import java.util.List;

public interface Job {

  void map(String key, List<String> value, Context context);
  
  void reduce(String key, String values[], Context context);
  
}
