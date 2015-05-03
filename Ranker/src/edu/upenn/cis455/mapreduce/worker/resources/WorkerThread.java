package edu.upenn.cis455.mapreduce.worker.resources;

import java.io.IOException;
import java.util.List;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;
import edu.upenn.cis455.mapreduce.worker.resources.FileManagement;

/*
 * Class representing threads used for map and reduce. Class is also the Context used by Jobs
 */
public class WorkerThread implements Context, Runnable{

	private Job job;
	private String className;
	private FileManagement fileManagement;
	private WorkerServlet parent;
	private boolean mapMode = true;

	public WorkerThread(String classJob, FileManagement fileManagement, WorkerServlet parent, boolean mapMode) {
		this.className = classJob;
		this.fileManagement = fileManagement;
		this.parent = parent;
		this.mapMode = mapMode;
	}

	/*public static synchronized void close(){
		System.out.println(WorkerServlet.cursorClosed);
		if(WorkerServlet.cursorClosed == false){
			WorkerServlet.setCursorClose();
			WorkerServlet.closeCursor();
			
		}
	}
	
	public static synchronized void closeOutputDB(){
		System.out.println("Closing Output DB");
		if(WorkerServlet.outputDBClosed == false){
			//WorkerServlet.setCursorClose();
			WorkerServlet.closeOutputDB();
			
		}
	}*/
	@Override
	public void run() {
		System.out.println("RUN METHOD");
		Class jobClass = null;
		try {
			//instantiate a class
			jobClass = Class.forName(className);
			job = (Job)jobClass.newInstance();

			//continuously fetch line and feed to map or reduce till you get a null
			KeyValueInput keyValueInput = null;
			KeyValuesInput keyValuesInput = null;
		
			if(mapMode) {
				while((keyValueInput = fileManagement.getOutlinks())!=null) {
					System.out.println("Thread: "+Thread.currentThread().getName()+" MAP Key: "+keyValueInput.getKey()+" Value: "+keyValueInput.getValue());
					parent.updateKeysRead(1); //update count of keys read
					List<String> content = keyValueInput.getValue();
					job.map(keyValueInput.getKey(), content, this);
				}
				//close();
				
			} else {
				while((keyValuesInput = fileManagement.getReduceLine())!=null) {
					System.out.println("Thread: "+Thread.currentThread().getName()+" MAP Key: "+keyValuesInput.getKey()+" Reduce values");
					parent.updateKeysRead(1); //update count of keys read
					job.reduce(keyValuesInput.getKey(), keyValuesInput.getValues(), this);
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}
				//closeOutputDB();
			}


			//change state and call it quits
			parent.updateCompletion();

		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException  e) {
			e.printStackTrace();
			//parent.wrapper.exit();
			System.out.println("ERROR IN WORKER THREAD: "+e.getMessage());
		}
	}

	/*
	 * Write method called by job
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */
	@Override
	public void write(String key, Object value) {
		if(mapMode) { //Write goes to different files depending on map or reduce phase
			//System.out.println("WorkerThread:write called with Key: "+key+" Value: "+value);
			parent.updateKeyWritten(1);
			//Write to spool out
			fileManagement.writeToSpoolOut(key, (String)value);
		} else {
			//System.out.println("WorkerThread:write called with Key: "+key+" Value: "+value);
			parent.updateKeyWritten(1);
			//Write to output 
			fileManagement.writeToOutput(key, value);
		}
	}


}
