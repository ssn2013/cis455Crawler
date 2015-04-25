package com.datformers.mapreduce.worker.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/*
 * Class handles all file operations
 */
public class FileManagement {
	private String storageDir;
	private String inputDir;
	private String spoolOutDir;
	private File spoolOutFile;
	private File inputDirFile;
	private String spoolInDirName;
	private File spoolInDir;
	private int numWorkers;
	private List<File> spoolOutFilesPointers = new ArrayList<File>();
	private List<PrintWriter> printWritersSpoolOut = new ArrayList<PrintWriter>();
	private int spoolInCounter = -1;
	private String listFiles[];
	private int currentFileIndex = 0;
	private File currentFile;
	private BufferedReader bufferedReader = null;

	public FileManagement() {
		
	}

	public FileManagement(String storageDir, String inputDir, int noWorkers) {
		this.storageDir = storageDir;
		this.inputDir = storageDir+"/"+inputDir;
		spoolOutDir = storageDir+"/spoolOut";
		spoolInDirName = storageDir+"/spoolIn";
		numWorkers = noWorkers;
		
		//create spool directory
		spoolOutFile = new File(spoolOutDir);
		if(spoolOutFile.exists()) {
			removeDirectoryWithContents(spoolOutFile);
			System.out.println("Successfully removed existing Spool-Out directory");
		}
		//create output files in spoolOut
		spoolOutFile.mkdir();
		for(int i=0; i<numWorkers; i++) {
			File f = new File(spoolOutDir, "worker"+i);
			if(!f.exists()) {
				try {
					f.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			spoolOutFilesPointers.add(f);
			try {
				printWritersSpoolOut.add(new PrintWriter(new FileWriter(f)));
			} catch (IOException e) {
				System.out.println("FILEMANAGEMENT EXCEPTION: "+e.getMessage());
				e.printStackTrace();
			}
		}
		
		//open input directory
		if(inputDir==null) {
		inputDirFile = new File(this.inputDir);
		}
		//setting up spool in 
		spoolInDir = new File(spoolInDirName);
		if(spoolInCounter==-1 && spoolInDir.exists()) {
			removeDirectoryWithContents(spoolInDir);
		}
		spoolInDir.mkdir();
	}
	
	/*
	 * Method removes a directory and all files inside
	 */
	private void removeDirectoryWithContents(File f) {
		if(!f.isDirectory())
			return;
		String[] listFiles = f.list(); //list of all files
		if(listFiles.length==0)
			return;
		for(int i=0; i<listFiles.length; i++) {
			File entry = new File(f.getPath(),listFiles[i]);
			if(entry.isDirectory()) { // if file entry is directory call same method
				removeDirectoryWithContents(entry);
			} else {
				entry.delete(); //else delete file
			}
		}
		f.delete();	
	}
	
	/*
	 * Synchronized method for writing to spool out 
	 */
	public synchronized void writeToSpoolOut(String key, String value) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(key.getBytes());
			byte[] converted = md.digest(); //get hash
			BigInteger big = new BigInteger(1,converted); //get biginter represetation of value
			int groupId = Math.abs(big.intValue()%numWorkers); //get file for worker
			
			//write to corresponding file
			printWritersSpoolOut.get(groupId).println(key+"\t"+value);
			//System.out.println("FileManagement:writeToSpoolOut: Writing done for KEY: "+key+" INTO: "+groupId+" WITH: "+printWritersSpoolOut.get(groupId));
		} catch (NoSuchAlgorithmException  e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Fetch a single key value pair for map requests
	 * This method is called by threads to fetch input for map
	 */
	public synchronized KeyValueInput getLine() throws IOException {
		KeyValueInput keyValueInput = null;
		
		//If first time reading
		if(listFiles==null || listFiles.length==0) {
			listFiles = inputDirFile.list(); //get list of files from input directory
			if(listFiles==null || listFiles.length==0)
				System.out.println("Listed files not present in: "+inputDirFile.getAbsolutePath());
			currentFileIndex = 0;
			bufferedReader = new BufferedReader(new FileReader(this.inputDir+"/"+listFiles[currentFileIndex]));
		}
		
		//Key value input
		String line = bufferedReader.readLine();
		while(line == null) { //in case of end of file attempt to open and read from the next file
			if(currentFileIndex == listFiles.length-1) 
				return null; //if no more files to read (i.e dont reading input) return null
			bufferedReader = new BufferedReader(new FileReader(this.inputDir+"/"+listFiles[++currentFileIndex]));
			line = bufferedReader.readLine();
		}
		String values[] = line.split("\t"); //for the line read split to key and value
		keyValueInput = new KeyValueInput(values[0].trim(), values[1].trim());		
		
		return keyValueInput;
	}
	
	/*
	 * Method to close all spool out files pointers, called at the end of map phase
	 */
	public void closeAllSpoolOut() {
		for(PrintWriter pw: printWritersSpoolOut){
			if(pw!=null)
				pw.close();
		}
	}
	
	/*
	 * MEthod to fetch contents of a specified spool out file
	 */
	public String getSpoolOutFileContentForWorker(int index) {
		try {
			String fileName = spoolOutDir+"/worker"+index;
			BufferedReader bf = new BufferedReader(new FileReader(new File(fileName)));
			StringBuffer buf = new StringBuffer();
			String line;
			while((line = bf.readLine())!=null) //read contents line by line and append to a string
				buf.append(line+'\n');
			return buf.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/*
	 * Method to write to a spool in file 
	 */
	public void writeToSpoolIn(BufferedReader br) {
		System.out.println("In FileManagement:writeToSpoolIn: start");
		spoolInCounter++;	//counter to keep track of which file is being written
		String name = "temp"+spoolInCounter;
		File f = new File(spoolInDirName, name);
		if(!f.exists()) //create file if not existing
			try {
				f.createNewFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		String line = null;
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(f));
			while((line = br.readLine())!=null) {
				pw.println(line);
			}
			pw.close();
			System.out.println("In FileManagement:writeToSpoolIn: successfully wrote to: "+name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Code entirely dedicated to reduce specific tasks
	private BufferedReader sortResultReader = null;
	private String outputDirName;
	private File outputDirFile;
	private File reduceOutputFile;
	private PrintWriter reduceOutputWriter;
	
	/*
	 * Method to handle sorting etc, called by WorkerServlet right before reduce phase starts
	 */
	public void setModeReduce(String outputDirName) {
		spoolInDir = new File(spoolInDirName);
		listFiles = spoolInDir.list();
		if(listFiles.length==0) {
			System.out.println("Got nothing in spool in");
			return;
		}
		StringBuffer commandCreateBuffer = new StringBuffer("sort"); //create the command to sort
		for(String file: listFiles) {
			commandCreateBuffer.append(" "+spoolInDirName+"/"+file);
		}
		String command = commandCreateBuffer.toString();
		try {
			Process sortProcess = Runtime.getRuntime().exec(command); //execute
			sortProcess.waitFor(); //wait for completion
			sortResultReader = new BufferedReader(new InputStreamReader(sortProcess.getInputStream())); //get inputstream object
			
			//create output folder
			this.outputDirName = storageDir+"/"+outputDirName;
			outputDirFile = new File(this.outputDirName);
			if(outputDirFile.exists()) {
				removeDirectoryWithContents(outputDirFile);
			}
			outputDirFile.mkdir();
			reduceOutputFile = new File(outputDirFile, "output");
			reduceOutputFile.createNewFile();
			reduceOutputWriter = new PrintWriter(reduceOutputFile); //attach a reader to output folder
		} catch (IOException | InterruptedException e) {
			System.out.println("FileManagement: Exception in settng spool in mode: "+e.getMessage());
			e.printStackTrace();
		}
	}
	
	/*
	 * Method to return key and values[] pair for reduce. Method called by threads
	 */
	private String previousReduceLine = ""; //each time the next line is read to see if the same key is repeated (to check if another associated value exists)
	public KeyValuesInput getReduceLine() {
		KeyValuesInput keyValuesInput = null;
		if(previousReduceLine==null)
			return null;
		if(!previousReduceLine.isEmpty()) {
			String parts[] = previousReduceLine.split("\t");
			keyValuesInput = new KeyValuesInput(parts[0].trim(), parts[1].trim()); //initialize key value pair
		}
		try {
			String line = null;
			while(true) {
				line = sortResultReader.readLine(); //read line
				if(line == null) {
					previousReduceLine = null; //when there's no more input to read
					break;
				}
				if(keyValuesInput == null) { //in the first case where previousReduceLine won't have the previous line
					String parts[] = line.split("\t");
					keyValuesInput = new KeyValuesInput(parts[0].trim(), parts[1].trim());
					continue;
				}
				previousReduceLine = line;
				if(!line.startsWith(keyValuesInput.getKey())) //for finding key	value lines corresponding to the same key
					break; //if not then we're done reading
				
				//finally the part where we append the given value (if mating last considered key), this is done to get an array of values
				String parts[] = line.split("\t");
				keyValuesInput.addValue(parts[1].trim()); 
			}
		} catch (IOException e) {
			System.out.println("EXCEPTION FileManagement:getReduceLine: "+e.getMessage());
		}
		return keyValuesInput;
	}

	/*
	 * Method to write output of reduce phase, called by threads.
	 */
	public synchronized void writeToOutput(String key, String value) {
		if(reduceOutputFile==null || printWritersSpoolOut ==null){
			return;
		}
		reduceOutputWriter.println(key+"\t"+value);
	}
	
	/*
	 * Sample main used for testing, irrelevant here.
	 */
	public static void main(String args[]) throws Exception {
		FileManagement fl = new FileManagement("/home/cis455/storage",  "testInput", 3);
		fl.setModeReduce("outputDir");
		KeyValuesInput kvInput = null;
		while((kvInput=fl.getReduceLine())!=null) {
			System.out.println(kvInput);
		}
	}

	/*
	 * MEthod to close the pointer to output of reduce phase. It is called by WorkerServlet when all threads are done writing output.
	 */
	public void closeReduceWriter() {
		reduceOutputWriter.close();
	}
}
	
