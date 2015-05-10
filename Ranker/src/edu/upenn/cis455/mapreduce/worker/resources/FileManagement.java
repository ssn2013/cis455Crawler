package edu.upenn.cis455.mapreduce.worker.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.datformers.storage.DBIndexerWrapper;
import com.datformers.storage.ParsedDocument;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.PrimaryIndex;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

/*
 * Class handles all file operations
 */
public class FileManagement {
	private String storageDir;
	private String spoolOutDir;
	private File spoolOutFile;
	private String spoolInDirName;
	private File spoolInDir;
	private int numWorkers;
	private List<File> spoolOutFilesPointers = new ArrayList<File>();
	private List<PrintWriter> printWritersSpoolOut = new ArrayList<PrintWriter>();
	private int spoolInCounter = -1;
	private String listFiles[];
	public  boolean readComplete = false;
	private String databaseIO;
	
	//Stuff for handling input
	boolean inputFromDb = false;
	boolean outputToDb = false;
	BufferedReader inputReader = null;
	DBIndexerWrapper wrapper = null;
	EntityCursor<ParsedDocument> cursor = null;
	PrimaryIndex<BigInteger, ParsedDocument> indexDoc = null;
	Iterator<ParsedDocument> inputIterator = null;
	Transaction txn = null;
	
	public FileManagement() {
	}
	
	public FileManagement(String storageDir) {
		this.storageDir = storageDir;
	}

	public FileManagement(String storageDir, String inputDir, int noWorkers, String databaseIO) {
		this.databaseIO = databaseIO;
		this.storageDir = storageDir;
		spoolOutDir = storageDir + "/spoolOut";
		spoolInDirName = storageDir + "/spoolIn";
		numWorkers = noWorkers;

		String inputName = this.storageDir+"/"+inputDir;
		//prepare input
		if(databaseIO!=null && databaseIO.equals("input")) {
			wrapper = new DBIndexerWrapper(inputName);
			wrapper.configure();
			wrapper.loadIndices();
			txn = wrapper.myEnv.beginTransaction(null, null);
			indexDoc = wrapper.getDocumentIndex();
			cursor = indexDoc.entities(txn, null);
			inputIterator = cursor.iterator();
			inputFromDb = true;
			
		} else if(databaseIO!=null && databaseIO.equals("output")) {
			outputToDb = true;
		} else {
			try {
				inputReader = new BufferedReader(new FileReader(new File(inputName)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		// create spool directory
		spoolOutFile = new File(spoolOutDir);
		if (spoolOutFile.exists()) {
			removeDirectoryWithContents(spoolOutFile);
		}
		// create output files in spoolOut
		spoolOutFile.mkdir();
		for (int i = 0; i < numWorkers; i++) {
			File f = new File(spoolOutDir, "worker" + i);
			if (!f.exists()) {
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
				e.printStackTrace();
			}
		}

		// setting up spool in
		spoolInDir = new File(spoolInDirName);
		if (spoolInCounter == -1 && spoolInDir.exists()) {
			removeDirectoryWithContents(spoolInDir);
		}
		spoolInDir.mkdir();
		
	}

	/*
	 * Method removes a directory and all files inside
	 */
	private void removeDirectoryWithContents(File f) {
		if (!f.isDirectory())
			return;
		String[] listFiles = f.list(); // list of all files
		if (listFiles.length == 0)
			return;
		for (int i = 0; i < listFiles.length; i++) {
			File entry = new File(f.getPath(), listFiles[i]);
			if (entry.isDirectory()) { // if file entry is directory call same
										// method
				removeDirectoryWithContents(entry);
			} else {
				entry.delete(); // else delete file
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
			byte[] converted = md.digest(); // get hash
			BigInteger big = new BigInteger(1, converted); // get biginter
															// represetation of
															// value
			int groupId = Math.abs(big.intValue() % numWorkers); // get file for
																	// worker

			// write to corresponding file
			printWritersSpoolOut.get(groupId).println(key + "\t" + value);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Fetch a single key value pair for map requests This method is called by
	 * threads to fetch input for map
	 */
	public synchronized MapperInput getOutlinks() throws IOException {
		// Put check to return null when no more DB entries
		
		if(readComplete == false){
			MapperInput keyValueInput = null;
			if(inputFromDb) {
				if (inputIterator.hasNext()) {

					//firsttime through DB
					ParsedDocument docs = inputIterator.next();
					BigInteger docId = docs.getDocID();
					ArrayList<String> outLinks = docs.getExtractedUrls();

					keyValueInput = new MapperInput(String.valueOf(docId), outLinks);
					return keyValueInput;
				}else{
					readComplete = true;
					return null;
				}
			} else { //read from files
				String line = inputReader.readLine();
//				System.out.println("FM read input line; "+line);
				if(line==null) {
					readComplete = true;
					return null;
				} else {
					String keyValue[] = line.split("\t");
					String[] values = keyValue[1].split(" ");
					List<String> outLinks = new ArrayList<String>(Arrays.asList(values));
					keyValueInput = new MapperInput(keyValue[0], outLinks);
					return keyValueInput;
				}
			}
		}else{
			return null;
		}

	}

	/*
	 * Method to close all spool out files pointers, called at the end of map
	 * phase
	 */
	public void closeAllSpoolOut() {
		for (PrintWriter pw : printWritersSpoolOut) {
			if (pw != null)
				pw.close();
		}
		
		//closing DB
		if(inputFromDb) {
			if(cursor!=null) {
				cursor.close();
				cursor = null;
				txn.commit();
				txn = null;
			}
			if(wrapper!=null)
				wrapper.exit();
		}
	}

	/*
	 * MEthod to fetch contents of a specified spool out file
	 */
	
	BufferedReader spoolOutReaderForWorker = null;
	StringBuffer spoolOutChunkForWorker = new StringBuffer();
	double PUSH_MAX_ALLOWED_SIZE = (1.9*1024*1024)-3;
	//double PUSH_MAX_ALLOWED_SIZE = (1.9*1024)-3;
	public void setSpoolOutFileReaderForWorker(int index) {
		String fileName = spoolOutDir + "/worker" + index;
		try {
			spoolOutReaderForWorker = new BufferedReader(new FileReader(new File(fileName)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	public String getSpoolOutChunkForWorker() {
		String line = null;
		try {
			while((line=spoolOutReaderForWorker.readLine())!=null) {
				String toSend = spoolOutChunkForWorker.toString();
				if((toSend.getBytes().length+line.getBytes().length)>PUSH_MAX_ALLOWED_SIZE) {
					spoolOutChunkForWorker = new StringBuffer(line+'\n');
					return toSend;
				}
				spoolOutChunkForWorker.append(line+'\n');
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(spoolOutChunkForWorker.length()>0) {
			String toSend = spoolOutChunkForWorker.toString();
			spoolOutChunkForWorker = new StringBuffer();
			return toSend;
		} else {
			return null;
		}
	}

	/*
	 * Method to write to a spool in file
	 */
	public void writeToSpoolIn(BufferedReader br) {
		spoolInCounter++; // counter to keep track of which file is being
							// written
		String name = "temp" + spoolInCounter;
		File f = new File(spoolInDirName, name);
		if (!f.exists()) // create file if not existing
			try {
				f.createNewFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		String line = null;
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(f));
			while ((line = br.readLine()) != null) {
				pw.println(line);
			}
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Code entirely dedicated to reduce specific tasks
	private BufferedReader sortResultReader = null;
	private String outputFileName;
	private File outputFilePointer;
	private File reduceOutputFile;
	private PrintWriter reduceOutputWriter;

	/*
	 * Method to handle sorting etc, called by WorkerServlet right before reduce
	 * phase starts
	 */
	public void setModeReduce(String outputDirName) throws InterruptedException {
		spoolInDir = new File(spoolInDirName);
		listFiles = spoolInDir.list();
		if (listFiles.length == 0) {
			return;
		}
		StringBuffer commandCreateBuffer = new StringBuffer("sort"); // create
																		// the
																		// command
																		// to
																		// sort
		for (String file : listFiles) {
			commandCreateBuffer.append(" " + spoolInDirName + "/" + file);
		}
		String command = commandCreateBuffer.toString();
		try {
			List<String> cm = new ArrayList<String>();
			cm.add("sh");
			cm.add("-c");
			cm.add("cat /dev/null > "+spoolInDirName+"/sorted");

			ProcessBuilder pp = new ProcessBuilder(cm);
			try {
				Process p = pp.start();
				p.waitFor();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
//			for(String file: listFiles){
//				List<String> cmd = new ArrayList<String>();
//				cmd.add("sh");
//				cmd.add("-c");
//				cmd.add("sort "+spoolInDirName + "/" + file+ ">> "+spoolInDirName+"/sorted");
//
//				ProcessBuilder pb = new ProcessBuilder(cmd);
//				try {
//					Process p = pb.start();
//					p.waitFor();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
			
			List<String> cmd = new ArrayList<String>();
			cmd.add("sh");
			cmd.add("-c");
			cmd.add(command+ " > "+spoolInDirName+"/sorted");

			ProcessBuilder pb = new ProcessBuilder(cmd);
			try {
				Process p = pb.start();
				p.waitFor();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			//sortProcess.waitFor(); // wait for completion
			File temp = new File(spoolInDirName+"/sorted");
			sortResultReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(spoolInDirName+"/sorted"))));
			this.outputFileName = storageDir+"/"+outputDirName;
			outputFilePointer = new File(this.outputFileName);
			if (outputFilePointer.exists()) {
				outputFilePointer.delete();
				outputFilePointer = new File(this.outputFileName);
//				removeDirectoryWithContents(outputFilePointer);
			}
//			outputDirFile.mkdir();
//			reduceOutputFile = new File(outputDirFile, "output");
//			reduceOutputFile.createNewFile();
			outputFilePointer.createNewFile();
			reduceOutputWriter = new PrintWriter(new FileWriter(outputFilePointer)); // attach a
																	// reader to
																	// output
																	// folder
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Method to return key and values[] pair for reduce. Method called by
	 * threads
	 */
	private String previousReduceLine = ""; // each time the next line is read
											// to see if the same key is
											// repeated (to check if another
											// associated value exists)

	public KeyValuesInput getReduceLine() {
		KeyValuesInput keyValuesInput = null;
		if (previousReduceLine == null)
			return null;
		if (!previousReduceLine.isEmpty()) {
			String parts[] = previousReduceLine.split("\t");
			keyValuesInput = new KeyValuesInput(parts[0].trim(),
					parts[1].trim()); // initialize key value pair
		}
		try {
			String line = null;
			while (true) {
				line = sortResultReader.readLine(); // read line
				if (line == null) {
					previousReduceLine = null; // when there's no more input to
												// read
					break;
				}
				if (keyValuesInput == null) { // in the first case where
												// previousReduceLine won't have
								
					// the previous line
					String parts[] = line.split("\t");
					keyValuesInput = new KeyValuesInput(parts[0].trim(),
							parts[1].trim());
					continue;
				}
				previousReduceLine = line;
				if (!line.startsWith(keyValuesInput.getKey())) // for finding
																// key value
																// lines
																// corresponding
																// to the same
																// key
					break; // if not then we're done reading

				// finally the part where we append the given value (if mating
				// last considered key), this is done to get an array of values
				String parts[] = line.split("\t");
				keyValuesInput.addValue(parts[1].trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return keyValuesInput;
	}

	/*
	 * Method to write output of reduce phase, called by threads.
	 */
	public synchronized void writeToOutput(String key, Object value) {
		
//		System.out.println("OUTPUT WRITING BY: "+Thread.currentThread().getName()+" Writing: "+key+"\t"+(String)value);
		reduceOutputWriter.println(key+"\t"+(String)value);
	}
	
	/*
	 * MEthod to close the pointer to output of reduce phase. It is called by
	 * WorkerServlet when all threads are done writing output.
	 */
	public void closeReduceWriter() {
		reduceOutputWriter.close();
	}
	
	
	BufferedReader fileReaderForSendingToMaster = null;
	StringBuffer bufferForSendingToMaster = new StringBuffer();
	//double MAX_ALLOWED_LENGTH = (1.9*1024*1024)-3;
	double MAX_ALLOWED_LENGTH = (1.9*1024)-3;
	public void setToMasterReader(String file) {
		try {
			fileReaderForSendingToMaster = new BufferedReader(new FileReader(new File(storageDir+"/"+file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	public String getDataFromFile() {
		String line = null;
		if(fileReaderForSendingToMaster==null)
			System.out.println("EMPTY READER");
		try {
			while((line = fileReaderForSendingToMaster.readLine())!=null) {
				String toSend = bufferForSendingToMaster.toString();
				if((toSend.getBytes().length + line.getBytes().length)>MAX_ALLOWED_LENGTH) {
					bufferForSendingToMaster = new StringBuffer(line+'\n');
					//System.out.println("FM: Writing; "+toSend);
					return toSend;
				}
				bufferForSendingToMaster.append(line+'\n');
			}
			if(bufferForSendingToMaster.length()==0) {
				return null;
			} else {
				String toSend = bufferForSendingToMaster.toString();
				bufferForSendingToMaster = new StringBuffer();
				return toSend;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
	}
}
