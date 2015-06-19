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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;







import com.datformers.storage.ParsedDocument;

import storage.WordIndexEntity;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

/*
 * Class handles all file operations
 */
public class FileManagement {
	private String storageDir;
	private String inputDir;
	private String spoolOutDir;
	private File spoolOutFile;
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
	public static boolean readComplete = false;
	
	
	private static StanfordCoreNLP pipeline;

	public FileManagement() {

	}

	public FileManagement(String storageDir, String inputDir, int noWorkers) {
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize,ssplit,pos,lemma");
		pipeline = new StanfordCoreNLP(props);

		this.storageDir = storageDir;
		this.inputDir = storageDir + "/" + inputDir;
		spoolOutDir = storageDir + "/spoolOut";
		spoolInDirName = storageDir + "/spoolIn";
		numWorkers = noWorkers;

		// create spool directory
		spoolOutFile = new File(spoolOutDir);
		if (spoolOutFile.exists()) {
			removeDirectoryWithContents(spoolOutFile);
			System.out
					.println("Successfully removed existing Spool-Out directory");
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
				System.out.println("FILEMANAGEMENT EXCEPTION: "
						+ e.getMessage());
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
	 * Function to lemmatize the given text
	 */
	public static List<String> lemmatize(String documentText) {
		List<String> lemmas = new LinkedList<String>();
		Annotation document = new Annotation(documentText);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {

			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				lemmas.add(token.get(LemmaAnnotation.class));
			}
		}

		return lemmas;
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
			// System.out.println("FileManagement:writeToSpoolOut: Writing done for KEY: "+key+" INTO: "+groupId+" WITH: "+printWritersSpoolOut.get(groupId));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Fetch a single key value pair for map requests This method is called by
	 * threads to fetch input for map
	 */
	public synchronized KeyValueInput getURLContent() throws IOException {
		// Put check to return null when no more DB entries
		if(readComplete == false){
			KeyValueInput keyValueInput = null;
			if (WorkerServlet.docIterator.hasNext()) {
				ParsedDocument docs = WorkerServlet.docIterator.next();
				String content = docs.getDocumentContents();
				BigInteger docId = docs.getDocID();

				List<String> lemmatized = lemmatize(content.toLowerCase());
				keyValueInput = new KeyValueInput(String.valueOf(docId), lemmatized);
				return keyValueInput;
		}else{
			readComplete = true;
			return null;
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
	}

	/*
	 * MEthod to fetch contents of a specified spool out file
	 */
	public String getSpoolOutFileContentForWorker(int index) {
		try {
			String fileName = spoolOutDir + "/worker" + index;
			BufferedReader bf = new BufferedReader(new FileReader(new File(
					fileName)));
			StringBuffer buf = new StringBuffer();
			String line;
			while ((line = bf.readLine()) != null)
				// read contents line by line and append to a string
				buf.append(line + '\n');
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
			System.out
					.println("In FileManagement:writeToSpoolIn: successfully wrote to: "
							+ name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Code entirely dedicated to reduce specific tasks
	private BufferedReader sortResultReader = null;
	private String outputDirName;
	private File outputDirFile;
	private File reduceOutputFile;
	private PrintWriter reduceOutputWriter;

	/*
	 * Method to handle sorting etc, called by WorkerServlet right before reduce
	 * phase starts
	 */
	public void setModeReduce(String outputDirName) throws InterruptedException {
		System.out.println("SPOOL IN DIR NAME--- "+spoolInDirName);
		spoolInDir = new File(spoolInDirName);
		listFiles = spoolInDir.list();
		if (listFiles.length == 0) {
			System.out.println("Got nothing in spool in");
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
		System.out.println("COMMAND --- "+ command);
		try {
			//Process sortProcess = Runtime.getRuntime().exec(command); // execute
			System.out.println("AFTER SORT");
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
			
			
			for(String file: listFiles){
				List<String> cmd = new ArrayList<String>();
				cmd.add("sh");
				cmd.add("-c");
				cmd.add("sort "+spoolInDirName + "/" + file+ ">> "+spoolInDirName+"/sorted");

				ProcessBuilder pb = new ProcessBuilder(cmd);
				try {
					Process p = pb.start();
					p.waitFor();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//sortProcess.waitFor(); // wait for completion
			System.out.println("AFTER WAIT");
			File temp = new File(spoolInDirName+"/sorted");
			if(temp.exists()){
				System.out.println("THIS IS STRANGE");
			}
			//String fil = "/Users/Adi/Documents/workspace/Indexer/storage/spoolIn/sorted";
			//sortResultReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fil))));
			//Thread.sleep(3000);
			sortResultReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(spoolInDirName+"/sorted"))));
			//System.out.println("THIS IS IT:"+sortResultReader.readLine());// get inputstream object
			System.out.println("AFTER LIFE");
			// create output folder
			System.out.println("OUTPUT DIR NAME --- "+outputDirName);
			//this.outputDirName = storageDir + "/" + outputDirName;
			String base = storageDir.substring(storageDir.lastIndexOf("/")+1);
			base = storageDir.replace(base, "");
			this.outputDirName = base+"/"+outputDirName;
			System.out.println(outputDirName);
			outputDirFile = new File(this.outputDirName);
			if (outputDirFile.exists()) {
				removeDirectoryWithContents(outputDirFile);
			}
			outputDirFile.mkdir();
			reduceOutputFile = new File(outputDirFile, "output");
			reduceOutputFile.createNewFile();
			reduceOutputWriter = new PrintWriter(reduceOutputFile); // attach a
																	// reader to
																	// output
																	// folder
		} catch (IOException e) {
			System.out
					.println("FileManagement: Exception in settng spool in mode: "
							+ e.getMessage());
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
		System.out.println("GetReduce");
		KeyValuesInput keyValuesInput = null;
		if (previousReduceLine == null)
			return null;
		if (!previousReduceLine.isEmpty()) {
			String parts[] = previousReduceLine.split("\t");
			System.out.println("PARTS -- "+parts);
			keyValuesInput = new KeyValuesInput(parts[0].trim(),
					parts[1].trim()); // initialize key value pair
		}
		try {
			String line = null;
			while (true) {
				line = sortResultReader.readLine(); // read line
				System.out.println("LINE -- "+line);
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
				System.out.println(parts);
				keyValuesInput.addValue(parts[1].trim());
			}
		} catch (IOException e) {
			System.out.println("EXCEPTION FileManagement:getReduceLine: "
					+ e.getMessage());
		}
		return keyValuesInput;
	}

	/*
	 * Method to write output of reduce phase, called by threads.
	 */
	public synchronized void writeToOutput(String key, Object value) {
		List<BigInteger> listDocIds = new ArrayList<BigInteger>();
		List<Double> ranks = new ArrayList<Double>();
		// Write to output
		Map<String, Double> sortedMapAsc = (Map<String, Double>) value;
		for (java.util.Map.Entry<String, Double> entry : sortedMapAsc
				.entrySet()) {
			ranks.add(entry.getValue());
			listDocIds.add(new BigInteger(entry.getKey()));

		}
		System.out.println(key+"--"+listDocIds+"---"+ranks);
		WordIndexEntity entity = new WordIndexEntity();
		entity.setWord(key);
		entity.setDocIds(listDocIds);
		entity.setRank(ranks);
		WorkerServlet.wrapperOutput.indexKey.put(entity);
	}

	/*
	 * MEthod to close the pointer to output of reduce phase. It is called by
	 * WorkerServlet when all threads are done writing output.
	 */
	public void closeReduceWriter() {
		reduceOutputWriter.close();
	}
}
