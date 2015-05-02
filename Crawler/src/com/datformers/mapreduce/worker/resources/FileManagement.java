package com.datformers.mapreduce.worker.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
//			System.out.println("Successfully removed existing Spool-Out directory");
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
//				System.out.println("FILEMANAGEMENT EXCEPTION: "+e.getMessage());
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
	 * Method to write to a spool in file 
	 */
	public void writeToSpoolIn(BufferedReader br) {
//		System.out.println("In FileManagement:writeToSpoolIn: start");
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
//			System.out.println("In FileManagement:writeToSpoolIn: successfully wrote to: "+name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
	
