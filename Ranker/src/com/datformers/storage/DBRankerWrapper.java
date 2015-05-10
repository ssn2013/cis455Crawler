package com.datformers.storage;


import com.sleepycat.je.Environment;

import java.io.File;
import java.math.BigInteger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.EntityStore;



public class DBRankerWrapper {
	
	
	private static String envDirectory = null;

	public static Environment myEnv;
	private static EntityStore store;
	private static File envHome;

//	public PrimaryIndex<BigInteger, ParsedDocument> documentKey;
//	public PrimaryIndex<BigInteger,VisitedURLStore> visitedUrlKey;
	public PrimaryIndex<BigInteger,DocRanksStore> pageRankKey;

	public DBRankerWrapper(String env) {
		
		System.out.println("OUTPUT DB PATHNAME" + env);
		envHome = new File(env);
		// if the directory does not exist, create it
		if (!envHome.exists()) {
			boolean result = false;
			try {
				envHome.mkdir();
				result = true;
			} catch (SecurityException se) {
				// handle it
			}
			if (result) {
//				System.out.println("DIR created");
			}
			//envHome = theDir;
		}
	}

	public void loadIndices() {
		
//		documentKey = store.getPrimaryIndex(BigInteger.class, ParsedDocument.class);
//		visitedUrlKey = store.getPrimaryIndex(BigInteger.class, VisitedURLStore.class);
		pageRankKey = store.getPrimaryIndex(BigInteger.class, DocRanksStore.class);
	}
	
//	public PrimaryIndex<BigInteger, ParsedDocument> getDocumentIndex() { 
//		return documentKey;
//	}

	public void configure() {
		try {
			EnvironmentConfig myEnvConfig = new EnvironmentConfig();
			StoreConfig storeConfig = new StoreConfig();
			myEnvConfig.setAllowCreate(true);
			storeConfig.setAllowCreate(true);
			//myEnvConfig.setTransactional(true);
			//storeConfig.setTransactional(true);
						// Open the environment and entity store
			myEnv = new Environment(envHome, myEnvConfig);
			
			store = new EntityStore(myEnv, "EntityStore", storeConfig);
			loadIndices();
		} catch (DatabaseException dbe) {
			System.err.println("Error opening environment and store: "
					+ dbe.toString());
			System.exit(-1);
		}
	}

	public void exit() {
		if (store != null) {
			try {
				store.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing store: " + dbe.toString());
				System.exit(-1);
			}
		}
		if (myEnv != null) {
			try {
				// Finally, close environment.
				myEnv.cleanLog();
				myEnv.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing MyDbEnv: " + dbe.toString());
				System.exit(-1);
			}
		}
	}


}
