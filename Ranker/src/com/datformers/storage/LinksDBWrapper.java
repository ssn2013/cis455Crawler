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
import com.sleepycat.persist.evolve.Mutations;

public class LinksDBWrapper {
	
	
	private static String envDirectory = null;

	public static Environment myEnv;
	private static EntityStore store;
	private static File envHome;

	public PrimaryIndex<BigInteger, ParsedDocument> linksKey;
	
	public LinksDBWrapper(String env) {
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
		linksKey = store.getPrimaryIndex(BigInteger.class, ParsedDocument.class);
	}

	public void configure() {
		try {
			//Mutations mutations = new Mutations();
			
			EnvironmentConfig myEnvConfig = new EnvironmentConfig();
			StoreConfig storeConfig = new StoreConfig();
			//storeConfig.setMutations(mutations);
			myEnvConfig.setAllowCreate(true);
			storeConfig.setAllowCreate(true);
			//myEnvConfig.setTransactional(true);
			//storeConfig.setTransactional(true);
						// Open the environment and entity store
			myEnv = new Environment(envHome, myEnvConfig);
			
			store = new EntityStore(myEnv, "EntityStore1", storeConfig);
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
				System.out.println("CLEAN LOG");
				myEnv.cleanLog();
				myEnv.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing MyDbEnv: " + dbe.toString());
				System.exit(-1);
			}
		}
	}

}
