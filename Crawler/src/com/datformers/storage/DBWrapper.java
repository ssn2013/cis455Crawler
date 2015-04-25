package com.datformers.storage;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {

	private static String envDirectory = null;

	private static Environment myEnv;
	private static EntityStore store;
	private static boolean dbOpen=false;
	
	/*
	 * Method to initialize environment
	 */
	public static void initialize(String environment) {
		try {
			if(dbOpen) return;
			dbOpen=true;
			EnvironmentConfig myEnvConfig = new EnvironmentConfig(); //initialize environment
			StoreConfig storeConfig = new StoreConfig(); //initialize store
			myEnvConfig.setAllowCreate(true);
			storeConfig.setAllowCreate(true);
			// Open the environment and entity store
			myEnv = new Environment(new File(environment), myEnvConfig);
			store = new EntityStore(myEnv, "EntityStore", storeConfig);
		
		} catch(DatabaseException dbe) {
			System.err.println("Error opening environment and store: " +
					dbe.toString());
			System.exit(-1);
		} 
	}
	
	/*
	 * Method to return store
	 */
	public static EntityStore getStore() {
		return store;
	}

	/*
	 * Method to close db and environment
	 */
	public static void commit() {
		store.sync();
	}
	public static void close() {
		//System.out.println("\nClose method called");
		if(!dbOpen) return;
		dbOpen=false;
		if (store != null) {
			try {
				//System.out.println("\nGonna close wrapper");
				store.close();
			} catch(DatabaseException dbe) {
				System.err.println("\nError closing store: " +
						dbe.toString());
				System.exit(-1);
			}
		}
		if (myEnv != null) {
			try {
				// Finally, close environment.
				System.out.println("\nGonna close Environment");
				myEnv.cleanLog();
				
				myEnv.close();
				
			} catch(Exception dbe) {
				
				System.err.println("\nError closing MyDbEnv: " +
						dbe.toString());
				System.exit(-1);
			}
		} 
		System.out.println("\nSuccessful closing of everything, now exiting");
	}
}
