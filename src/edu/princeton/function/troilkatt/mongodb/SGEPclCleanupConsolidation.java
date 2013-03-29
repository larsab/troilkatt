package edu.princeton.function.troilkatt.mongodb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.PclCleanupConsolidation;

public class SGEPclCleanupConsolidation {
	/**
	 * Helper function to copy a file line by line
	 * 
	 * @param br source file
	 * @param bw destination file
	 * @return 
	 * @throws IOException 
	 */
	public static void copy(BufferedReader br, BufferedWriter bw) throws IOException {
		String line;
		while ((line = br.readLine()) != null) {
			bw.write(line + "\n");
		}
	}

	/**
	 * @param args [0] inputFilename
	 *             [1] outputFilename
	 *             [2] MongoDB server address
	 */
	public static void main(String[] args) throws Exception {	
		if (args.length < 3) {
			System.err.println("Usage: java PclLogTransform inputFilename outputFilename mongoServerAddress");
			System.exit(2);
		}
		
		/*
		 * Arguments
		 */
		String inputFilename = args[0];
		String gid = FilenameUtils.getDsetID(inputFilename);
		String outputFilename = args[1];		
		String serverAdr = args[2];

		MongoClient mongoClient = new MongoClient(serverAdr);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		String loggedStr = (String) GeoMetaCollection.getField(coll, gid, "calculated:logged");
		if (loggedStr == null) {
			System.err.println("Could not read meta data for: " + gid);				
			return;
		}
		boolean logged = loggedStr.equals("1");
				
		/*
		 * Open input and output streams
		 */									
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));					
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));
		
		/*
		 * Write converted file
		 */
		PclCleanupConsolidation cleaner = new PclCleanupConsolidation(logged);
		cleaner.process(br, bw);
		
		bw.close();
		br.close();
 	
		mongoClient.close();
	}
}
