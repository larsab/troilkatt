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
import edu.princeton.function.troilkatt.tools.PclLogTransform;

/**
 * Log transform expression values if they are not already log transformed. For the latter
 * the file is copied to an output directory.
 */
public class SGEPclLogTransform {
	/**
	 * Helper function to copy a file line by line
	 * 
	 * @param br source file
	 * @param bw destination file
	 * @return none
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
	 *             [2] MongoDB server IP address
	 *             [3] MongoDB server listen port
	 */
	public static void main(String[] args) throws Exception {	
		if (args.length < 4) {
			System.err.println("Usage: java SGEPclLogTransform inputFilename outputFilename mongoServerIP mongoServerPort");
			System.exit(2);
		}
		
		/*
		 * Arguments
		 */
		String inputFilename = args[0];
		String gid = FilenameUtils.getDsetID(inputFilename);
		String outputFilename = args[1];		
		String serverAdr = args[2];
		int serverPort = Integer.valueOf(args[3]);

		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		/*
		 * Use the calculated "info" information to determine if valeus have already
		 * been log transformed.
		 */
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
		if (logged) { // already log transformed 
			// nothing to do just copy the file
			copy(br, bw);
		}
		else {
			PclLogTransform.process(br, bw);
		}
		
		bw.close();
		br.close();
		
		mongoClient.close(); 				
	}
}
