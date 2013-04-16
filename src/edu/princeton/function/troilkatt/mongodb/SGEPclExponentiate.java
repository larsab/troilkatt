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
import edu.princeton.function.troilkatt.tools.PclExponentiate;

/**
 * Exponentiate the expression values in a PCL file. If the file has not been
 * log transformed the file is copied.
 */
public class SGEPclExponentiate {
	/**
	 * Exponentiate the samples values
	 * 
	 * @param br input file handle
	 * @param bw output file handle
	 * @throws IOException 
	 */
	static public void process(BufferedReader br, BufferedWriter bw) throws IOException {
		int lineCnt = 0;
		String line;
		while ((line = br.readLine()) != null) {
			lineCnt++;
			line = line + "\n";
			
			if (lineCnt < 3) {
				// Header or weight line
				bw.write(line);
				continue;
			}
			
			String[] cols = line.split("\t");
			if (cols.length < 3) {
				System.err.println("Too few columns in row: " + line);
				continue;
			}
			
			cols[cols.length - 1] = cols[cols.length - 1].trim(); 
			bw.write(cols[0] + "\t" + cols[1] + "\t" + cols[2]);
			for (int i = 3; i < cols.length; i++) {
				bw.write("\t");
				try {
					float val = Float.valueOf(cols[i]);
					bw.write(String.valueOf(Math.exp(val)));
				} catch (NumberFormatException e) {
					// Blanks are missing values
				}
			}
			bw.write("\n");			
		}
	}
	
	/**
	 * @param args command line arguments
	 *   [0] input pcl file
	 *   [1] output pcl file
	 *   [2] MongoDB server IP address
	 *   [3] MongoDB serevr listen port
	 * 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length < 5) {
			System.err.println("Usage: java SGEPclExponentiate inputFilename outputFilename mongoServerIP mongoServerPort");
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
			SGEPclLogTransform.copy(br, bw);
		}
		else {
			PclExponentiate.process(br, bw);
		}
		
		bw.close();
		br.close();
		
		mongoClient.close(); 		
	}

}
