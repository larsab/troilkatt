package edu.princeton.function.troilkatt.mongodb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.PclMissingValues;

/**
 * Add missing values. Also remove rows, columns and with too many missing values.
 */
public class SGEPclMissingValues {
	

	/**
	 * Main entry point.
	 * 
	 * @param args [0] inputFilename
	 *             [1] outputFilename
	 *             [2] gene cutoff value (float)
	 *             [3] sample cutoff value (float)
	 *             [4] dataset cutoff value (float)
	 *             [5] MongoDB server IP address
	 *             [6] MongoDB server listen port
	 */
	public static void main(String[] args) throws Exception {	
		if (args.length < 7) {
			System.err.println("Usage: java PclMissingValues inputFilename outputFilename geneCutoff sampleCutoff datasetCutoff mongoServerAddress");
			System.exit(2);
		}
		
		/*
		 * Arguments
		 */
		String inputFilename = args[0];
		String gid = FilenameUtils.getDsetID(inputFilename);
		String outputFilename = args[1];		
		float geneCutoff = Float.valueOf(args[2]);
		int sampleCutoff = Integer.valueOf(args[3]);
		float datasetCutoff = Float.valueOf(args[4]);
		String serverAdr = args[5];
		int serverPort = Integer.valueOf(args[6]);

		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		/*
		 * "info" calculation determined if zeros are missing values and the cutoff-value for
		 * expression values.
		 */
		String zeroAreMVsStr = (String) GeoMetaCollection.getField(coll, gid, "calculated:zerosAreMVs");
		if (zeroAreMVsStr == null) {
			System.err.println("Could not read meta data for: " + gid);				
			return;
		}
		boolean zerosAsMVs = zeroAreMVsStr.equals("1");		
		
		String mvCutoffStr = (String) GeoMetaCollection.getField(coll, gid, "calculated:cutoff");
		if (mvCutoffStr == null) {
			System.err.println("Could not read meta data for: " + gid);		
			return;
		}
		
		float mvCutoff = Float.NaN;
		// Need to catch the NumberFormatException since this value may NaN
		try {
			mvCutoff = Float.valueOf(mvCutoffStr);
		} catch (NumberFormatException e) {
			// Do nothing since this is expected for som datasets
			System.err.println("Missing value cutoff is: NaN (this is expected for some datasets)");
		}
		
		// MissingValue tool (initialized in each map call)
		PclMissingValues converter = new PclMissingValues(geneCutoff, sampleCutoff, datasetCutoff, zerosAsMVs, mvCutoff);
				
		/*
		 * Open input and output streams
		 */									
		BufferedReader bri = new BufferedReader(new FileReader(inputFilename));					
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));
		
		/*
		 * Write converted file
		 */
		String line;
		while ((line = bri.readLine()) != null) {
			String outputLine = converter.insertMissingValues(line);
			if (outputLine != null) {
				bw.write(outputLine);
			}
		}
		bw.close();
		bri.close();
 				
		if (converter.tooManyMissingValues()) {
			// too many missing: file should not be included
			OsPath.delete(outputFilename);
		}		
		
		mongoClient.close();
	}
}
