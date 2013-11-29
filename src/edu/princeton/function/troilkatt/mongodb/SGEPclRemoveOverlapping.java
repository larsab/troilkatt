package edu.princeton.function.troilkatt.mongodb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.hbase.TroilkattTable;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;

/**
 * Remove overlapping samples from a PCL file. The overlap is calcualated before running this
 * tool.
 */
public class SGEPclRemoveOverlapping {

	/**
	 * Remove overlapping samples from a PCL file. The overlapping samples to remove are read
	 * from the MongoDB geoMeta collection
	 * 
	 * @param inputFilename input PCL filename
	 * @param outputFilename output PCL filename
	 * @param serverAdr MongoDB server IP address
	 * @param serverPort MongoDB server listen port
	 * @return none
	 */
	public static void process(String inputFilename, String outputFilename, String serverAdr, int serverPort) throws IOException {
		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB("troilkatt");
		DBCollection coll = db.getCollection("geoMeta");
				
		String dsetID = FilenameUtils.getDsetID(inputFilename, true);
		String allSamplesStr = (String) GeoMetaCollection.getField(coll, dsetID, "meta:sampleIDs");
		if (allSamplesStr == null) {
			System.err.println("Could not find MongoDB meta:sampleIDs field for: " + dsetID);
			System.exit(-1);
		}
		ArrayList<String> allSamples = TroilkattTable.string2array(allSamplesStr);
		
		String includedSamplesStr = (String) GeoMetaCollection.getField(coll, dsetID, "calculated:sampleIDs-overlapRemoved");
		ArrayList<String> includedSamples = null;
		if (includedSamplesStr == null) {
			System.err.println("Could not find MongoDB calculated:sampleIDs-overlapRemoved field for: " + dsetID);
			System.err.println("This is expected for datasets without any overlapping samples");
			includedSamples = allSamples; // no samples where removed			
		}
		else if (includedSamplesStr.equals("none")) { // dataset should be deleted
			// Nothing more to do
			mongoClient.close();
			return;

		}
		else {
			includedSamples = TroilkattTable.string2array(includedSamplesStr);
		}
		
		int nToDelete = allSamples.size() - includedSamples.size();
		String[] toDelete = new String[nToDelete];
		int i = 0;
		if (nToDelete > 0) {
			for (String s1: allSamples) {
				if (! includedSamples.contains(s1)) {
					toDelete[i] = s1;
				}					
			}
		}
		
		/*
		 * Open input stream
		 */			
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		
		// Read header line
		String headerLine = br.readLine();
		if (headerLine == null) {
			System.err.println("Could not read header line");		
			br.close();
			return;
		}
			
		int[] deleteColumnIndexes = null;
		if (nToDelete > 0) { // some samples should be removed						
			// Parse first line to find indexes of samples to delete
			deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
			if (deleteColumnIndexes == null ) {
				System.err.println("Could not find all samples to delete in header line: " + headerLine);
				br.close();
				return;
			}
		}

		/*
		 * Open output stream and process the file
		 */									
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));		
		processFile(br, bw, deleteColumnIndexes, headerLine);
		
		/*
		 * Cleanup
		 */
		bw.close();		
		br.close();	
		mongoClient.close();
	}	
		
	/**
	 * Helper function to read in, check samples against samples to be deleted, and write non-deleted
	 * samples
	 * @param lin initialized line input stream
	 * @param bw initialized buffered writer stream
	 * @param deleteColumnIndexes indexes to delete, or null if no samples should be deleted
	 * @param headerLine previously read header lien
	 * @throws IOException 
	 */
	public static void processFile(BufferedReader lin, BufferedWriter bw, int[] deleteColumnIndexes,
			String headerLine) throws IOException {
		String line = headerLine;
		while (line != null) {
			// read line is at end of the loop since the first line to parse is the
			// provided header line

			String outputLine = null;
			if (deleteColumnIndexes == null) { // nothing to delete
				outputLine = line;
			}
			else {
				outputLine = GeoGSMOverlap.deleteColumnsFromLine(line, deleteColumnIndexes);
			}

			bw.write(outputLine + "\n");

			// Read in next line to parse				
			line = lin.readLine();
		}
	}
	

	/**
	 * Main entry point
	 * 
	 * @param args [0] input filename
	 *             [1] output filename
	 *             [2] mongoDB server IP
	 *             [3] mongoDB server port
	 */
	public static void main(String[] args) throws Exception {		
		if (args.length < 4) {
			System.err.println("Usage: java PclRemoveOverlapping inputFilename outputFilename mongoDBServerIP mongoDBServerPort");
			System.exit(2);
		}
		
		String inputFilename = args[0];
		String outputFilename = args[1];
		String serverAdr = args[2];
		int serverPort = Integer.valueOf(args[3]);
		process(inputFilename, outputFilename, serverAdr, serverPort);
	}
}