package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Remove overlapping samples from a PCL file. 
 * 
 * This class only consists of the main function since the code to do the
 * removal is in the GeoGSMOverlap file.
 */
public class PclRemoveOverlap {

	/**
	 * @param args command line arguments
	 *   [0] input pcl file
	 *   [1] output pcl file      
	 *   [2] file with excluded datasets and samples
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		/*
		 * Parse arguments
		 */
		if (args.length != 3) {
			System.err.println("Usage: inputPclFilename outputPclFilename excludedFilename");
			System.exit(-1);
		}
		String inputFilename = args[0];
		String outputFilename = args[1];
		String excludedFilename = args[2];
		
		/*
		 * Initialize datastructures used to determine which files and columns to delete
		 */
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		
		GeoGSMOverlap.readOverlapFile(excludedFilename, deletedDatasets, deletedSamples);
		
		// Check if entire dataset should be removed
		String dsetID = FilenameUtils.getDsetID(inputFilename);
		if (deletedDatasets.contains(dsetID)) {
			System.out.println("Dataset " + OsPath.basename(inputFilename) + " is to be deleted");
			System.exit(0);
		};
		
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));
		
		String headerLine = br.readLine();
		if (headerLine == null) {
			System.err.println("Could not read header line");
			System.exit(-1);
		}
		
		int[] deleteColumnIndexes = null;
		if (deletedSamples.containsKey(dsetID)) { // some samples should be removed
			String[] toDelete = deletedSamples.get(dsetID);
			
			// Parse first line to find indexes of samples to delete
			deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
			if (deleteColumnIndexes == null ) {
				System.err.println("Could not find all samples to delete in header line: " + headerLine);
				System.exit(-1);
			}
		}

		/*
		 * Convert file
		 */
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
			line = br.readLine();			
		}
		
		br.close();
		bw.close();
	}

}
