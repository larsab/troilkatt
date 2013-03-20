package edu.princeton.function.troilkatt.sge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;

public class PclRemoveOverlapping {

	public static void process(String inputFilename, String outputFilename, String overlapFilename) throws IOException {
		// Datasets and samples to be deleted
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();

		GeoGSMOverlap.readOverlapFile(overlapFilename, deletedDatasets, deletedSamples, null);
			
		String dsetID = FilenameUtils.getDsetID(inputFilename);
		if (deletedDatasets.contains(dsetID)) {			
			// Nothing more to do
			return;
		}
		
		/*
		 * Open input stream
		 */			
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		
		// Read header line
		String headerLine = br.readLine();
		if (headerLine == null) {
			System.err.println("Could not read header line");			
			return;
		}
			
		int[] deleteColumnIndexes = null;
		if (deletedSamples.containsKey(dsetID)) { // some samples should be removed
			String[] toDelete = deletedSamples.get(dsetID);
			
			// Parse first line to find indexes of samples to delete
			deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
			if (deleteColumnIndexes == null ) {
				System.err.println("Could not find all samples to delete in header line: " + headerLine);					
				return;
			}
		}

		/*
		 * Open output stream and process the file
		 */									
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));		
		processFile(br, bw, deleteColumnIndexes, headerLine);
		bw.close();		
		br.close();		
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
	 * @param args [0] input filename
	 *             [1] output filename
	 *             [2] overlap filename
	 */
	public static void main(String[] args) throws Exception {			
		String inputFilename = args[0];
		String outputFilename = args[1];
		String overlapFilename = args[2];
		process(inputFilename, outputFilename, overlapFilename);
	}

}