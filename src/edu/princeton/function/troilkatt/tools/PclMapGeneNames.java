package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * Map gene names
 * 
 * Based on spell/mapGeneNames.rb
 * @author larsab
 *
 */
public class PclMapGeneNames {
	
	/*
	 * Alisas to unique identifier mapping
	 */
	protected HashMap<String, String> alias2id;
	
	/*
	 * Global parser state
	 */
	// Set to true when header line is parsed (and the below nCols and maxAllovedMissingGenes
	// are initialized)
    protected boolean headerRead;
    // Set to true when the EWEIGHT line has been read
    protected boolean eweightRead;

	/**
	 * Constructor.
	 */
	public PclMapGeneNames() {
		alias2id = new HashMap<String, String>();
		headerRead = false;
		eweightRead = false;
	}
	
	/**
	 * Add gene name mappings. These are read from a file with the following format:
	 *  
	 * alias<tab>geneID<newline>
	 * 
	 * @param mapFilename filename to read
	 * @return none
	 * @throws IOException 
	 */
	public void addMappings(String mapFilename) throws IOException {
		BufferedReader ins = new BufferedReader(new FileReader(mapFilename));
		String line;
		while ((line = ins.readLine()) != null) {
			// Remove whitespace
			line = line.trim();
			// Convert all names to upperspace
			line = line.toUpperCase();
			
			String[] parts = line.split("\t");
			if (parts.length != 2) {
				System.err.println("Invalid line in mapping file: " + line);
				continue;
			}
			alias2id.put(parts[0], parts[1]);			
		}
		ins.close();
	}
	
	/**
	 * Map gene names to common namespace.
	 * 
	 * Note! at least one mapping must be added before this function is called.
	 * 
	 * @param inputFilename
	 * @param outputFilename
	 * @throws IOException 
	 */
	public void mapFile(String inputFilename, String outputFilename) throws IOException {
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
		
		mapFile(ins, os);
		
		ins.close();
		os.close();
	}
	
	/**
	 * Map genes to common namespace
	 * 
	 * @param ins input file
	 * @param os output file
	 * @throws IOException 
	 */
	public void mapFile(BufferedReader ins, BufferedWriter os) throws IOException {
		String line;		
		while ((line = ins.readLine()) != null) {
			String outputLine = mapRow(line);
			if (outputLine != null) {
				os.write(outputLine);
			}
		}
	}

	/**
	 * Map the gene identifier in the first column to the common namespace
	 * 
	 * @param line input row
	 * @return output row in the form of a tab delimited line with a newline, or
	 * null if neither the gene ID nor gene name coud be mapped, or if there were
	 * too few columns in the line.
	 */
	public String mapRow(String line) {
		line = line + "\n"; // for proper split
		
		// First two header lines are returned unmodified
		if (headerRead == false) {
			headerRead = true;
			return line;
		}
		if (eweightRead == false) {
			eweightRead = true;
			return line;
		}
		
		line = line.trim();
		String[] cols = line.toUpperCase().split("\t");		
		
		if (cols.length < 3) {
			System.err.println("Too few columns in row: " + line);
			return null;
		}
		cols[cols.length - 1] = cols[cols.length -1].trim();
		
		String fileGeneID = cols[0];
		String fileGeneName = cols[1];
		
		// Attempt first to map the geneID
		String globalID = alias2id.get(fileGeneID);
		if (globalID == null) {
			// Secondly, attempt to map the gene name
			globalID = alias2id.get(fileGeneName);
		}
		if (globalID == null) {
			System.err.println("Could not map row with id: " + fileGeneID + " and name" + fileGeneName);
			return null;
		}
		
		// Map and return new line
		cols[0] = globalID;
		cols[1] = globalID;
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cols.length - 1; i++) {
			sb.append(cols[i]);
			sb.append("\t");
		}
		sb.append(cols[cols.length - 1]);
		sb.append("\n");
		return sb.toString();
	}

	/**
	 * @param args command line arguments
	 * [0] pcl file to map aliases from
	 * [1] gene mapping file
	 * [2] output file
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Invalid arguments, usage: input.pcl genes.map output.pcl.map");
			System.exit(-1);
		}
		
		String inputFile = args[0];
		String mapFile = args[1];
		String outputFile = args[2];		
		
		PclMapGeneNames mapper = new PclMapGeneNames();
		mapper.addMappings(mapFile);
		mapper.mapFile(inputFile, outputFile);
	}
}
