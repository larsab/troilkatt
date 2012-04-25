package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * Convert a GEO Dataset SOFT file to the PCL format.
 *
 * This code is based on the convertSoft2Pcl.rb ruby script
 */
public class GeoGDS2Pcl {
	// Meta data fields
	protected final static String tagTableStart = "!dataset_table_begin";
	protected final static String tagTableEnd = "!dataset_table_end";
	protected final static String tagSampleCount = "!dataset_sample_count";
	
	protected Logger toolLogger; 
	
	/*
	 * Parser state
	 */
	// Set to true once dataTableStart has been read. The next to read is then the
	// labels (header) line for the data table columns
	protected boolean atLabels; 
	protected boolean inDataSection;
	
	// It is necessary to keep track of the number of sampels in the dataset table
	// since GDSXXX_full.soft files has additional columns with meta-data that
	// are not to be included in the output pcl file
	protected int nSamples;
	protected HashMap<String, String> labels;
	
	/**
	 * Constructor
	 */
	public GeoGDS2Pcl() {
		atLabels = false;
		inDataSection = false;
		nSamples = 0;
		labels = new HashMap<String, String>();
		
		toolLogger = Logger.getLogger("troilkatt.tool-gds2pcl");
	}
	
	/**
	 * Reset parser. This method must be called for each new file if the same parser 
	 * instance is used to parse multiple input files.  
	 */
	public void reset() {
		atLabels = false;
		inDataSection = false;
		nSamples = 0;
		labels.clear();
	}
	
	/**
	 * Convert a Dataset SOFT file to the PCL format
	 * 
	 * @param ins input file
	 * @param os output file
	 * @return none
	 * @throws IOException
	 * @throws ParseException 
	 */
	public void convert(BufferedReader ins, BufferedWriter os) throws IOException, ParseException {	
		String line;
		while ((line = ins.readLine()) != null) {
			String outputLine = convertLine(line);
			if (outputLine != null) {
				os.write(outputLine);
			}
		} 
	}
	
	/**
	 * This function is called by the driver class to convert a line read from
	  * a soft file.
	 * 
	 * Note! The parser relies on global state, so it is assumed that this function
	 * is called subsequnetly for the lines in the file.
	 * 
	 * @param line line to parse
	 * @return line to be written to output file (including newline), or null if the line does not contain
	 * information to be included in the output file.
	 * @throws ParseException 
	 */
	public String convertLine(String line) throws ParseException {
		if (line == null) {
			return null;
		}
		
		if (line.endsWith("\n")) {
			line = line.substring(0, line.length() - 1);
		}
		
		String outputLine = null;
		
		/*
		 * Find labels: these are lines above the data table that starts with '#'
		 */
		if ((! inDataSection) && (line.startsWith("#"))) {
			String[] parts = line.split(" = ");
			if (parts.length != 2) {
				toolLogger.warn("Invalid label line: " + line);
				return null;
			}
			// Remove # from the key, and remove tabs and '~'
			String key = parts[0].substring(1, parts[0].length());
			key = key.replace("\t", " ");
			key = key.replace("~","-");
			// Labels are saved in the labels hash map
			labels.put(key, parts[1]);
		}
		else if (line.contains(tagSampleCount)) {
			String[] parts = line.split(" = ");
			if (parts.length != 2) {
				toolLogger.warn("Invalid sample count line: " + line);
				return null;
			}
			nSamples = Integer.valueOf(parts[1]);
		}
		/*
		 * At labels is initially false. It is set to true once table start
		 * tag has been found.
		 */
		else if (line.contains(tagTableStart)) { // Data table tag found
			// Next line in data table is the header line
			atLabels = true;
		}
		/*
		 * Parse header line in data table
		 */
		else if (atLabels) {
			atLabels = false;
			// Data section follows next
			inDataSection = true;
			
			/*
			 * Parse labels and return the first two rows in the PCL file 
			 * (headers and EWEIGHT)
			 */
			StringBuilder sb = new StringBuilder();
			
			String[] parts = line.split("\t");
			if (parts.length < 3) {
				toolLogger.warn("Invalid header line: " + line);
				throw new ParseException("Header line with less than 3 columns: " + line);
			}
			String label1 = labels.get(parts[0]);
			if (label1 == null) {				
				toolLogger.warn("Missing label: " + parts[0]);
				throw new ParseException("Missing label in header line: " + line);				
			}
			String label2 = labels.get(parts[1]);
			if (label2 == null) {
				// Not an error?
				toolLogger.warn("Missing label: " + parts[1]);
				throw new ParseException("Missing label in header line: " + line);					
			}
			sb.append(label1);
			sb.append("\t");
			sb.append(label2);
			sb.append("\tGWEIGHT"); // Note! includes tab
			for (int i = 2; i < 2 + nSamples; i++) {
				String label = labels.get(parts[i]);
				if (label == null) {
					toolLogger.warn("Missing label: " + parts[i]);
					throw new ParseException("Missing label in header line: " + line);
				}
				sb.append("\t");
				sb.append(parts[i] + ": " + label); // Include the GSM identifier				
			}
			sb.append("\n");
			
			/*
			 * Write second (EWEIGHT) row to PCL file.
			 * All EWEGITHS are set to 1
			 */
			sb.append("EWEIGHT\t\t");
			for (int i = 2; i < 2 + nSamples; i++) {
				sb.append("\t1");
			}
			sb.append("\n");
			
			outputLine = sb.toString();
		}		
		else if (inDataSection) {
			if (line.contains(tagTableEnd)) { // Data table ended
				inDataSection = false;
			}
			else {
				/*
				* Write data row
				*/
				String[] parts = line.split("\t");
				if (parts.length < 3 ) {
					toolLogger.warn("Invalid data line: " + line);
					return null;
				}
			
				StringBuilder sb = new StringBuilder();
				
				// Frist three columns
				sb.append(parts[0]);
				sb.append("\t");
				sb.append(parts[1]);
				sb.append("\t1");
				// remaining columns
				for (int i = 2; i < 2 + nSamples; i++) {
					sb.append("\t");
					try {
						@SuppressWarnings("unused")
						Float val = Float.valueOf(parts[i]);
						sb.append(parts[i]);
					} catch (NumberFormatException e) { // Not a valid floating point
						// No value written
					}
				}
				sb.append("\n");
				outputLine = sb.toString();
			}
		} // inDataSection
		
		return outputLine;
	}
	
	/**
	 * Convert a GEO dataset SOFT file to the PCL format. 
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GDSXXX.soft)
	 *  1: output filename (GDSXXX.pcl)
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {
		GeoGDS2Pcl gds2pcl = new GeoGDS2Pcl();
		
		if (argv.length < 2) {
			System.err.println("Usage: java GeoGDS2Pcl inputFilename outputFilename");
		}
		String inputFilename = argv[0];
		String outputFilename = argv[1];
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
		gds2pcl.convert(ins, os);
		os.close();
		ins.close();
	}
}
