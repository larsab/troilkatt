package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.princeton.function.troilkatt.fs.OsPath;


/**
 * Convert a GEO Dataset SOFT file to the PCL format.
 * 
 * Note! The parsing is complex so it is done in two stages, each requiring a pass through
 * the file:
 * 1. The file is parsed to find the meta-data necessary for the second stage
 * 2. The file is parsed to extract the data to be written to the output PCL file 
 *
 * This code is based on SeriesFamilyParser.java
 */
public class GeoGSE2Pcl {
	/*
	 * "Knowledge base", which simply is a list of column header keywords
	 * 
	 * NOTE! All should be in upper case
	 */
	// List of keywords used to identify the platform probe column number
	public static final String[] platformProbeIDs ={"ID"}; 	                          
	// List of keywords used to detect the gene names column number (in prioritized order)                
	public static final String[] geneNames = {"GENE_SYMBOL", "GENE SYMBOL", "GENE_NAME", "GENE NAME", "GENE ID", "ORF", "DDB"};		
	// List of keywords used to detect the sample probe ID    
	public static final String[] sampleProbeIDs = {"ID_REF"};    
	// List of keywords used to detect the expression values    
	public static final String[] expressionValues = {"VALUE"};
	// During zeros-as-missing-values estimation, zeros with fewer than this number decimals
	// are considered to be true values (and hence not missing values)
	public static final int valueDesimals = 5;
	
	public static final String EMPTY_STRING = "";
	
		
	// Series ID
	public String dsetID;
	/*
	 * Data structures set on first pass of file (stage1)
	 */	
	// Per-platform, index of column with unique platform IDs
	// The platform IDs are used to build a data structure with gene ID -> expression values mapping
	public HashMap<String, Integer> platformIDColumns; 
	// Per-platform, index of column with gene names
	public HashMap<String, Integer> platformGeneNameColumns;

	// Per-sample, index of columns of the sample table for the unique IDs
	// The sample IDs are used as index in the platformID->values data structure
	public HashMap<String, Integer>sampleGeneIDColumns;
	// Per-sample index of expression value columns
	public HashMap<String, Integer>sampleValueColumns;

	// For each platform, a list samples IDs
	public HashMap<String, ArrayList<String>> platformSampleIDs;
	// Per-sample title
	public HashMap<String, String> sampleTitles;

	// Ordered list of ALL sample IDs (based on order in file)
	public ArrayList<String> orderedSampleIDs;
	// Ordered list of ALL platform IDs (based on order in file)
	public ArrayList<String> orderedPlatformIDs;
	
	// Whether to treat zeros as missing values
	// Note! The decision is per file (and NOT per sample)
	public boolean zerosAsMissingVals;
	
	/*
	 * Data structures updated in second pass (stage2)
	 * 
	 * Note that the second pass is done once per platform
	 */
	// Gene IDs to gene name mappings
	public HashMap<String, String> geneID2Name;
	// Gene ID to expression value mappings
	// Note a SoftReference is used to avoid OutOfMemoryError's	
	public SoftReference<HashMap<String, float[]>> geneID2ValsSoftReference;		
	// An ordered list of geneIDs (based on order in file)
	public ArrayList<String> orderedGeneIDs;
	
	/*
	 * Other data structures
	 */
	// A lot of debugging information is written to the logger
	protected Logger logger;
	
	/**
	 * Constructor 
	 * 
	 * @param l initialized logger
	 */
	public GeoGSE2Pcl(Logger l) {
		logger = l;			
	}
	
	/**
	 * Constructor 
	 */
	public GeoGSE2Pcl() {
		logger = Logger.getLogger("GeoGSE2PCL");		
	}

	/**
	 * Parse a SOFT file to find gene name and expression value columns.
	 * 
	 * @param br input SOFT file
	 * @param fos output file which contains the stage1 data structures in serialized form.
	 * If null no output file is written.
	 * @return true if file could successfully be parsed to find gene IDs and sample values.
	 * Note that missing gene names are not considered an error
	 * @throws ParseException if series file contains invalid entries that prevent
	 * @throws IOException 
	 */
	public boolean stage1(BufferedReader br, FileOutputStream fos) throws ParseException, IOException {
		/*
		 * Parser state
		 */
		boolean inPlatformTableHeader = false;	
		//boolean inPlatformTable = false;
		boolean inSampleTableHeader = false;
		boolean inSampleTable = false;
		String currentPlatformID = null;
		String currentSampleID = null;
		
		int currentPlatformIDCol = -1;
		int currrentGeneNamesCol = -1;
		int currentSampleProbeIDCol = -1;
		// Expression values column in sample table
		int currentExpressionValueCol = -1;
		// Column header for expression values column
		String currentExpressionValueHeader = null;
		
		// Set to true if an error occurs. That is, if sampleIDs or Expression values
		// columns could not be found
		boolean parseErrors = false;
		
		/*
		 * Errors encountered during the parsing
		 */
		// Set to true if currently parsed sample has missing expression values
		boolean missingExpressionValueCol = false;
		// Set to true if currently parsed sample has "null" expression values
		boolean nullExpressionValue = false;
		// Set to true if currently parsed sample has "empty  expression values
		boolean emptyExpressionValue = false;
		// Set to true if currently parsed sample has integer (no desimal) expression values
		boolean intExpressionValue = false;
		// An expression value that could not be parsed was encountered
		boolean unknownFormatExpressionValue = false;
		
		/*
		 * Statistics
		 */
		/* Number of zero expression values with at least N desimals, and no desimals. 
		 * These are used to determine whether to treat zero's as missing values
		 * Note! The counts are aggregated for all samples */
		int zeroAsValue = 0;
		int zeroAsMissing = 0;
		
		/*
		 * Output data structures
		 */
		platformIDColumns = new HashMap<String, Integer>();
		platformGeneNameColumns = new HashMap<String, Integer>();
	
		sampleGeneIDColumns = new  HashMap<String, Integer>();
		sampleValueColumns = new HashMap<String, Integer>();
	
		platformSampleIDs = new HashMap<String, ArrayList<String>>();
		sampleTitles = new HashMap<String, String>();
		orderedPlatformIDs = new ArrayList<String>();
		orderedSampleIDs = new ArrayList<String>();
		
		dsetID = null;	
		
		/*
		 * Read in file to set above data structures
		 */
		String line;
		while ((line = br.readLine()) != null) {								
			/*
			 *  Parse lines to check if parser state should be changed
			 */
			if (line.contains("!Series_geo_accession")) {
				dsetID = getVal(line);
				if (dsetID == null) {
					throw new ParseException("Invalid series ID: " + line);					
				}									
			}
			else if (line.contains("^PLATFORM")) {
				/*
				 * This is the only exception that is thrown, since invalid plarformIDs
				 * makes it impossible to split the file by platform
				 */
				currentPlatformID = getVal(line);
				if (currentPlatformID == null) {
					throw new ParseException("Invalid ^PLATFORM line: " + line);
				}
				if (orderedPlatformIDs.contains(currentPlatformID)) {
					throw new ParseException("Duplicate Platform ID: " + currentPlatformID);
				}
				orderedPlatformIDs.add(currentPlatformID);
				platformSampleIDs.put(currentPlatformID, new ArrayList<String>());			
			}
			else if (line.contains("!platform_table_begin")) {
				inPlatformTableHeader = true; // Next line is the header line
				//inPlatformTable will be set to true when header line has been parsed
			}
			else if (line.contains("!platform_table_end")) {
				//inPlatformTable = false;
				
				if (currentPlatformID == null) {
					throw new ParseException("Current platform ID is not set");
				}
				if (platformIDColumns.get(currentPlatformID) == null) {
					platformIDColumns.put(currentPlatformID, currentPlatformIDCol);
					platformGeneNameColumns.put(currentPlatformID, currrentGeneNamesCol);               	
				}
				else {
					throw new ParseException("Platform columns already set for platform: " + currentPlatformID);
				}
				currentPlatformID = null;
				currentPlatformIDCol = -1;
				currrentGeneNamesCol = -1;
			}
			else if (line.contains("^SAMPLE")) {			
				currentSampleID = getVal(line);
				if (currentSampleID == null) {
					throw new ParseException("Invalid ^SAMPLE line: " + line);
				}
				if (orderedSampleIDs.contains(currentSampleID)) {
					throw new ParseException("Duplicate sample ID: " + currentSampleID);
				}
				orderedSampleIDs.add(currentSampleID);			
			}
			else if (line.contains("!Sample_title")) {
				String val = getVal(line);
				if (val == null) {
					throw new ParseException("Invalid !Sample_title: " + line);
				}						
				sampleTitles.put(currentSampleID, val);
			}
			else if (line.contains("!Sample_platform_id")) {
				String platformID = getVal(line);
				if (platformID == null) {
					throw new ParseException("Invalid platform ID: " + line);
				}	
				
				ArrayList<String> samples = platformSampleIDs.get(platformID);
				if (samples == null) {
					throw new ParseException("Platform sample ID data structure not initialized for platform: " + platformID);
				}
				samples.add(currentSampleID);
			}
			else if (line.contains("!sample_table_begin")) {                
				inSampleTableHeader = true; // The next line contains the sample header columns
				// inSampleTable will be set to true when header lines has been parsed
			}
			else if (line.contains("!sample_table_end")) {
				inSampleTable = false; // stop counting zeros	 
							
				if (currentSampleID == null) {
					throw new ParseException("Current sample ID is not set");
				}
				if (sampleGeneIDColumns.get(currentSampleID) == null) {
					sampleGeneIDColumns.put(currentSampleID, currentSampleProbeIDCol);
					sampleValueColumns.put(currentSampleID, currentExpressionValueCol);
				}
				else {
					throw new ParseException("Sample columns already set for sample: " + currentSampleID);
				}
				currentSampleID = null;				
				currentSampleProbeIDCol = -1;
				currentExpressionValueCol = -1;
				currentExpressionValueHeader = null;
				
				if (missingExpressionValueCol) {
					logger.warn("Missing expression value column");
				}
				missingExpressionValueCol = false;
				
				if (nullExpressionValue) {
					logger.warn("'null' expression values found");
				}
				nullExpressionValue = false;
				
				if (emptyExpressionValue) {
					logger.warn("Empty expression values found");
				}
				emptyExpressionValue = false;
				
				if (intExpressionValue) {
					logger.warn("'0' (no desimals) expression values found");
				}
				intExpressionValue = false;
				
				if (unknownFormatExpressionValue) {
					logger.warn("An expression value with unknown format was found");
				}
				unknownFormatExpressionValue = false;
			}
			/*
			 * Parse line based on parser state
			 */
			else if (inPlatformTableHeader) {
		        String[] parts = line.split("\t");
		
		        // Attempt to find one or more platform probe ID columns
		        int nPlatfomProbeIDColsFound = 0;
		        for (String s: platformProbeIDs) {	        
		        	for (int i = 0 ; i < parts.length; i++) {
		        		String header = parts[i].trim().toUpperCase();
		        		if (header.equals(s)) {
		        			if (currentPlatformIDCol == -1) {	               
		        				logger.info("Platform probe ID col: " + header + " (col: " + i + ")");
		        				currentPlatformIDCol = i;	                				
		        			}
		        			else {
		        				logger.warn("Multiple proble IDs col: " + header + " (col: " + i + ")");
		        			}
		
		        			nPlatfomProbeIDColsFound++;
		        		}	        			
		        	}
		        }	   
		        if (nPlatfomProbeIDColsFound > 1) {
		        	logger.error("Multiple platform probe ID columns found");
		        	parseErrors = true;
		        }
		        else if (nPlatfomProbeIDColsFound == 0) {
		        	logger.error("Platform probe ID column was not found");
		        	parseErrors = true;
		        }
		        
		        // Attempt to find one or more columns with gene names
		        int nGeneNameColFound = 0;
		        for (String s: geneNames) {
		        	for (int i = 0 ; i < parts.length; i++) {
		        		String header = parts[i].trim().toUpperCase();
		        		if (header.equals(s)) {
		        			if (currrentGeneNamesCol == -1) {	               
		        				logger.info("Gene names col: " + header + " (col: " + i + ")");
		        				currrentGeneNamesCol = i;	                				
		        			}
		        			else {
		        				logger.warn("Multiple gene name columns: " + header + " (col: " + i + ", old: " + currrentGeneNamesCol + ")" );
		        			}	        				
		        			nGeneNameColFound ++;
		        		}
		        	}	                	
		        }
		        if (nGeneNameColFound == 0) {
		        	// It is not considered an error if the gene names column could not be found
		        	// since the geneIDs can be mappped to gene names later 
		        	logger.warn("Could not find a column with gene names");
		        }
		        
		        inPlatformTableHeader = false; // Parsed header line
		        //inPlatformTable = true;
			}
			else if (inSampleTableHeader) {
				String[] parts = line.split("\t");	        	
		        	
				// Attempt to find sample probe ID column
				int nSampleProbeIDsFound = 0;
				for (String s: sampleProbeIDs) {
					for (int i = 0; i < parts.length; i++) {
						String header = parts[i].trim().toUpperCase();
		
						if (header.equals(s)) {
							if (currentSampleProbeIDCol == -1) {	               
								logger.info("Sample probe ID col: " + header + " (col: " + i + ")");
								currentSampleProbeIDCol = i;
								nSampleProbeIDsFound++;
							}
							else if (currentSampleProbeIDCol != i) {
								logger.warn("Inconsistent sample probe IDs cols: new: " + i + " previous: " + currentSampleProbeIDCol + ")");
								nSampleProbeIDsFound++;
							}	        											
						}
					}
				}
				if (nSampleProbeIDsFound > 1) {
					logger.error("Multiple sample probe ID columns found");
					parseErrors = true;
				}
				else if (nSampleProbeIDsFound == 0) {
					logger.error("Sample probe ID column not found");
					parseErrors = true;
				}
		
				// Attempt to find expression values column
				int nExpressionValueColsFound = 0;
				for (String s: expressionValues) {
					for (int i = 0; i < parts.length; i++) {
						String header = parts[i].trim().toUpperCase();
		
						if (header.equals(s)) {
							if (currentExpressionValueCol == -1) {	               
								logger.info("Expression value col: " + header + " (col: " + i + ")");
								currentExpressionValueCol = i;
								currentExpressionValueHeader = header;
								nExpressionValueColsFound++;
							}
							else if (currentExpressionValueCol != i) {
								logger.warn("Inconsistent expression value cols: new: " + i + " previous: " + currentExpressionValueCol + ")");
								nExpressionValueColsFound++;
							}	        											
						}
					}
				}
		        if (nExpressionValueColsFound > 1) {
		        	logger.error("Multiple expression value columns found");
		        	parseErrors = true;
		        }
		        else if (nExpressionValueColsFound == 0) {
		        	logger.error("Expression value column not found");
		        	parseErrors = true;
		        }
				
				inSampleTableHeader = false; // Done parsing sample table header
				inSampleTable = true; // Sample table values follows
			}		
			else if (inSampleTable) {						
				// Column header not set (this is the first line in the sample table)
				String[] parts = line.split("\t");
				if (currentExpressionValueCol == -1) {  // Expression value column not yet found
					// Compare sample table columns against the expression value header
					for (int i = 0; i < parts.length; i++) {
						if (parts[i].equals(currentExpressionValueHeader)) {
							currentExpressionValueCol = i;						
							break;
						}
					}
					
					if (currentExpressionValueCol == -1) { // expression values not found for sample table                        
						missingExpressionValueCol = true;
					}
				}
				// Expression value row (not first line in sample table)
				else { 
					if (parts.length <= currentExpressionValueCol) {
						// Ignore
					}
					else {         	        				        			
						String ev = parts[currentExpressionValueCol].trim();
						if (ev.equals("")) {
							// Is a missing value
							emptyExpressionValue = true;						
						}
						else if (ev.toLowerCase().equals("null")) {                            
							nullExpressionValue = true;						
						}
						else if (ev.equals("0")) {
							zeroAsMissing += 1;						
							intExpressionValue = true;
						}					
						else {
							try {
								if (Float.valueOf(ev) == 0.0) {											
									if ((ev.charAt(1) == '.') && (ev.length() >= (valueDesimals - 1))) {
										zeroAsValue += 1;
									}
									else {
										zeroAsMissing += 1;
									}
								}
							} catch (NumberFormatException e) {
								unknownFormatExpressionValue = true;
								logger.debug("NumberFormatException for: " + ev);
							}
						}
					}
				}
			} // else if in sample table
		} // while ! EOF
		
		/* 
		 * Determine if zero values should be treated as missing
		 */
		if ((zeroAsValue == 0) && (zeroAsMissing == 0)) {
			zerosAsMissingVals = false;
		}
		else if (zeroAsValue >= zeroAsMissing) {
			zerosAsMissingVals = false;
		}
		else {
			zerosAsMissingVals = true;
		}
		
		if (parseErrors) {
			// Do not write output if parsing failed
			return false; // failure
		}
		else {
			// If output file is specified then stage1 parameters should be written to an output file
			if (fos != null) {
				writeStage1Results(fos);
			}
			
			return true; // success
		}
	}
	
	/**
	 * Method that writes stage1 data structures in serialized form.
	 * 
	 * Note! the results are written in the forma of serialized data structures.
	 * These are ment to be used only as input to stage2 and not for persistent
	 * storage nor analytics.
	 * 
	 * @param fos output file
	 * @throws IOException 
	 */
	private void writeStage1Results(FileOutputStream fos) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(dsetID);
		oos.writeObject(platformIDColumns);
		oos.writeObject(platformGeneNameColumns);
		oos.writeObject(sampleGeneIDColumns);
		oos.writeObject(sampleValueColumns);
		oos.writeObject(platformSampleIDs);
		oos.writeObject(sampleTitles);
		oos.writeObject(orderedSampleIDs);
		oos.writeObject(orderedPlatformIDs);		
		oos.writeObject(zerosAsMissingVals);
	}
	
	/**
	 * Method to read stage1 data structures in serialized form. This function is
	 * called from the constructor.
	 * 
	 * @param fis input file. This file is written bu the writeStage1Results()
	 * method
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public void readStage1Results(FileInputStream fis) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(fis);
		dsetID = (String) ois.readObject();		
		/*
		 * Extract to local variable where the unchecked cast warning is suppressed
		 * This is also done for the other objects read from the file
		 */
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> readObject1 = (HashMap<String, Integer>) ois.readObject();
		platformIDColumns = readObject1;
				
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> readObject2 = (HashMap<String, Integer>) ois.readObject();
		platformGeneNameColumns = readObject2;
			
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> readObject3 = (HashMap<String, Integer>) ois.readObject();
		sampleGeneIDColumns = readObject3;
		
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> readObject4 = (HashMap<String, Integer>) ois.readObject();
		sampleValueColumns = readObject4;
		
		@SuppressWarnings("unchecked")
		HashMap<String, ArrayList<String>> readObject5 = (HashMap<String, ArrayList<String>>) ois.readObject();
		platformSampleIDs = readObject5;
		
		@SuppressWarnings("unchecked")
		HashMap<String, String> readObject6 = (HashMap<String, String>) ois.readObject();
		sampleTitles = readObject6; 
		
		@SuppressWarnings("unchecked")
		ArrayList<String> readObject7 = (ArrayList<String>) ois.readObject();
		orderedSampleIDs = readObject7;
		
		@SuppressWarnings("unchecked")
		ArrayList<String> readObject8 = (ArrayList<String>) ois.readObject();
		orderedPlatformIDs = readObject8;		
		
		Boolean readObject9 = (Boolean)ois.readObject();
		zerosAsMissingVals = readObject9;
	}

	/**
	 * Split a specific platform from a SOFT file and write the meta-data and samples for this 
	 * platform to the output file. Samples belonging to other platforms are ignored.
	 * 
	 * Note! stage1 must have been run before this function is called
	 * 
	 * @param br SOFT file to read from
	 * @param bw split SOFT file to write to
	 * @param pid platform to split
	 * @return none
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void writeSoftPerPlatform(BufferedReader br, BufferedWriter bw, String pid) throws ParseException, IOException {
		/*
		 * Parser state
		 */
		boolean inPlatformTableHeader = false;
		boolean inPlatformTable = false;
		boolean inSampleTableHeader = false;
		boolean inSampleTable = false;
		// Set to true to ensure that all meta data before first platform table are written
		boolean inPlatformMeta = true;
		boolean inSampleMeta = false;
		
		String currentPlatformID = null;
		String currentSampleID = null;	
		
		/*
		 * Parse file
		 */
		String line;
		while ((line = br.readLine()) != null) {
			if (line.contains("^PLATFORM")) {
				currentPlatformID = getVal(line);
				if (currentPlatformID == null) {
					throw new ParseException("Invalid ^PLATFORM line: " + line);
				}
				// Only write the specified platform
				if (! pid.equals(currentPlatformID)) {
					// By not setting inPlatformHeader=true the entire platform table will be skipped
					inPlatformMeta = false;
					continue; 
				}
				
				bw.write(line + "\n");	
				
				inPlatformMeta = true;
			}			
			else if (line.contains("!platform_table_begin")) {
				inPlatformMeta = false;
				
				// Only write the specified platform
				if (! pid.equals(currentPlatformID)) {
					// By not setting inPlatformHeader=true the entire platform table will be skipped
					continue; 
				}						
				bw.write(line + "\n");
				inPlatformTableHeader = true; // Next line is the header line
				//inPlatformTable will be set to true when header line has been parsed
			}
			else if (line.contains("!platform_table_end")) {
				bw.write(line + "\n");
				currentPlatformID = null;			
				inPlatformTable = false;
			}
			else if (line.contains("^SAMPLE")) {			
				currentSampleID = getVal(line);
				if (currentSampleID == null) {
					throw new ParseException("Invalid ^SAMPLE line: " + line);
				}
				currentPlatformID = getPlatformID(currentSampleID);
				// Only write samples belonging to the specified platform				
				if (! pid.equals(currentPlatformID)) {
					// By not setting inSampleHeaderTable=true the entire sample table will be skipped
					continue;
				}
				inSampleMeta = true;
				bw.write(line + "\n");
			}
			else if (line.contains("!sample_table_begin")) {
				inSampleMeta = false;
				
				currentPlatformID = getPlatformID(currentSampleID);
				// Only write samples belonging to the specified platform				
				if (! pid.equals(currentPlatformID)) {
					// By not setting inSampleHeaderTable=true the entire sample table will be skipped
					continue;
				}
				bw.write(line + "\n");
				inSampleTableHeader = true; // The next line contains the sample header columns
				// inSampleTable will be set to true when header lines has been parsed
			}
			else if (line.contains("!sample_table_end")) {		
				bw.write(line + "\n");
				currentSampleID = null;
				currentPlatformID = null;			
				inSampleTable = false; 	 
			}
			else if (inPlatformMeta) {				
				bw.write(line + "\n");					
			}
			else if (inPlatformTableHeader) {
				bw.write(line + "\n");
				inPlatformTableHeader = false; // Parsed header line
				inPlatformTable = true;
			}
			else if (inPlatformTable) {
				bw.write(line + "\n");	
			}
			else if (inSampleMeta) {
				bw.write(line + "\n");
			}
			else if (inSampleTableHeader) {
				bw.write(line + "\n");
				inSampleTableHeader = false; // Done parsing sample table header
				inSampleTable = true; // Sample table values follows
			}		
			else if (inSampleTable) {					
				bw.write(line + "\n");			
			} // else if in sample table
		} // while ! EOF
	}
	
	/**
	 * This function is called by the driver class to convert a line read from
	  * a soft file.
	 * 
	 * Note! The parser relies on global state, so it is assumed that this function
	 * is called subsequently for the lines in the file.
	 * 
	 * Note! The parser uses soft references to avoid an OutOfMemoryError. If there is
	 * not enough memory a ParseException is thrown
	 * 
	 * @param br SOFT file to read from
	 * @param bw optional parameter for the output PCL file. If null, no output is written.
	 * @param pid optional paramter, if set only the platform with id "pid" is 
	 * parsed. This parameter is useful to reduce the memory usage for very large files.
	 * @return none
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void stage2(BufferedReader br, BufferedWriter bw, String pid) throws ParseException, IOException {
		/*
		 * Parser state
		 */
		boolean inPlatformTableHeader = false;
		boolean inPlatformTable = false;
		boolean inSampleTableHeader = false;
		boolean inSampleTable = false;
		
		String currentPlatformID = null;
		String currentSampleID = null;	
		
		int currentNSamples = -1;
		int currentSampleIndex = -1;		
		
		/*
		 * Data structures updated in this pass
		 */		
		geneID2Name = new HashMap<String, String>();
		HashMap<String, float[]> nm = new HashMap<String, float[]>();
		geneID2ValsSoftReference = new SoftReference<HashMap<String, float[]>>(nm);
		orderedGeneIDs = new ArrayList<String>();		
		
		/*
		 * Errors and warnings encountered during the parsing (stage 2)
		 */
		// Warning count
		int nameNotFound = 0;
		int idNotFound = 0;
		//public int duplicateNames;
		int sampleNameNotFound = 0;
		int geneNameNotFound = 0;
		int expressionValueNotFound = 0;
		
		/*
		 * Parse file
		 */
		String line;
		while ((line = br.readLine()) != null) {		
			if (line.contains("^PLATFORM")) {
				currentPlatformID = getVal(line);
				if (currentPlatformID == null) {
					throw new ParseException("Invalid ^PLATFORM line: " + line);
				}	
			}
			else if (line.contains("!platform_table_begin")) {				
				// Only parse a specific platform
				if (pid != null) {
					if (! pid.equals(currentPlatformID)) {
						// By not setting inPlatformHeader=true the entire platform table will be skipped
						continue; 
					}
				}

				currentNSamples = getPlatformSampleCount(currentPlatformID);
				if (currentNSamples < 1) {
					throw new ParseException("No samples for platform: " + currentPlatformID);
				}	
				logger.info("Platform " + pid + " has " + currentNSamples + " samples");				

				inPlatformTableHeader = true; // Next line is the header line
				//inPlatformTable will be set to true when header line has been parsed
			}
			else if (line.contains("!platform_table_end")) {
				currentNSamples = -1;					
				currentPlatformID = null;			

				inPlatformTable = false;
				
				logger.info("Platform " + pid + " has " + geneID2Name.size());
			}
			else if (line.contains("^SAMPLE")) {			
				currentSampleID = getVal(line);
				if (currentSampleID == null) {
					throw new ParseException("Invalid ^SAMPLE line: " + line);
				}			
			}
			else if (line.contains("!sample_table_begin")) {             
				currentPlatformID = getPlatformID(currentSampleID);
				// Only parse a specific platform
				if (pid != null) {
					if (! pid.equals(currentPlatformID)) {
						// By not setting inSampleHeaderTable=true the entire sample table will be skipped
						continue;
					}
				}

				currentSampleIndex = getPlatformSampleIndex(currentPlatformID, currentSampleID);
				if (currentSampleIndex < 0) {
					throw new ParseException("Invalid sample index for sample: " + currentSampleID + " in platform: " + currentPlatformID);
				}			
				// System.out.println("sample index = " + currentSampleIndex);

				inSampleTableHeader = true; // The next line contains the sample header columns
				// inSampleTable will be set to true when header lines has been parsed
			}
			else if (line.contains("!sample_table_end")) {						
				currentSampleID = null;
				currentPlatformID = null;			
				currentSampleIndex = -1;

				inSampleTable = false; 	 
			}
			/*
			 * Parse line based on parser state
			 */
			else if (inPlatformTableHeader) {
				inPlatformTableHeader = false; // Parsed header line
				inPlatformTable = true;
			}
			else if (inPlatformTable) {
				String[] parts = line.split("\t");			

				String name = null;			
				int nameIndex = platformGeneNameColumns.get(currentPlatformID);
				// Note! nameIndex can be -1 if gene name column was not found
				if ((nameIndex > 0) && (nameIndex < parts.length)) {		
					name = parts[nameIndex].toUpperCase();
				}
				else {
					logger.debug("Name not found in line: " + line);
					name = "N/A"; // default if none available
					nameNotFound++;
				}

				int idIndex = platformIDColumns.get(currentPlatformID);
				if (idIndex>-1 && idIndex < parts.length) {
					String geneID = parts[idIndex];
					geneID2Name.put(geneID, name);				
					HashMap<String, float[]> g2v = geneID2ValsSoftReference.get();
					if (g2v == null) {
						// Out of memory
						throw new ParseException("Out of memory");
					}
					// Allocate new array and initialize to NaN
					float[] vals = new float[currentNSamples];	
					for (int i = 0; i < currentNSamples; i++) {
						vals[i] = Float.NaN;
					}
					g2v.put(geneID, vals);
					orderedGeneIDs.add(geneID);
				}
				else {
					//throw new ParseException("Row without gene ID: " + line);
					logger.debug("ID not found in line: " + line);
					idNotFound++;
				}	
			}
			else if (inSampleTableHeader) {
				inSampleTableHeader = false; // Done parsing sample table header
				inSampleTable = true; // Sample table values follows
			}		
			else if (inSampleTable) {					
				String[] parts = line.split("\t");
				if (! sampleGeneIDColumns.containsKey(currentSampleID)) {
					throw new ParseException(currentSampleID + " not in sampleGeneIDColumns");
				}
				int idCol = sampleGeneIDColumns.get(currentSampleID);
				if (idCol < 0 || idCol >= parts.length) {
					logger.debug("could not find sample ID column in row: " + line);		
					geneNameNotFound++;
					return;
				}
				String geneID = parts[idCol];

				if (! sampleValueColumns.containsKey(currentSampleID)) {
					throw new ParseException(currentSampleID + " not in sampleValueColumns");
				}
				int valCol = sampleValueColumns.get(currentSampleID);
				if (valCol >= parts.length) {
					logger.debug("could not find expression value column in row: " + line);
					expressionValueNotFound++;
					return;
				} 			

				HashMap<String, float[]> g2v = geneID2ValsSoftReference.get();
				if (g2v == null) {
					// Out of memory
					throw new ParseException("Out of memory");
				}
				
				if (! g2v.containsKey(geneID)) {
					throw new ParseException("No values found for geneID: " + geneID + " (" + g2v.size() + " values in map)");
				}
				float[] curVals = g2v.get(geneID);
							
				if (curVals.length < currentSampleIndex) {
					throw new ParseException("Invalid length of values: " + curVals.length + "(index=" + currentSampleIndex + ")");
				}
				if (! Float.isNaN(curVals[currentSampleIndex])) {				
					throw new ParseException("Duplicate gene IDs: " + geneID + " val: " + curVals[currentSampleIndex] + " and " + parts[valCol]);
				}
				else {		
					try {
						float v = Float.valueOf(parts[valCol]);
						if (zerosAsMissingVals && (v == 0.0)) {
							curVals[currentSampleIndex] = Float.NaN;
						}
						else {
							curVals[currentSampleIndex] = v;
						}
					} catch (NumberFormatException e) {
						// Not a valid floating point so it is kept as missing
						// (not necessary to set the value since it is already NaN)						
					}
				}			

				String name = geneID2Name.get(geneID);
				//if (name != null) {
				//	if (currentGeneName2Val.containsKey(name)) {
				//		logger.debug("duplicate gene name: " + name + " discarding previous value");
				//		duplicateNames++;
				//	}
				//	currentGeneName2Val.put(name, parts[valCol]);
				//}
				//else {
				if (name == null) {
					logger.debug("sample value without a gene name: " + geneID);
					sampleNameNotFound++;
				}				
			} // else if in sample table
		} // while ! EOF
		
		/*
		 * Print stats
		 */
		logger.info("Platfrorm name not found: " + nameNotFound);
		logger.info("ID not found: " + idNotFound);
		//logger.info("Duplicate names: " + duplicateNames);
		logger.info("Sample not found: " + sampleNameNotFound);
		logger.info("Gene name not found: " + geneNameNotFound);
		logger.info("Expression value not found: " + expressionValueNotFound);
		
		if (bw != null) {
			writeOutputLines(bw, pid);
		}
	}

	/**
	 * Helper method to get the index of a sample
	 * 
	 * @param pid platform ID
	 * @param sid sample ID
	 * @return sample index, or -1 if an error occured
	 */
	private int getPlatformSampleIndex(String pid, String sid) {
		ArrayList<String> sampleIDs = platformSampleIDs.get(pid);
		if (sampleIDs == null) {
			return -1;
		}
		return sampleIDs.indexOf(sid);
	}

	/**
	 * Helper method to get the number of samples in a platform
	 * 
	 * @param pid platform ID
	 * @return number of samples for platform, or -1 if an error occured
	 */
	private int getPlatformSampleCount(String pid) {
		ArrayList<String> sampleIDs = platformSampleIDs.get(pid);
		if (sampleIDs == null) {
			return -1;
		}
		else {
			return sampleIDs.size();
		}
	}
	
	/**
	 * Helper method to get the platform ID for a sample
	 * 
	 * @param sid sample ID
	 * @return platform ID for sample, or null if the sample ID was not found
	 */
	private String getPlatformID(String sid) {
		for (String pid: platformSampleIDs.keySet()) {
			if (platformSampleIDs.get(pid).contains(sid)) {
				return pid;
			}
		}
		
		return null;
	}

	/**
	 * Get a list of Platform IDs for the platforms found in the file.
	 * 
	 * @return list of platform IDs
	 */
	public ArrayList<String> getPlatformIDs() {
		return orderedPlatformIDs;
	}
	
	/**
	 * Get dset ID
	 * 
	 * @return dataset ID
	 * @throws ParseException 
	 */
	public String getDsetID() throws ParseException {
		if (dsetID == null) {
			throw new ParseException("Datset ID not found");
		}
		return dsetID;
	}
	
	/**
	 * Get PCL file output lines.
	 * 
	 * Note! The stage 2 data structures must have been initialized by running the stage2()
	 * method before calling this method.
	 * 
	 * @param platforID platform to return PCL lines file for. 
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void writeOutputLines(BufferedWriter os, String platformID) throws ParseException, IOException {
		//ArrayList<String> lines = new ArrayList<String>();

		/*
		 * Error tests
		 */
		int nSamples = orderedSampleIDs.size();
		if (nSamples != sampleTitles.size()) {
			throw new ParseException("Sample title data structures has invalid number of samples: " + sampleTitles.size() + ", should have been: " + nSamples);
		}

		/*
		 * Create indexes for output lines
		 */
		ArrayList<String> sampleIDs = platformSampleIDs.get(platformID);
		if (sampleIDs == null) {
			throw new ParseException("SampleIDs not found for platform: " + platformID);
		}
				
		if (orderedGeneIDs == null) {
			throw new ParseException("Ordered gene IDs not found for platform: " + platformID);
		}
		
		/*
		 * Create header lines
		 */
				
		// Create header line
		String h1 = "ID_REF\tGENE_NAME\tGWEIGHT";
		for (String sampleID: sampleIDs) {
			if (sampleTitles.containsKey(sampleID) == false) {
				throw new ParseException("No sample title for sample: " + sampleID);
			}
			h1 = h1 + "\t" + sampleID + ": " + sampleTitles.get(sampleID); // SampleID is included
		}
		h1 = h1 + "\n";
		//lines.add(h1);
		os.write(h1);
		
		// Create EWEIGHT line
		String h2 = "EWEIGHT\t\t";
		for (int i = 0; i < sampleIDs.size(); i++) {
			h2 = h2 + "\t1";
		}
		h2 = h2 + "\n";
		//lines.add(h2);
		os.write(h2);
		
		/*
		 * Write output lines.
		 * 
		 * The samples are written in the same order as they are read from the file.
		 * This order is maintained by the array list used to store the per-platform
		 * gene IDs
		 */
		for (String id: orderedGeneIDs) {
			String name = geneID2Name.get(id);
			String line = id;
			
			if (name == null) { // Name not set
				name = "NOT FOUND";
			}
			else if (name.isEmpty()) {
				name = "NOT SPECIFIED";
			}			
			line = line + "\t" + name + "\t1"; // name and EWEIGHT
		
			HashMap<String, float[]> g2v = geneID2ValsSoftReference.get();
			if (g2v == null) {
				// Out of memory
				throw new ParseException("Out of memory");
			}
			float[] vals = g2v.get(id);
			if (vals == null) {
				throw new ParseException("No expression values found for sample: " + id);
			}
			
			for (float val: vals) {
				if (Float.isNaN(val)) { // missing value
					line = line + "\t";
				}
				else {
					line = line + "\t" + String.valueOf(val);
				}
			}
			line = line + "\n";
			//lines.add(line);
			os.write(line);
		}
					
		//return lines;
	}
	
	/**
	 * Parse a line of the form "key = val" and return key.
	 * 
	 * @param line of the form "key = val"
	 * @return key part in "key = val" line, or null if line could not be parsed. Key is
	 * also trimmed before it is returned.
	 */
	public static String getKey(String line) {
		String[] parts = line.split("=");
		if (parts.length != 2) {
			return null;
		}
		
		String key = parts[0];
		return key.trim();
	}

	/**
	 * Parse a line of the form "key = val" and return val.
	 * 
	 * @param line of the form "key = val"
	 * @return value part in "key = val" line, or null if line could not be parsed. Val is
	 * also trimmed before it is returned.
	 */
	public static String getVal(String line) {
		String[] parts = line.split("=");
		if (parts.length != 2) {
			return null;
		}
		
		String val = parts[1];
		return val.trim();
	}
	
	/**
	 * Test if the given string is a valid number
	 * 
	 * @param x string to test
	 * @return true if a valid floating point number, false otherwise
	 */
	public static boolean isNumeric(String x) {
		try {
			Float.valueOf(x);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}		
	}
		
	/**
	 * Convert a Series SOFT file to the PCL format
	 * 
	 * @param inputFilename input file
	 * @param outputDir output directory	
	 * @return none
	 * @throws IOException
	 * @throws ParseException 
	 */	
	public void convert(String inputFilename, String outputDir) throws IOException, ParseException {
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));						
		// do not write stage results
		stage1(ins, null);			
		ins.close();		
		System.out.println("Stage 1 done");
	
		String dsetID = FilenameUtils.getDsetID(inputFilename);
		ArrayList<String> platformIDs = getPlatformIDs();
		System.out.println("Run computation for N platforms: " + platformIDs.size());
		for (String pid: platformIDs) {								
			ins = new BufferedReader(new FileReader(inputFilename));			
			stage2(ins, null, pid); // do not write output file
			ins.close();			
			System.out.println("Stage 2 done for platform: " + pid);		
			
			String outputFilename = OsPath.join(outputDir, dsetID + "-" + pid + ".pcl");
			BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename)); 
			writeOutputLines(os, pid);
			os.close();
			
			System.out.println("Stage 2 write done");
		}
	}

	/**
	 * Convert a GEO series SOFT file to the PCL format. 
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft)
	 *  1: output directory
	 *  3: log4j.properties file
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {
		
		if (argv.length < 3) {
			System.err.println("Usage: java GeoGSE2Pcl inputFilename outputDir log4j.properties [serFilename]");
			System.exit(-1);
		}
		PropertyConfigurator.configure(argv[2]);
		Logger logger = Logger.getLogger("parse");
		GeoGSE2Pcl gse2pcl = new GeoGSE2Pcl(logger);		
		gse2pcl.convert(argv[0], argv[1]);		
	}
}
