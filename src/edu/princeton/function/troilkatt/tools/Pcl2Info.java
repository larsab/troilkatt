package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


/**
 * Calculate various statistics for a PCL file. These are used later by the processing 
 * pipeline.
 */
public class Pcl2Info {
	// Parser state
	protected boolean headerLineRead;
	protected boolean eWeightLineRead;
		
	protected double min;
	protected double max;
	protected double mean;
	protected long numPos;
	protected long numNeg;
	protected long numZero;
	protected long numMissing;
	protected long numTotal;
	
	/**
	 * Constructor
	 */
	public Pcl2Info() {
		headerLineRead = false;
		eWeightLineRead = false;		
		initValues();
	}
	
	/**
	 * Reset parse. This function must be called if the same parser instance is
	 * used to parse multiple files.
	 */
	public void reset() {
		headerLineRead = false;
		eWeightLineRead = false;		
		initValues();
	}
	
	/**
	 * Helper function to initialized the calculated map 
	 */
	private void initValues() {
		min = 1e10;
		max = -1e10;
		mean = 0.0;
		numPos = 0;
		numNeg = 0;
		numZero = 0;
		numMissing = 0;
		numTotal = 0;		
	}

	/**
	 * Calculate fields for a PCL file
	 * 
	 * @param ins input file
	 * @return hash map with calculated values
	 * @throws IOException
	 * @throws ParseException 
	 */
	public HashMap<String, String> calculate(BufferedReader ins) throws IOException, ParseException {
		String line;
		
		while ((line = ins.readLine()) != null) {
			calculateLine(line);			
		} 
		
		return getResults();
	}
	
	/**
	 * This function is called by the driver class for each line in the file.
	 * 
	 * Note! The parser relies on global state, so it is assumed that this function
	 * is called subsequently for the lines in the file.
	 * 
	 * @param line line to parse
	 * @return none
	 */
	public void calculateLine(String line) {
		
		// Handle first two lines in the file
		if (! headerLineRead) {
			headerLineRead = true;
			return;
		}
		if (! eWeightLineRead) {
			eWeightLineRead = true;
			return;
		}
		
		line = line + "\n";
		String[] parts = line.split("\t");
		parts[parts.length - 1] = parts[parts.length - 1].trim(); 

		// 3 first columns are gene name, gene name, and EWEIGHT
		for (int i = 3; i < parts.length; i++) {
			if (! isNumeric(parts[i])) {
				numMissing += 1;					
			}
			else {
				numTotal += 1;							
				Double val = Double.valueOf(parts[i]);
				mean += val;
				if (val > max) {
					max = val;
				}
				if (val < min) {
					min = val;
				}
				if (val > 0) {
					numPos += 1;
				}
				else if (val < 0) {
					numNeg += 1;
				}
				else {
					numZero += 1;
				}
			}
		}
	}	
	
	/**
	 * Return a hash map of calculated values.
	 * @return
	 */
	public HashMap<String, String> getResults() {
		HashMap<String, String> results = new HashMap<String, String>();
		
		int tested_numChans = 2;
		int tested_logXformed = 1;
		int tested_zerosMVs = 0;
		String tested_MVcutoff = "NA";

		if (numTotal != 0) {
			mean /= numTotal;
		}
		else {
			mean = 0;
		}

		if ((numNeg == 0) || ((numPos / numNeg) > 7.5)) {
			tested_numChans = 1;
		}
		if (max > 100) {
			tested_logXformed = 0;
		}
		if (numZero > (5 * numMissing)) {
			tested_zerosMVs = 1;
		}
		if (tested_numChans == 1) {
			if (tested_logXformed == 1) {
				tested_MVcutoff = "0";
			}
			else if (min > -500) {
				tested_MVcutoff = "2";
			}
		}
		
		results.put("min", String.valueOf(min));
		results.put("max", String.valueOf(max));
		results.put("mean", String.valueOf(mean));
		results.put("numNeg", String.valueOf(numNeg));
		results.put("numPos", String.valueOf(numPos));		
		results.put("numZero", String.valueOf(numZero)); 
		results.put("numMissing", String.valueOf(numMissing));
		results.put("numTotal", String.valueOf(numTotal));
		results.put("channels", String.valueOf(tested_numChans));
		results.put("logged", String.valueOf(tested_logXformed));
		results.put("zerosAreMVs", String.valueOf(tested_zerosMVs));
		results.put("cutoff", tested_MVcutoff);
		
		return results;
	}

	/**
	 * Test if the given string is a valid number
	 * 
	 * @param x: string to test
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
	 * Print "info" file content, which was the file format used for the old
	 * pipelines.
	 * 
	 * @param pclFilename input pcl filename
	 */
	public void printInfoFile(String pclFilename) {
		System.out.println("File\tDatasetID\tOrganism\tPlatform\tValueType\t#channels\tTitle\tDesctiption\tPubMedID\t" +
				"#features\t#samples\tdate\tMin\tMax\tMean\t#Neg\t#Pos\t#Zero\t#MV\t#Total\t#Channels\t" +
				"logged\tzerosAreMVs\tMVcutoff");
		String dset = FilenameUtils.getDsetID(pclFilename);
		System.out.print(dset);
		System.out.print("\t" + dset + "\tN/A\tN/A\tN/A\tN/A\tN/A");
		System.out.print("\tN/A\tN/A\tN/A\tN/A\tN/A");
		System.out.print("\t" + min + "\t" + max + "\t" + mean + "\t" + numNeg + "\t" + numPos + "\t");
		HashMap<String, String> res = getResults();
		System.out.print(numZero + "\t" + numMissing + "\t" + numTotal + "\t" + res.get("channels") + "\t");
		System.out.println(res.get("logged") + "\t" + res.get("zerosAreMVs") + "\t" + res.get("cutoff"));		
	}
	
	/**
	 * Extract meta data from a SOFT file and print these on stdout
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft)	 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {
		
		if (argv.length < 1) {
			System.err.println("Usage: java Pcl2Info inputFilename");
			System.exit(2);
		}
	
		BufferedReader ins = new BufferedReader(new FileReader(argv[0]));
		Pcl2Info converter = new Pcl2Info();
		converter.calculate(ins);
		ins.close();
		converter.printInfoFile(argv[0]);
	}
}
