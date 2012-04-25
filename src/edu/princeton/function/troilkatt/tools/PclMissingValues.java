package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Missing value calculation for PCL files
 * 
 */
public class PclMissingValues {
	// genes with # missing values > (# expression values / GENE_CUTOFF_FACTOR) are discarded
	public float geneCutoffFactor;
	// datasets with #missing values > (# expression values / DATASET_CUTOFF_FACTOR) are discarded
	public float datasetCutoffFactor;
	// datasets with less than SAMPLE_CUTOFF_FACTOR are discarded
	public int sampleCutoffFactor;

	// Are zero values treated as missing?
	public boolean zerosAsMissingValues;
	// Values less than the cutoff are set as missing
	public float missingValueCutoff;
	
	/*
	 * Global parser state
	 */
	// Set to true when header line is parsed (and the below nCols and maxAllovedMissingGenes
	// are initialized)
    protected boolean headerRead;
    // Set to true when the EWEIGHT line has been read
    protected boolean eweightRead;
	// Number of columns per row (set after parsing first line)
	protected int nCols;            
	// Maximum number of values that can be missing in a row
	// This value depends on the number of columns in the input file and the geneCutoffValue
	protected int maxAllowedMissingGenes;
	// Count of missing values in included rows (i.e. deleted genes/rows are not considered)
	protected int missingTotal;   
	// Count of of expression values in included rows
	protected int totalValues;    
	
	/**
	 * Constructor. 
	 * 
	 * @param inputFilename PCL file to convert
	 * @param infoFilename meta data file with information about the dataset to convert
	 * @param geneCutoffFactor maximum allowed missing values per gene (in percentage of total values). Genes
	 * with more missing values are discarded.
	 * @param sampleCutoff minimum samples requires for a dataset. Datasets with fewer samples are
	 * discarded.
	 * @param datasetCutoff maximum allowed missing values per dataset (in percentage of total values). Genes
	 * with more missing values are discarded.
	 * @param zeroAsMissing should zeros be treated as missing values.
	 * @param mvCutoff values less than this are set as missing
	 * @throws IOException if .info file cannot be read.
	 */
	public PclMissingValues(float geneCutoff, int sampleCutoff, float datasetCutoff,
			boolean zeroAsMissing, float mvCutoff) throws IOException {
		this.geneCutoffFactor = geneCutoff;
		this.sampleCutoffFactor = sampleCutoff;
		this.datasetCutoffFactor = datasetCutoff;
		this.zerosAsMissingValues = zeroAsMissing;
		this.missingValueCutoff = mvCutoff;
		
		System.out.println("Discard genes with more than " + geneCutoff + "% missing values.");
		System.out.println("Discard datasets with more than " + datasetCutoff + "% missing values.");
		System.out.println("Discard datasets with less than " + sampleCutoff + " samples.");
		
		maxAllowedMissingGenes = -1; // Maximum number of values that can be missing in a row		
		nCols = -1;                  // number of columns per row (set after parsing first line)		
		missingTotal = 0;   // number of missing values in included rows
		totalValues = 0;    // number of expression values in included rows
		headerRead = false;
		eweightRead = false;
	}
	
	/**
	 * Do missing value calculation.
	 * 
	 * @param inputFilename PCL file to do missing value estimation
	 * @param outputFilename output file
	 * @return true if an output file was created, and false if there were insuffucient rows
	 * or columns.
	 */
	public void process(String inputFilename, String outputFilename) throws IOException {
		int rowsRead = 0;
		int rowsWritten = 0;
		int rowsDiscarded = 0; 
		BufferedReader ib = new BufferedReader(new FileReader(inputFilename));

		// Write data first to temporary file in case the data needs to be deleted
		String tmpFilename = inputFilename + ".tmp";
		PrintWriter os = new PrintWriter(new FileWriter(tmpFilename));
		
		String line;				
		while ((line = ib.readLine()) != null) {
			rowsRead++;
			String outputLine = insertMissingValues(line);
			if (outputLine != null) {
				os.write(outputLine);
				rowsWritten++;
			}
			else {
				rowsDiscarded++;
			}
			
		} // while more lines
		
		os.close();
		ib.close();
		
		System.out.println("Rows read:      " + rowsRead);
		System.out.println("Rows written:   " + rowsWritten);
		System.out.println("Rows discarded: " + rowsDiscarded);
		
		if (tooManyMissingValues()) {
			System.err.printf("Discarding dataset: too many missing values: %d of %d\n",
					missingTotal, totalValues);
			OsPath.delete(tmpFilename);
		}
		else if (rowsWritten <= 2) {
			System.err.println("Discarding dataset: too few output rows:" + rowsWritten);
		}
		else {
			System.err.printf("Discarded %d of %d genes with too many missing values\n",
					rowsDiscarded, rowsRead);
			System.err.printf("Of remaining, %d of %d values are missing\n", missingTotal, totalValues);
			if (OsPath.rename(tmpFilename, outputFilename) == false) {
				throw new IOException("Could not rename tmp file to output filename: " + outputFilename);
			}
		}
	}
	
	/**
	 * Do missing value esitmation from a line (row) read from the input file
	 * 
	 * @param line read from input file
	 * @return output line, or null if the row had too many missing values or too few columns
	 */
	public String insertMissingValues(String line) {
		line = line + "\n";
		if (headerRead == false) { // First line: header
			/*
			 * Set global variables that depends on the number of samples in the file
			 */
			nCols = line.split("\t").length;	
			maxAllowedMissingGenes = (int) Math.ceil( (nCols - 3) * (geneCutoffFactor / 100));
			// Make sure there are enough samples
			if (nCols < sampleCutoffFactor) {
				System.err.printf("Too few samples in dataset: %d (%d required)\n", nCols, sampleCutoffFactor);
			}
			headerRead = true;
			return line;
		}			
		else if (eweightRead == false) { // Second line: eweight
			eweightRead = true;
			return line;
		}
		else { // Data line 
			/*
			 * Replace missing values
			 */
			String[] cols = line.trim().split("\t");
			cols[cols.length - 1] = cols[cols.length - 1].trim();
			// The three first columns are headers
			if (cols.length < 3) {
				System.err.println("Warning: invalid PCL table row: " + line);
				return null;
			}
		
			int missingInRow = 0;
			String outputLine = cols[0] + "\t" + cols[1] + "\t" + cols[2];
			for (int i = 3; i < cols.length; i++) {
				try {
					Float val = Float.valueOf(cols[i]);
					if (zerosAsMissingValues && (val == 0)) {
						missingInRow++;							
						outputLine = outputLine + "\t";
					}										
					else if ((Float.isNaN(missingValueCutoff)) || (val >= missingValueCutoff)) {
						outputLine = outputLine + "\t" + val;							
					}
					else {
						missingInRow++;							
						outputLine = outputLine + "\t";
					}
				} catch (NumberFormatException e) {
					missingInRow++;						
					outputLine = outputLine + "\t";
				}
			}
			
			// Set non-existing columns to missing
			for (int i = cols.length; i < nCols; i++) {
				missingInRow++;							
				outputLine = outputLine + "\t";
			}
			
			// Discard lines with too many missing values
			if (missingInRow >= maxAllowedMissingGenes) { 
				// More than 50% of values in row are missing: discard line
				return null;
			}
			else {
				// Do not include discarded rows in the missing count
				missingTotal += missingInRow;
				totalValues += nCols - 3;
				return outputLine + "\n";
			}	
		}	
	}
	
	/**
	 * Calculate whether the output file has too many missing values.
	 * 
	 * No parameter since it uses statistics calculated during the missing value replacement.
	 */
	public boolean tooManyMissingValues() {
		int maxAllowedMissingInDataset = (int) Math.ceil(totalValues * (datasetCutoffFactor / 100));
		System.out.println("Total values: " + totalValues);
		System.out.println("Dataset cutoff factor: " + datasetCutoffFactor);
		System.out.println("Missing total: " + missingTotal);
		System.out.println("Max allowed missing in datasets: " + maxAllowedMissingInDataset);
		if (missingTotal >= maxAllowedMissingInDataset) {			
			return true;
		}
		else {
			return false;
		}
	}
	
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
		if (args.length != 4) {
			System.err.println("Usage: inputPclFilename outputPclFilename zeroAsMissing MissingValCutoff");
			System.exit(-1);
		}
		String inputFilename = args[0];
		String outputFilename = args[1];
		if (OsPath.isfile(outputFilename)) {
			System.err.println("Warning: Deleting previuosly created outputfile: " + outputFilename);
			OsPath.delete(outputFilename);
		}
		
		float geneCutoff = 50;
		int sampleCutoff = 3;
		float datasetCutoff = 50;
		boolean zeroAsMissing = args[2].equals("true");
		
		float mvCutoff = Float.NaN;
		if (! args[3].equals("NaN")) {			
			mvCutoff = Float.valueOf(args[3]);
		}
		
		PclMissingValues converter = new PclMissingValues(geneCutoff, sampleCutoff, 
				datasetCutoff, zeroAsMissing, mvCutoff);
		converter.process(inputFilename, outputFilename);
	}
}
