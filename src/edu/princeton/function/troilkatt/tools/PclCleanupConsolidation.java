package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Merge of collectFinalData.rb and DivLogNorm.java
 */
public class PclCleanupConsolidation {	
	public class Gene  {
		public String 	orf;
		public String 	name;
	    public double[] exprLvls;
	    public int		id;
	    //public ArrayList	utilArray;

	    public Gene (String _orf, String _name) {
	    	orf = _orf;
	    	name = _name;
	    	//utilArray = new ArrayList();
	    }   
	}

	// Argument: True if file has been log transformed 
	protected boolean logTransformed;

	/*
	 * DivLogNorm variables
	 */
	public ArrayList<Gene> genes;
	public ArrayList<String> exprNames;
	public int numExprs;
	
	/**
	 * Constructor
	 * 
	 * @param lt true if file has been log transformed
	 */
	public PclCleanupConsolidation(boolean lt) {
		logTransformed = lt;
				
		genes = new ArrayList<Gene>();
		exprNames = new ArrayList<String>();
		numExprs = 0;
	}
	
	/** 
	 *  Clears out all data, and reads in new data in a pcl file from the
	 *  buffered reader provided
	 *  
	 *  @parm fin input file
	 *  @return true if file was read successfully, false otherwise
	 */
	public boolean readDataFromPCL(BufferedReader fin) throws IOException {
		// Clear out all arrays and re-initialize
		genes.clear();
		numExprs = 0;
		int geneID = 0;

		// Read in the data from the input file
		
		int row = 0;
		int gweightCol = -1;
		int colOffset = 2;

		//While there's more data to read in...
		String line = fin.readLine();
		while ((line != null) && (!line.equals(""))) {
			boolean processRow = true;

			//If this is the first row, this determines number of experiments
			if (row == 0) {
				String[] names = line.split("\t");
				for (int i = 2; i < names.length; i++) {
					//If there is a GWEIGHT column, make a note
					if (names[i].equalsIgnoreCase("GWEIGHT")) {
						gweightCol = i;
						colOffset = 3;
					}
					//Otherwise, record the experiment name
					else {
						exprNames.add(names[i]);
						numExprs++;
					}
				}
				if (names.length < 4) {
					return false;
				}
				processRow = false;
			}

			//If this is the second row, it may contain weights, or it may
			//contain data
			else if (row == 1) {
				String[] lineParts = line.split("\t");

				//If the first column is EWEIGHT, then just ignore this row
				if (lineParts[0].equalsIgnoreCase("EWEIGHT")) {
					processRow = false;
				}
			}

			//If the line is up for processing...
			if (processRow) {
				String[] lineParts = line.split("\t");

				if (lineParts.length > 1) {
					//Columns 0 and 1 are the SpotIDs and Names
					Gene gene = new Gene(lineParts[0].toUpperCase(), lineParts[1].toUpperCase());
					gene.id = geneID++;
					gene.exprLvls = new double[numExprs];
					genes.add(gene);

					//Columns from 2 to the end are data points (except the
					// gweightCol)
					for (int i = 2; i < lineParts.length; i++) {
						if (i != gweightCol) {
							//Try to convert the string to a number, if you
							// fail, just fill in with NaNs;
							try {
								double val = Double.parseDouble(lineParts[i]);
								if (!Double.isNaN(val)) {
									gene.exprLvls[i - colOffset] = val;
								}
								else {
									gene.exprLvls[i - colOffset] = Double.NaN;
								}

							}
							catch (NumberFormatException except) {
								gene.exprLvls[i - colOffset] = Float.NaN;
							}
						}
					}
					//If there are remaining holes, they should be NaNs
					for (int i=(lineParts.length - colOffset); i<numExprs; i++) {
						gene.exprLvls[i] = Float.NaN;
					}
				}
			}

			//Increment row number and repeat
			row++;
			line = fin.readLine();
		}	
		
		//Finally, return success
		return true;
	}

	/**
	 * Divide gene expression values by per gene medians
	 */
	public void divideAllGenesByIndividualMedians() {
		for (int i=0; i<genes.size(); i++) {
			Gene g = (Gene) genes.get(i);
			ArrayList<Double> exprs = new ArrayList<Double>();
			for (int j=0; j<g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j]))
					exprs.add(g.exprLvls[j]);
			}
			Double[] expArr = new Double [exprs.size()];
			expArr = (Double[])exprs.toArray(expArr);
			Arrays.sort(expArr);
			
			double median = expArr[(int)(expArr.length/2.0)];
			
			for (int j=0; j<g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j]))
					g.exprLvls[j] /= median;
			}
		}
	}

	/**
	 * Log transform
	 */
	public void logTransformAll() {
		for (int i = 0; i < genes.size(); i++) {
			Gene g = genes.get(i);

			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					if (g.exprLvls[j] < 0)
						g.exprLvls[j] = -Math.log(-g.exprLvls[j]) / Math.log(2);
					else if (g.exprLvls[j] > 0)
						g.exprLvls[j] = Math.log(g.exprLvls[j]) / Math.log(2);
					else { //exactly zero entries
						g.exprLvls[j] = Math.log(0.001) / Math.log(2);
					}
				}
			}
		}
	}

	/**
	 * Normalize 
	 */
	public void normalizeAllGenes() {
		for (int i = 0; i < genes.size(); i++) {
			Gene g = (Gene) genes.get(i);
			//Find the mean and standard deviation
			double mean = 0;
			double stdDev = 0;
			int count = 0;
			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					mean += g.exprLvls[j];
					count++;
				}
			}
			if (count > 0) mean /= (float)count;
			
			count = 0;
			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					stdDev += (g.exprLvls[j] - mean) * (g.exprLvls[j] - mean);
					count++;
				}
			}
			if (count > 0) stdDev = Math.sqrt(stdDev / (float)count);

			//Mean shift to 0, make deviaiton 1
			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					g.exprLvls[j] -= mean;
					if (stdDev > 0) {
						g.exprLvls[j] /= stdDev;
					}
				}
			}
		}
	}

	/**
	 * Write transformed data to stdout
	 */
	public void writeNewPclToStdOut() {
		System.out.print("YORF\tNAME\tGWEIGHT");
		for (int i = 0; i < exprNames.size(); i++) {
			System.out.print("\t" + (String) exprNames.get(i));
		}
		System.out.println();
		
		System.out.print("EWEIGHT\t\t");
		for (int i = 0; i < numExprs; i++) {
			System.out.print("\t1");
		}
		System.out.println();

		for (int i = 0; i < genes.size(); i++) {
			Gene g = (Gene) genes.get(i);
			System.out.print(g.orf + "\t" + g.name + "\t1");
			DecimalFormat threePlaces = new DecimalFormat("0.000");
			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					System.out.print("\t"+threePlaces.format(g.exprLvls[j]));
					//System.out.print("\t" + g.exprLvls[j]);
				}
				else {
					System.out.print("\t");
				}
			}
			System.out.println();
		}
	}
	
	/**
	 * Write output data to a file
	 * 
	 * @param bw output file
	 * @throws IOException 
	 */
	public void writeNewPclToFile(BufferedWriter bw) throws IOException {
		bw.write("YORF\tNAME\tGWEIGHT");
		for (int i = 0; i < exprNames.size(); i++) {
			bw.write("\t" + (String) exprNames.get(i));
		}
		bw.write("\n");
		
		bw.write("EWEIGHT\t\t");
		for (int i = 0; i < numExprs; i++) {
			bw.write("\t1");
		}
		bw.write("\n");

		for (int i = 0; i < genes.size(); i++) {
			Gene g = (Gene) genes.get(i);
			bw.write(g.orf + "\t" + g.name + "\t1");
			DecimalFormat threePlaces = new DecimalFormat("0.000");
			for (int j = 0; j < g.exprLvls.length; j++) {
				if (!Double.isNaN(g.exprLvls[j])) {
					bw.write("\t"+threePlaces.format(g.exprLvls[j]));
					//bw.write("\t" + g.exprLvls[j]);
				}
				else {
					bw.write("\t");
				}
			}
			bw.write("\n");
		}
	}
	
	/**
	 * Main function for "DivLogNorm"
	 * 
	 * @param br input file
	 * @param bw output file
	 * @param divide true if all genes should be divided by per gene median
	 * @param logT true if the input data should be log transformed
	 * @param normalize true if the input data should be normalized
	 * @return true on success
	 * @throws IOException 
	 */
	public boolean divLogNorm(BufferedReader br, BufferedWriter bw, 
			boolean divide, boolean logTr, boolean normalize) throws IOException {
	
		if (readDataFromPCL(br) == false) {
			System.err.println("Could not read input file");
			return false;
		}
		
		if (divide) {
			divideAllGenesByIndividualMedians();
		}
		if (logTr) {
			logTransformAll();
		}
		if (normalize) {
			normalizeAllGenes();
		}
	
		writeNewPclToFile(bw);
		
		return true;	
	}

	/**
	 * Process file
	 * @throws IOException 
	 */
	public void process(BufferedReader ins, BufferedWriter os) throws IOException {
		if (! logTransformed) {
			// Log transform, but do not divide by median nor normalize			
			divLogNorm(ins, os, false, true, false);
		}
		else {
			// just copy the file
			String line;
			while ((line = ins.readLine()) != null) {
				os.write(line + "\n");
			}
		}
	}
	
	/**
	 * Arguments: see documentation for run
	 * 0 - pcl input file"
	 * 1 - pcl output file	
	 * 2 - 1 if file has been logTransformed, 0 if not
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {	
		if (args.length != 3) {
			System.err.println("Invalid arguments, usage: input.file output.file logTransformed");
			System.exit(2);
		}
		String inputFilename = args[0];
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		String outputFilename = args[1];
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));
		boolean lt = args[2].equals("true");
		
		PclCleanupConsolidation cc = new PclCleanupConsolidation(lt);
		// Divide-by-median and normalize are always false
		cc.process(br, bw);
		
		br.close();
		bw.close();
	}
}





