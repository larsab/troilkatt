package edu.princeton.function.troilkatt.sge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSE2Pcl;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Split an GEO series (GSEXXX.soft) into platform specific soft files 
 */
public class GeoGSESplit extends GeoGSE2Pcl {
	
	/**
	 * Constructor 
	 */
	public GeoGSESplit(Logger logger) {
		super(logger);
	}
	
	/**
	 * Parse input GSE file and write out data structure to be used by stage 2 in a ser file.
	 * Also write platform specific soft files to the output directory
	 * 
	 * @param inputFilename GSE filename
	 * @param outputDir directory where platform specific output files are written
	 * @param serDir directory where ser files are written. We assume this is on the global
	 * file system.
	 * @return none
	 */
	public void process(String inputFilename, String outputDir, String serDir) {
		// Create a file in the global ser directory with a .ser extension
		BufferedReader ins = null;
		try {
			String serFilename = OsPath.join(serDir, FilenameUtils.getDsetID(inputFilename, false)) + ".ser";
			FileOutputStream ser = new FileOutputStream(serFilename);	
			/*
			 * Parse input file and save "ser" file
			 */
			ins = new BufferedReader(new FileReader(inputFilename));								
			stage1(ins, ser);			
			ins.close();		
			System.out.println("Stage 1 done");
		} catch (IOException e) {
			System.err.println("Could not parse GSE file, I/O excpetion:" + e);
			if (ins != null) {
				try {
					ins.close();
				} catch (IOException e1) {
					// ignore
				}
			}			
			return;
		} catch (ParseException e) {
			System.err.println("Could not parse GSE file, parse excpetion:" + e);
			if (ins != null) {
				try {
					ins.close();
				} catch (IOException e1) {
					// ignore
				}
			}			
			return;
		}
		
		/*
		 * Write platform specific files
		 */
		ArrayList<String> pids = getPlatformIDs();		
		for (String pid: pids) {
			// Make sure that the same platform IDs are used in case the input file specified 
			// a platform. Otherwise the ser filename may be wrong
			if (FilenameUtils.hasPlatID(inputFilename)) {
				String platID = FilenameUtils.getPlatID(inputFilename);
				if (! platID.equals(pid)) {					
					// platform IDs do not match
					System.err.println("Platform IDs do not match: input platform: " + platID + ", split platform: " + pid);					
					continue;
				}
			}
			
			String outputBasename = FilenameUtils.mergeDsetPlatIDs(dsetID, pid) + ".soft";
			String outputFilename = OsPath.join(outputDir, outputBasename);
			
			BufferedReader br = null;			
			BufferedWriter bw = null;
			try {
				// Re-open input file
				br = new BufferedReader(new FileReader(inputFilename));
				// and open a new output file				
				bw = new BufferedWriter(new FileWriter(outputFilename));							
				writeSoftPerPlatform(br, bw, pid);	
				bw.close();
				System.out.println("Outputfile written: " + outputFilename);
			} catch (IOException e) {
				System.err.println("Could not write output files, I/O excpetion:" + e);				
				try {
					bw.close();
					OsPath.delete(outputFilename);
				} catch (IOException e2) {
					// Do nothing
				}
			} catch (ParseException e) {
				System.err.println("Could not write output files, parse error:" + e);
				try {
					bw.close();
					OsPath.delete(outputFilename);
				} catch (IOException e2) {
					// Do nothing
				}
			} finally {
				try {					
					br.close();
				} catch (IOException e) {
					// ignore	
				}
			}
		} // for each platform
	}

	/**
	 * Convert a GEO series SOFT file to the PCL format. 
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft)
	 *  1: output directory
	 *  2: ser directory on global filesystem
	 *  3: log4j.properties file
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {
		
		if (argv.length < 4) {
			System.err.println("Usage: java GeoGSE2Pcl inputFilename outputDir serDir log4j.properties");
			System.exit(-1);
		}
		PropertyConfigurator.configure(argv[3]);
		Logger logger = Logger.getLogger("geoSplit");
		GeoGSESplit gse2pcl = new GeoGSESplit(logger);
		gse2pcl.process(argv[0], argv[1], argv[2]);		
	}

}
