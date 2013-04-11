package edu.princeton.function.troilkatt.sge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSE2Pcl;
import edu.princeton.function.troilkatt.tools.ParseException;

public class GeoGSE2Pcl2 extends GeoGSE2Pcl {
	
	/**
	 * Constructor 
	 */
	public GeoGSE2Pcl2(Logger logger) {
		super(logger);
	}
	
	public boolean convert(String inputFilename, String outputDir, String serDir) {
		String serFilename = OsPath.join(serDir, FilenameUtils.getDsetID(inputFilename, false)) + ".ser";
		String dsetID = FilenameUtils.getDsetID(inputFilename);
		String platformID = FilenameUtils.getPlatID(inputFilename);
		String outputFilename = dsetID + ".pcl";
		
		FileInputStream ser = null;
		try {
			ser = new FileInputStream(serFilename);
			readStage1Results(ser);
			ser.close();
		} catch (ClassNotFoundException e) {
			System.err.println("Could not read stage 1 results from file: " + serFilename);
			return false;
		} catch (FileNotFoundException e) {
			System.err.println("File not found: " + serFilename);
			e.printStackTrace();
			try {
				if (ser != null) {
					ser.close();
				}
			} catch (IOException e2) {
				// do nothing
			} 
			return false;
		} catch (IOException e) {
			System.err.println("IOException when reading ser file");
			e.printStackTrace();
			try {
				if (ser != null) {
					ser.close();
				}
			} catch (IOException e2) {
				// do nothing
			} 
			return false;
		} 
			
		BufferedReader br = null;			
		BufferedWriter bw = null;
		try {
			// Open input and output file
			br = new BufferedReader(new FileReader(inputFilename));			
			bw = new BufferedWriter(new FileWriter(outputFilename));
			
			// parse and write output file
			stage2(br, bw, platformID); 
			
			br.close();
			bw.close();
		} catch (FileNotFoundException e) {
			System.err.println("File not found exception");
			e.printStackTrace();
			try {
				if (br != null) {
					br.close();
				}
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e2) {
				// do nothing
			} 
			return false;
		} catch (IOException e) {
			System.err.println("IOException when converting file");
			e.printStackTrace();
			try {
				if (br != null) {
					br.close();
				}
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e2) {
				// do nothing
			} 
			return false;
		} catch (ParseException e) {
			System.err.println("ParseException when converting file");
			e.printStackTrace();
			try {
				if (br != null) {
					br.close();
				}
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e2) {
				// do nothing
			} 
			return false;
		}			
		
		return true;
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
		
		if (argv.length < 4) {
			System.err.println("Usage: java GeoGSE2Pcl inputFilename outputDir serDir log4j.properties");
			System.exit(-1);
		}
		PropertyConfigurator.configure(argv[4]);
		Logger logger = Logger.getLogger("soft2pcl");
		GeoGSE2Pcl2 gse2pcl = new GeoGSE2Pcl2(logger);
		gse2pcl.convert(argv[0], argv[1], argv[2]);		
	}

}
