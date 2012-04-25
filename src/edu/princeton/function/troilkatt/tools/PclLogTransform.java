package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Log transform a PCL file
 */
public class PclLogTransform {
	/**
	 * Do the log transform
	 * @throws IOException 
	 */
	static public void process(BufferedReader br, BufferedWriter bw) throws IOException {
		int lineCnt = 0;
		String line;
		while ((line = br.readLine()) != null) {
			lineCnt++;
			line = line + "\n";
			
			if (lineCnt < 3) {
				// Header or weight line
				bw.write(line);
				continue;
			}
			
			String[] cols = line.split("\t");
			if (cols.length < 3) {
				System.err.println("Too few columns in row: " + line);
				continue;
			}
			
			cols[cols.length - 1] = cols[cols.length - 1].trim(); 
			bw.write(cols[0] + "\t" + cols[1] + "\t" + cols[2]);
			for (int i = 3; i < cols.length; i++) {
				bw.write("\t");
				try {
					float val = Float.valueOf(cols[i]);
					bw.write(String.valueOf(Math.log(val)));
				} catch (NumberFormatException e) {
					// Blanks are missing values
				}
			}
			bw.write("\n");			
		}
	}
	
	/**
	 * @param args command line arguments
	 * [0] input pcl file
	 * [1] output pcl file
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		/*
		 * Parse arguments
		 */
		if (args.length != 2) {
			System.err.println("Usage: inputPclFilename outputPclFilename");
			System.exit(-1);
		}
		String inputFilename = args[0];
		String outputFilename = args[1];
		if (OsPath.isfile(outputFilename)) {
			System.err.println("Warning: Deleting previuosly created outputfile: " + outputFilename);
			OsPath.delete(outputFilename);
		}
		
		/*
		 * Process
		 */		
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));
		
		PclLogTransform.process(br, bw);
		
		br.close();
		bw.close();
	}

}
