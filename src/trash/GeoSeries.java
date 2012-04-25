package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;

/**
 * GEO series file tools.
 *
 */
@Deprecated
public class GeoSeries {
	public String tagOrg = "!Platform_organism = ";
	public String tagPlat = "!Series_platform_id = ";
	public String tagDset = "!Series_geo_accession = ";
	public String tagTitle = "!Series_title = ";
	public String tagDesc = "!Series_summary = "; // may be multiple
	public String tagPmid = "!Series_pubmed_id = ";
	public String tagFeatCount = "!Platform_data_row_count = ";
	public String tagChanCount = "!Sample_channel_count = "; // may be multiple	
	//public String tagValType = "!dataset_value_type = "; NOT IN GSE FILES
	public String tagModDate = "!Series_last_update_date = ";
	
	public String tagTableStart = "!sample_table_begin";
	public String tagTableEnd = "!sample_table_end";
	
	public void convertSoft2Pcl(String softFilename, String pclFilename, String infoFilename) {
		HashMap<String, String>labels = new HashMap<String, String>();
		boolean inDataSection = false;
		boolean atLabels = false;

		double min = 1e10;
		double max = -1e10;
		double mean = 0.0;
		double numPos = 0.0;
		double numNeg = 0.0;
		double numZero = 0.0;
		double numMissing = 0.0;
		double numTotal = 0.0;
		
		PrintStream fout = null;
		try {
			fout = new PrintStream(new FileOutputStream(pclFilename));
		} catch (FileNotFoundException e) {
			System.err.println("Could not open PCL file: " + pclFilename + ": " + e.getMessage());
		}
		
		BufferedReader fin = null;
		try {
			fin = new BufferedReader(new FileReader(softFilename));
		} catch (FileNotFoundException e) {
			System.err.println("Could not open SOFT file: " + softFilename + ": " + e.getMessage());
		}
		
		String org = "";
		String platform = "";
		String dset = "";
		String title = "";
		String desc = "";
		String pmid = "";
		String featCount = "";
		String chanCount = "";
		int sampCount = 0;
		String valType = "";
		String date = "";
		
		int lineCnt = 0;
		while (true) {
			String line = null;
			try {
				line = fin.readLine();
			} catch (IOException e) {
				System.err.println("ERROR: Could not read from SOFT file: " + e.getMessage());
				System.exit(-1);
			}
			if (line == null) {
				break;
			}
			lineCnt++;
			line.trim();
			
			// Parse header
			if (line.contains(tagOrg)) {					
				org = line.substring(tagOrg.length()).replace("\t"," ");
			}
			else if (line.contains(tagPlat)) {
				platform = line.substring(tagPlat.length()).replace("\t", " ");
			}
			else if (line.contains(tagDset)) {
				dset = line.substring(tagDset.length()).replace("\t", " ");
			}
			else if (line.contains(tagTitle)) {
				title = line.substring(tagTitle.length()).replace("\t", " ");
			}
			else if (line.contains(tagDesc)) {
				desc = line.substring(tagDesc.length()).replace("\t", " ");
			}
			else if (line.contains(tagPmid)) {
				pmid = line.substring(tagPmid.length()).replace("\t", " ");
			}
			else if (line.contains(tagFeatCount)) {
				featCount = line.substring(tagFeatCount.length()).replace("\t", " ");
			}
			else if (line.contains(tagChanCount)) {
				chanCount = line.substring(tagChanCount.length()).replace("\t", " ");
			}
			else if (line.contains("^SAMPLE")) {
				sampCount++;
			}
			//else if (line.contains(tagValType)) {
			//	valType = line.substring(tagValType.length()).replace("\t", " ");
			//}
			else if (line.contains(tagModDate)) {
				date = line.substring(tagModDate.length()).replace("\t", " ");
			}
			
			if (! inDataSection && line.startsWith("#")) {
				String[] parts = line.split(" = ");
				String val = "";
				if (parts.length != 2) {
					//System.err.println("Warning: could not data label: " + line);					
				}
				else {
					val = parts[1];
				}
				String key = parts[0].substring(1).replace("\t", " ").replace("~", "-");
				labels.put(key, val);
			}
			
			if (atLabels) {
				atLabels = false;
				inDataSection = true;
				String[] parts = line.split("\t");
				if (parts.length < 2) {
					System.err.println("Error: could not parse label at line " + lineCnt + ": " + line);
					System.exit(-1);
				}
				if (! labels.containsKey(parts[0])) {
					System.err.println("Error: invalid label at line " + lineCnt + ": " + parts[0]);
					System.exit(-1);
				}
				if (! labels.containsKey(parts[1])) {
					System.err.println("Error: invalid label at line " + lineCnt + ": " + parts[1]);
					System.exit(-1);
				}
				
				fout.print(labels.get(parts[0]) + "\t" + labels.get(parts[1]) + "\tGWEIGHT");
				for (int i = 2; i < parts.length; i++) {
					if (! labels.containsKey(parts[i])) {
						System.err.println("Error: invalid label: " + parts[i]);
						System.exit(-1);
					}
					fout.print("\t" + labels.get(parts[i]));
				}
				fout.print("\n");
				fout.print("EWEIGHT\t\t");
				for (int i = 2; i < parts.length; i++) {
					fout.print("\t1");
				}
				fout.print("\n");
			}
			else if (line.contains(tagTableStart)) {
				atLabels = true;
			}
			else if (inDataSection) {
				if (line.contains(tagTableEnd)) {
					inDataSection = false;
				}
				else {
					String[] parts = line.split("\t");
					String p1 = "";
					if (parts.length < 2) {
						//System.err.println("Warning: could not parse data line at line " + lineCnt + ": " + line);
						//System.exit(-1);
					}
					else {
						p1 = parts[1];
					}
					fout.print(parts[0] + "\t" + p1 + "\t1");
					for (int i = 2; i < parts.length; i++) {
						if (! isNumeric(parts[i])) {
							numMissing += 1;
							fout.print("\t");
						}
						else {
							numTotal += 1;
							fout.print("\t" + parts[i]);
							Float val = Float.valueOf(parts[i]);
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
					fout.print("\n");
				}
			}
		}

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

		fout.flush();
		fout.close();
		
		PrintStream finfo = null;
		try {
			finfo = new PrintStream(new FileOutputStream(infoFilename));
		} catch (FileNotFoundException e) {
			System.err.println("Could not open info file: " + infoFilename + ": " + e.getMessage());
		}

		finfo.println("File\tDatasetID\tOrganism\tPlatform\tValueType\t#channels\tTitle\tDesctiption\tPubMedID\t" +
				"#features\t#samples\tdate\tMin\tMax\tMean\t#Neg\t#Pos\t#Zero\t#MV\t#Total\t#Channels\t" +
				"logged\tzerosAreMVs\tMVcutoff");
		finfo.print(new File(softFilename).getName());
		finfo.print("\t" + dset + "\t" + org + "\t" + platform + "\t" + valType + "\t" + chanCount + "\t" + title);
		finfo.print("\t" + desc + "\t" + pmid + "\t" + featCount + "\t" + sampCount + "\t" + date);
		finfo.print("\t" + min + "\t" + max + "\t" + mean + "\t" + numNeg + "\t" + numPos + "\t");
		finfo.print(numZero + "\t" + numMissing + "\t" + numTotal + "\t" + tested_numChans + "\t");
		finfo.println(tested_logXformed + "\t" + tested_zerosMVs + "\t" + tested_MVcutoff);
		finfo.flush();
		finfo.close();
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
	 * Debug
	 * @param argv
	 */
	public static void main(String[] argv) {
		if (argv.length != 3) {
			System.err.println("Usage: input.soft output.pcl output.info");
			System.exit(-1);
		}
		GeoSeries dset = new GeoSeries();
		dset.convertSoft2Pcl(argv[0], argv[1], argv[2]);
	}
}
