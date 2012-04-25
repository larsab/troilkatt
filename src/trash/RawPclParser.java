/**
 * Create INFO file for a PCL file
 */
package edu.princeton.function.troilkatt.tools;

import java.io.IOException;

/**
 * @author larsab
 *
 * Deprecated; use GeoRaw2Pcl instead
 */
@Deprecated
public class RawPclParser extends SeriesFamilyParser {

	/**
	 * Constructor
	 * 
	 * @param softFile: SOFT file for the sereies to parse
	 * @param pclFile: PCL file created by normalizing RAW CEL files
	 * @param infoFile: output INFO filename 
	 */
	public RawPclParser(String softFile, String pclFile, String infoFile) {
		super(softFile, pclFile, infoFile);
	}
	
	/**
	 * Constructor
	 * 
	 * @param pclFile: PCL file created by normalizing RAW CEL files
	 * @param infoFile: output INFO filename 
	 */
	public RawPclParser(String pclFile, String infoFile) {
		// Instead of the pcl file the soft file is used. Besdies the filename,
		// it does not contain any of the information of a SOFT file
		// TODO: find better way of doign it
		super(pclFile, pclFile, infoFile);
	}

	/**
	 * Debug
	 * 
	 * @param argv: soft-filename pcl-filename info-filename <gene-map>
	 * @throws IOException 
	 */
	public static void main(String[] argv) throws IOException {
		RawPclParser dset = null;
		if (argv.length < 2) {
			System.err.println("Usage: output.pcl output.info");
			System.exit(-1);
		}
		dset = new RawPclParser(argv[0], argv[1]);			
		dset.createInfoFile();
	}

}
