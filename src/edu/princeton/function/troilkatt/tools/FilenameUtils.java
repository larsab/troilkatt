package edu.princeton.function.troilkatt.tools;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Various utility functions for working with GEO SOFT and PCL files
 */
public class FilenameUtils {
	public static final String PLATFORM_SEPERATOR = "-";
	public static final Pattern INTEGERS_ONLY = Pattern.compile("\\d+");
	/**
	 * Convert filename to dataset ID
	 * 
	 * @param filename
	 * @param includePlatformSubfix true if platform subfix should not be stripped
	 * @return dataset ID with or without platform subfix
	 */
	public static String getDsetID(String filename, boolean includePlatformSubfix) {
		
		if (filename == null) {
			return null;
		}
		
		String basename = OsPath.basename(filename);
		String dsetID = basename.split("\\.")[0];		
		dsetID = dsetID.split("_")[0];
		
		if (includePlatformSubfix) {
			return dsetID;
		}
		else {
			return dsetID.split(PLATFORM_SEPERATOR)[0];
		}
	}
	
	/**
	 * Convert filename to dataset ID
	 * 
	 * @param filename
	 * @return dataset ID that may include platform subfix
	 */
	public static String getDsetID(String filename) {
		return FilenameUtils.getDsetID(filename, true);
	}
	
	/**
	 * Merge a dataset ID and a platform ID
	 * 
	 * @param dsetID dataset ID
	 * @param platID platform ID
	 * @return dsetID + SEPERATOR + platID
	 */
	public static String mergeDsetPlatIDs(String dsetID, String platID) {
		return dsetID + PLATFORM_SEPERATOR + platID;
	}
	
	/**
	 * Get the plaform ID in a filename
	 * 
	 * @param filename
	 * @return platform ID, or empty string if the filename does not have a platform ID
	 */
	public static String getPlatID(String filename) {
		if (filename == null) {
			return null;
		}
		
		String basename = OsPath.basename(filename);
		String dsetID = basename.split("\\.")[0];		
		dsetID = dsetID.split("_")[0];
		
		String[] parts = dsetID.split(PLATFORM_SEPERATOR);
		if (parts.length != 2) {
			return "";
		}
		else {
			return parts[1];
		}
	}
	
	/**
	 * @return True if the filename contains a platform ID, False otherwise
	 */
	public static boolean hasPlatID(String filename) {
		if (FilenameUtils.getPlatID(filename).isEmpty()) {
			return false;
		}
		else {
			return true;
		}
	}
	
	/**
	 * If name is long, e.g. GSM1145128_NetFra-1_Drosophila_2_.CEL, it will convert it to GSM1145128.CEL
	 * @param sampleName
	 * @return shorter name (or the same)
	 */
	public static String convertSampleName(String sampleName) {
		if (sampleName == null || sampleName.trim().equals(""))
			return "";

		if (!sampleName.toLowerCase().startsWith("gsm"))
			return sampleName;
					
		Matcher makeMatch = INTEGERS_ONLY.matcher(sampleName);
		makeMatch.find();		
		String digits = makeMatch.group();
		return sampleName.substring(0,3).toUpperCase() + digits; 
	}
	
	public static void main(String[] args) {
	
		String name = "GSM1145128_NetFra-1_Drosophila_2_.CEL";
		System.out.println(name+": "+convertSampleName(name));
		name = "GSM1145128CEL";
		System.out.println(name+": "+convertSampleName(name));
		name = "GSM1145128.CEL";
		System.out.println(name+": "+convertSampleName(name));
		name = "GSM1145128_asdf.CEL";
		System.out.println(name+": "+convertSampleName(name));
		name = "gsm1145128_asdf.cel";
		System.out.println(name+": "+convertSampleName(name));
		name = "GSM915201_DMSO_G.CEL";
		System.out.println(name+": "+convertSampleName(name));
		name= "GSM244169.cel";
		System.out.println(name+": "+convertSampleName(name));
		name= "GSM953405_1540_3674_18638_1c48_Celegans.CEL";
		System.out.println(name+": "+convertSampleName(name));				
	}
	
	
}
