package edu.princeton.function.troilkatt.tools;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Various utility functions for working with GEO SOFT and PCL files
 */
public class FilenameUtils {
	public static final String PLATFORM_SEPERATOR = "-";
	
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
}
