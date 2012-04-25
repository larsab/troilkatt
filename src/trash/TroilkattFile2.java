package edu.princeton.function.troilkatt.fs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import org.apache.log4j.Logger;


/**
 * Data structure for holding information about a file saved in HDFS or the local
 * file system.
 * 
 * The format on the local filesystem is just the temporary directory used by troilkatt
 */
public class TroilkattFile2 { 
	public String hostname = null; // host which local file is on
	public String localName = null;
	public String hdfsName = null;		

	/**
	 * Constructor.
	 *
	 * @param localName absolute filename on local filesystem that contains the file. If null
	 * the file is not on the local filesystem.
	 * @param hdfsDir absolute filename on HDFS where the file should be stored. If null the 
	 * tile is not in HDFS.	
	 */
	public TroilkattFile2(String localName, String hdfsName) {				
		this.localName = localName;
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException("Could not get hostname: " + e);
		}
		this.hdfsName = hdfsName;		
	}
	
	/**
	 * TroilkattFile objects are equal of they:
	 * 1. Have the same HDFS filename
	 * 2. One does not have a HDFS filename, and they have the same local name 
 	 *
	 * @return true if the rowKey and colKey are equal
	 */
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (! (o instanceof TroilkattFile2)) {
			return false;
		}
		
		TroilkattFile2 h = (TroilkattFile2) o;		
		if ((h.hdfsName != null) && (this.hdfsName != null)) {		
			return h.hdfsName.equals(this.hdfsName);
		}
		else if ((h.localName != null) && (this.localName != null)) {
			return h.localName.equals(this.localName);
		}
		else {
			return false;
		}
	}
	
	/**
	 * @return the local FS filename
	 */
	public String getLocalFSFilename() {
		return localName;
	}
	
	/**
	 * @return the HDFS filename
	 */
	public String getHDFSFilename() {
		return hdfsName;
	}
	
	/**
	 * @return the local FS base filename.
	 */
	public String getLocalFSBasename() {
		if (localName != null) {
			return OsPath.basename(localName);
		}
		else {
			return null;
		}
	}
	
	/**
	 * @param dir directory name
	 * @return HDFS filename relative to dir, or null if HDFS filename is not set
	 * or the HDFS filename does not contain the dir prefix.
	 */
	public String getHDFSRelativeFilename(String dir) {
		if (hdfsName == null) {
			return null;
		}
		else if (! hdfsName.startsWith(dir)) {
			return null;
		}
		else {
			return hdfsName.replace(dir, "");
		}		
	}
		
	/**
	 * Remove duplicates from a filelist
	 */
	public static ArrayList<TroilkattFile2> removeDuplicates(ArrayList<TroilkattFile2> files, Logger logger) {
		if (files.size() == 0) {
			return files;
		}
		
		// Verify that there are no duplicates
		ArrayList<TroilkattFile2> unique = new ArrayList<TroilkattFile2>();
		for (TroilkattFile2 hbf: files) {
			if (unique.contains(hbf)) {
				logger.debug("outputFiles list contains duplicates: " + hbf.localName);				
			}
			else {
				unique.add(hbf);
			}
		}	
		
		return unique;
	}
	
	/**
	 * Check if a list of Troilkatt files contain a file with the same localName
	 */
	public static boolean hasLocalName(ArrayList<TroilkattFile2> list, String name) {
		for (TroilkattFile2 tf: list) {
			if (name.equals(tf.localName)) {
				return true;
			}
		}
		return false;
	}
}

