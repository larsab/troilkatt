package edu.princeton.function.troilkatt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;

/**
 * Troilkatt status file manipulation
 * 
 * The file format is one status per line with the following format: 
 *
 *   timestamp: stageID: status
 *
 * Status may be: start, done, or recover
 */
public class TroilkattStatus {
	protected Logger logger = Logger.getLogger("troilkatt.status"); 
	
	/* Status path on local FS and HDFS/NFS */
	protected String localFilename;      // Local filename: absolute name	
	protected String persistentFilename; // tfs filename: absolute name
	
	
	// Set in constructor
	protected TroilkattFS tfs = null;       
	
	/**
	 * Constructor.
	 * 
	 * Verify that Troilkatt status and timestamp files are on the local filesystem.
	 * If not, either download the file from tfs or create a new file.
	 * 
	 * @param tfsHandle TFS handle
	 * @param troilkattProperties initialized TroilkattProperties object 
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	public TroilkattStatus(TroilkattFS tfsHandle, TroilkattProperties troilkattProperties) throws IOException, TroilkattPropertiesException {
		tfs = tfsHandle;
		
		String troilkattDir = troilkattProperties.get("troilkatt.localfs.dir");		
		persistentFilename = troilkattProperties.get("troilkatt.tfs.status.file");
		localFilename = OsPath.join(troilkattDir, OsPath.basename(persistentFilename));
				
		/*
		 * Verify, download, or create status file
		 */
		tfs.getStatusFile(persistentFilename, localFilename);
	}
	
	/**
	 * Save status file to persistent storage
	 * @throws TroilkattPropertiesException 
	 * @throws IOException 
	 */
	public void saveStatusFile() throws IOException, TroilkattPropertiesException {
		tfs.saveStatusFile(localFilename, persistentFilename);
	}
	
	/**
	 * @return a timestamp (the current date/time as milliseconds sine 1970).
	 */
	public static long getTimestamp() {
		Date date = new Date();
		return date.getTime();
	}
	
	/**
	 * Convert a timestamp to a string with in the format: yyyy-MM-dd-HH:mm
	 * 
	 * @return a String representation of the timestamp
	 */
	public String timeLong2Str(long timestamp) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
		Date date = new Date(timestamp);
		return dateFormat.format(date);
	}

	/**
	 * Convert a date/time given as a string to an integer.
	 *
	 * @param timestr date/time in the following format: YYYY-MM-DD-HH-mm.
	 * @return time converted to an integer value (as returned by time.time()). The 
	 * integer has a granularity of a minute due to the argument format.
	 */
	public long timeStr2Long(String timestr) {
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
		Date date;
		try {
			date = dfm.parse(timestr);
		} catch (ParseException e) {
			logger.fatal("Could not parse time string: " + timestr);
			throw new RuntimeException(e);
		}

		return date.getTime();	
	}

	/**
	 * Check the status of the last entry for the given stage in the status file. 
	 * Note that for most cases the getStatus(stageID, timestamp) should be used
	 * instead since it allows specifying which iteration to get the status for.
	 *
	 * @param stageID stage to get status for
	 * @return last status for the given stage, or null if no status for the stage
	 * was found.
	 * @throws IOException if status file cannot be read 
	 */
	public String getLastStatus(String stageID) throws IOException {
		String lastLine = getLastLine(stageID, null);
		if (lastLine == null) {
			return null;
		}
		else {
			String parts[] = lastLine.split(":");
			return parts[2].trim();
		}
	}
	
	/**
	 * Check the status of the last entry for the given stage at the specified 
	 * timestamp in the status file. 
	 *
	 * @param stageID stage to get status for
	 * @param timestamp timestamp to get status for
	 * @return status, or null if the entry was not found 
	 * @throws IOException 
	 */
	public String getStatus(String stageID, long timestamp) throws IOException {
		BufferedReader inputStream = null;
		
		// There may multiple statuses for the same timestamp 
		String lastStatus = null;
		
		try {
			inputStream = new BufferedReader(new FileReader(localFilename));

			String l;            
			while ((l = inputStream.readLine()) != null) {
				String parts[] = l.split(":");
				if (parts.length != 3) {					
					logger.warn("Invalid line in status file:" + l);
					continue;
				}
				
				long ts = -1;
				String id = parts[1].trim();				
				try {
					ts = Long.valueOf(parts[0]);
				} catch (NumberFormatException e) {
					logger.warn("Invalid line in status file: " + l);
					continue;
				}
				
				if ((timestamp == ts) && stageID.equals(id)) {
					lastStatus = parts[2].trim();
				}					
			}   
			inputStream.close();
		} 
		catch (IOException e) { 			
			logger.fatal("Could not read status file: " + e.toString());
			throw e;
		}

		return lastStatus;
	}
	
	/**
	 * Return timestamp of the last status of the given type for a given stage
	 *
	 * @param stageID stage to get timestamp for
	 * @param status status to match
	 * @return timestamp of last status of the given type for the given stage, or
	 *  -1 if not found or the timestamp was invalid due to a corrupted file.
	 * @throws IOException 
	 */
	public long getLastStatusTimestamp(String stageID) throws IOException {
		String lastLine = getLastLine(stageID);
		if (lastLine == null) {
			return -1;
		}
		else {
			String parts[] = lastLine.split(":");
			try {
				return Long.valueOf(parts[0]);
			} catch (NumberFormatException e) {
				logger.fatal("Invalid timestamp in status file: " + parts[0]);
				return -1;
			}
		}
	}

	/**
	 * Append status file.
	 * 
	 * @param stageID stage identifier
	 * @param timestamp timestamp for new status.	
	 * @param newStatus new status to add.
	 * @throws IOException if status file cannot be updated.
	 */
	public void setStatus(String stageID, long timestamp, String newStatus) throws IOException {
		PrintWriter statusFile = null;
		try {		
			statusFile = new PrintWriter(new FileWriter(localFilename, true));
			statusFile.printf("%d:%s:%s\n", timestamp, stageID, newStatus);
			statusFile.close();
		} catch (IOException e) {
			logger.fatal("Could not update status file: " + e.toString());
			throw e;
		}	
	}

	
	
	/**
	 * Get last line in status file for a given stage.
	 * 
	 * @param stageID ID in stage
	 * @return last line with given stageID, or null if no status was found
	 * @throws IOException 
	 */
	private String getLastLine(String stageID) throws IOException {
		return getLastLine(stageID, null);
	}

	/**
	 * Get last line in status file for a given stage that matches the provided
	 * status.
	 * 
	 * @param stageID ID in line to find
	 * @param status status to match. If null the line is not matched to a status.
	 * @return last line with given stageID, or null if no status is found
	 * @throws IOException 
	 */
	private String getLastLine(String stageID, String status) throws IOException {
		BufferedReader inputStream = null;
		String lastLine = null;
	
		try {
			inputStream = new BufferedReader(new FileReader(localFilename));
	
			/* Find last status for stage */
			String l;            
			while ((l = inputStream.readLine()) != null) {
				String parts[] = l.split(":");
				if (parts.length != 3) {					
					logger.warn("Invalid line in status file:" + l);
					continue;
				}
				if (parts[1].equals(stageID)) {
					if (status == null) {
						lastLine = l;					
					}
					else if (parts[2].equals(status)) { // status != null
						lastLine = l;					
					}			
				}
			}   
			inputStream.close();
		} 
		catch (IOException e) { 			
			logger.fatal("Could not read status file: " + e.toString());
			throw e;
		}
	
		if (lastLine == null) {			
			logger.warn("Could not find previous status for stage: " + stageID);
			return null;
		}		
		else {
			return lastLine;
		}
	}

	/**
	 * Main used for debugging
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	public static void main(String[] args) throws IOException, TroilkattPropertiesException {		
		TroilkattFS tfs = new TroilkattHDFS();
		TroilkattProperties troilkattProperties = new TroilkattProperties("/home/larsab/workspace/skarntyde/troilkatt2/conf/ice.xml");
		troilkattProperties.set("troilkatt.localfs.dir", "/tmp");
		TroilkattStatus status = new TroilkattStatus(tfs, troilkattProperties);
		
		String rootStatus = status.getLastStatus("Troilkatt");
		long timestamp = status.getLastStatusTimestamp("Troilkatt");		
		System.out.println("Troilkatt status: " + rootStatus);
		System.out.println("Last timestamp: " + timestamp);
		
		System.out.println("Source status: " + status.getLastStatus("000-geo_meta"));
		System.out.println("Source status at last timestamp: " + status.getStatus("000-geo_meta", timestamp));
		
		System.out.println("Stage 1 status: " + status.getLastStatus("001-remove_overlapping"));
		System.out.println("Stage 1 status at last timestamo: " + status.getStatus("001-remove_overlapping", timestamp));
		
		System.out.println("Stage 3 status: " + status.getLastStatus("002-missing_values"));
		System.out.println("Stage 3 status at last timestamo: " + status.getStatus("002-missing_values", timestamp));
	}
}
