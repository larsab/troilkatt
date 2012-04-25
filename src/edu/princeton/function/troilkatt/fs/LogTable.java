package edu.princeton.function.troilkatt.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.hbase.LogTableSchema;
import edu.princeton.function.troilkatt.pipeline.StageException;

/**
 * LogTable implement log file archiving in Hbase.
 * 
 * Hbase was chosen since we assume that most log-files are small, and that there
 * are very many log-files. Hence, it will not be practical to store these directly
 * in HDFS since it would require a lot of memory on the NameNode. In addition, 
 * log-files are created in parallel on all cluster-nodes. It is therefore not practical
 * to move all log-files to a machine and then use for example tar to archive them. 
 * A tool such as HadoopArchiver could be modified to both collect the log-files using
 * MapReduce and to reduce the NameNode overhead. However, HadoopArchiver does not
 * support effecient random accesses to the individual log-files, which we assume some
 * of the Troilkatt log-analysis tools will need. Finally, special purpose log-file 
 * analysis tools such as Chukwa are overkill for Troilkatt since these are often built
 * for real-time analysis of very large log files. Troilkatt is simpler since we always
 * know when the log-files should be saved (at the end of each iteration), and since the
 * analysis on the log-files is typically simpler.
 * 
 * Disadvantages of Hbase is that the log-files are not directly visible in HDFS, and must
 * therefore be accessed using the hard-to-use Hbase shell, or by Troilkatt tools. In 
 * addition it creates an additional dependency for Troilkatt.
 * 
 * The BigTable architecture is as follows:
 * -Each pipeline has a seperate Table. This makes it easier to administer the system by for
 * example dropping tables for debug and test pipelines.
 * -Each table is named using the pipeline name. 
 * -There is one row per stage instance execution.
 * -The row key is: stagenum-stagename.timestamp 
 * -There is one row per mapreduce task.
 * -The row key for mapreduce tasks is: stagenum-stagename.timestamp.taskid
 * -There are four column families: output, error, log, and other. Files are distributed
 * among column families based on their file suffix. 
 * -Just a single version of each cell is kept.
 */
public class LogTable {	
	// There is one table per pipeline and the same name is used both for the
	// pipeline and and the table
	protected String tableName;
	// The table itself
	protected HTable table;
	// Table schema and useful utility functions
	public LogTableSchema schema;
	
	protected Configuration hbConfig;
	protected Logger logger;
	
	/**
	 * Constructor.
	 * 
	 * Note! creating a Hbase Table is expensive, so it should only be done once. In addition,
	 * there should be one instance per thread.
	 * 
	 * @throws PipelineException 
	 * 
	 */
	public LogTable(String pipelineName, Configuration hbConfig) throws PipelineException {		
		this.hbConfig = hbConfig;
		logger = Logger.getLogger("troilkatt.logtable." + pipelineName);
		tableName = "troilkatt-log-" + pipelineName;
		
		schema = new LogTableSchema(tableName);
		try {
			table = schema.openTable(hbConfig, true);
		} catch (HbaseException e2) {
			logger.fatal("Could not open table: ", e2);
			throw new PipelineException("Could not open table: " + e2.toString());
		}
	}
	
	/**
	 * Destructor
	 */
	public void finalize() {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				logger.warn("Table.close exception: ", e);
			}
		}
	}

	/**
	 * Save logfiles to Hbase
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used for the row ID
	 * @param logFiles log files to save
	 * 
	 * @throws StageException if file content could not be save in Hbase
	 * @return number of files save 
	 */
	public int putLogFiles(String stageName, long timestamp, ArrayList<String> localFiles) throws StageException {
	
		String rowKey = getRowKey(stageName, timestamp);
		return putLogFilesCommon(rowKey, localFiles);	
	}

	/**
	 * Save logfiles for a MapReduce task
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used for the row ID
	 * @param logFiles log files to save
	 * 
	 * @throws StageException if file content could not be save in Hbase
	 * @return number of files saved
	 */	
	public int putMapReduceLogFiles(String stageName, long timestamp, String taskID, 
			ArrayList<String> localFiles) throws StageException {
		String rowKey = getRowKey(stageName, timestamp, taskID);
		System.err.println("Put into: " + rowKey);
		return putLogFilesCommon(rowKey, localFiles); 	
	}
	
	/**
	 * Retrieve all logfiles for a stage to the local FS
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param localDir directory where all log-files are saved
	 * @return list of absolute filenames for all log files retrieved for stage
	 * @throws StageException  if a retrieved file cannot be written to the local FS, or
	 * the row could not be read from Hbase 
	 */
	public ArrayList<String> getLogFiles(String stageName, long timestamp, String localDir) throws StageException {
		if (! OsPath.isdir(localDir)) {
			if (! OsPath.mkdir(localDir)) {
				logger.fatal("Could not create output directory: " + localDir);
				throw new StageException("mkdir failed for: " + localDir);
			}
		}
		
		String rowKey = getRowKey(stageName, timestamp);
		Get rowGet = new Get(Bytes.toBytes(rowKey));
		// All columns are retrieved, so the family, qualifier, a filter, etc are not specified
		
		Result result;
		try {
			logger.info("Get row " + rowKey + " in table " + tableName);
			result = table.get(rowGet);
		} catch (IOException e) {
			logger.fatal("Could not get row: " + rowKey + ": " + e.toString());
			throw new StageException("Could not get row: " + rowKey + ": " + e.toString());
		}
		return getRowLogFiles(result, localDir);
	}
	
	/**
	 * Retrieve all logfiles for a MapReduce stage to the local FS
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param localDir directory where all log-files are saved. The logfiles for each task
	 * will be stored in a subdirectory useing the task ID as name.
	 * @return list of absolute filenames for all log files retrieved for stage
	 * @throws StageException  if a retrieved file cannot be written to the local FS, or
	 * the row could not be read from Hbase
	 * @throws IOException 
	 */
	public ArrayList<String> getMapReduceLogFiles(String stageName, long timestamp, String localDir) throws StageException {
		if (! OsPath.isdir(localDir)) {
			if (! OsPath.mkdir(localDir)) {
				logger.fatal("Could not create output directory: " + localDir);
				throw new StageException("mkdir failed for: " + localDir);
			}
		}
		else {
			logger.warn("Output directory already exists: " + localDir);
		}
		
		// First key is the row without a task ID
		String rowStartKey = getRowKey(stageName, timestamp);
		String parts[] = stageName.split("-");
		if (parts.length < 2) {
			throw new RuntimeException("Invalid stageName: " + stageName);
		} 
		int stageNum = 0;
		try {
			stageNum = Integer.valueOf(parts[0]);
		} catch(NumberFormatException e) {
			throw new RuntimeException("Invalid stageName (first part not a number): " + stageName);
		}
		// Last key is the first row for the next stage
		String rowEndKey = String.format("%03d-", stageNum + 1);
		// Filter to ensure that only rows for the given stage are selected
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
			      new SubstringComparator(rowStartKey));
		// Need a scanner to iterate over all MapReduce task rows
		Scan scan = new Scan(Bytes.toBytes(rowStartKey), Bytes.toBytes(rowEndKey));		
		scan.setFilter(filter);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		ResultScanner scanner;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			logger.warn("Could not create Hbase table scanner " + e.getMessage());
			throw new StageException("Could not create Hbase table scanner " + e.getMessage());
		}
		
		// Iterate over all returned rows		
		for (Result res: scanner) {
			String rowKey = Bytes.toString(res.getRow());
			if (! rowKey.contains(rowStartKey)) {
				logger.warn("Scanner returned row which was not part of stage: " + rowStartKey);
				continue;
			}
			String taskID = getTaskId(rowKey);
			if (taskID == null) { // is master log files
				ArrayList<String> rlf = getRowLogFiles(res, localDir);
				logFiles.addAll(rlf);
			}
			else {
				String taskDir = OsPath.join(localDir, taskID);
				if (! OsPath.mkdir(taskDir)) {
					logger.fatal("Could not create output directory: " + taskDir);
					throw new StageException("mkdir failed for: " + taskDir);
				}
				ArrayList<String> rlf = getRowLogFiles(res, taskDir);
				logFiles.addAll(rlf);
			}
		}
		
		return logFiles;
	}

	/**
	 * Check if a logfile exists. This function is mostly used for testing and debugging.
	 * 
	 * @param stageName stage name used finor the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param logFilename file to check
	 * @return true if a logfile exists, false otherwise
	 * @throws StageException 
	 */
	public boolean containsFile(String stageName, long timestamp, String logFilename) throws StageException {
		String rowKey = getRowKey(stageName, timestamp);
			
		String family = getFamily(logFilename);			
		String qualifier = OsPath.basename(logFilename); // column key is filename
		Get cellGet = new Get(Bytes.toBytes(rowKey));	
		cellGet.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		boolean result;
		try {
			result = table.exists(cellGet);
		} catch (IOException e) {
			logger.fatal("Could not test row: " + rowKey + ": " + e.toString());
			throw new StageException("Could not test row: " + rowKey + ": " + e.toString());
		}
		return result;
	}
	
	/**
	 * Check if a logfile exists. This function is mostly used for testing and debugging.
	 * 
	 * @param stageName stage name used finor the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param taskID mapreduce task ID to check
	 * @param logFilename file to check
	 * @return true if a logfile exists, false otherwise
	 * @throws StageException 
	 */
	public boolean containsFile(String stageName, long timestamp, String taskID, String logFilename) throws StageException {
		String rowKey = getRowKey(stageName, timestamp, taskID);
			
		String family = getFamily(logFilename);			
		String qualifier = OsPath.basename(logFilename); // column key is filename
		Get cellGet = new Get(Bytes.toBytes(rowKey));	
		cellGet.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		boolean result;
		try {
			result = table.exists(cellGet);
		} catch (IOException e) {
			logger.fatal("Could not test row: " + rowKey + ": " + e.toString());
			throw new StageException("Could not test row: " + rowKey + ": " + e.toString());
		}
		return result;
	}

	/**
	 * Helper function to save either sequential stage log files or MapReduce stage 
	 * logfiles. 
	 * 
	 * @param rowKey key for row to create
	 * @param localFiles files to save
	 * @return number of files saved. Note that in addition to files that cannot be 
	 * read, empty files are also not saved.
	 * 
	 * @throws StageException if a row could not be saved in Hbase
	 */
	private int putLogFilesCommon(String rowKey, ArrayList<String> localFiles) throws StageException {
		if (localFiles.size() == 0) {
			logger.debug("No logfiles to save for " + rowKey);
			return 0;
		}
		
		logger.debug("Save " + localFiles.size() + " logfiles to row: " + rowKey);
		System.err.println("Save " + localFiles.size() + " logfiles to row: " + rowKey);

		// Create update that contains all log files
		Put update = new Put(Bytes.toBytes(rowKey));

		int saved = 0;
		byte[] emptyFileContent = Bytes.toBytes("");
		// Create a row with all log file content
		for (String f: localFiles) {
			if (! OsPath.isfile(f)) {
				logger.warn("Invalid log file: " + f);
				continue;
			}
			
			String family = getFamily(f);			
			String qualifier = OsPath.basename(f); // column key is filename 
			byte[] fileContent;
			try {
				fileContent = FSUtils.readFile(f);
				if (fileContent.length > 0) {
					update.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), fileContent);					
				}	
				else { // is empty file
					update.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), emptyFileContent);
				}
				saved++;
			} catch (IOException e) {
				logger.warn("Could not read logfile: " + f + ": " + e.getMessage());				
			}								
		}
		
		if (saved == 0) {
			return 0;
		}
		
		// Do the update		
		try {
			table.put(update);
		} catch (IOException e) {
			logger.fatal("Could not save log files in Hbase: " + e.getMessage());
			System.err.println("Could not save log files in Hbase: " + e.getMessage());
				throw new StageException("Could not save log files in Hbase: " + e.getMessage());
		} catch (IllegalArgumentException e) {
			// The log file was probably to large to be saved
			logger.fatal("Could not save log files in Hbase: " + e.getMessage());
			System.err.println("Could not save log files in Hbase: " + e.getMessage());
			//throw new StageException("Could not save log files in Hbase: " + e.getMessage());
		}
		
		return saved;
	}
	
	/**
	 * Helper function to save all logfiles saved in the provided row to the provided
	 * directory
	 * @param result Hbase row with files to save
	 * @param dir directory where row content should be saved
	 * @return absolute filename of all retrived log files
	 * @throws StageException if a retrieved file cannot be written to the local FS
	 */
	private ArrayList<String> getRowLogFiles(Result result, String dir) throws StageException {
		ArrayList<String> logFiles = new ArrayList<String>();
		if (result.isEmpty()) {
			logger.info("Empty result received for row");
			return logFiles;
		}
		
		// Iterate over each column family
		for (String f: schema.colFams) {
			byte[] colFamBytes = Bytes.toBytes(f);
			// The column names are unknown since these are based on the file names
			NavigableMap<byte[], byte[]> qualifiers = result.getFamilyMap(colFamBytes);
		
			// Iterate over all columns in the family		
			for (byte[] colQualBytes: qualifiers.keySet()) {
				String filename = Bytes.toString(colQualBytes);
				byte[] value = result.getValue(colFamBytes, colQualBytes);
				if (value == null) {
					logger.warn(String.format("No data for %s:%s", f, filename));
					continue;
				}
			
				// Write file content to the output directory on the local FS
				String outputFilename = OsPath.join(dir, filename);                
				logger.debug("Write downloaded data to: " + outputFilename);
				try {
					FSUtils.writeFile(outputFilename, value);
				} catch (IOException e) {
					logger.fatal("Could not write logfile to local FS: " + e.toString());
					throw new StageException("Could not write logfile to local FS: " + e.toString());
				}
				logFiles.add(outputFilename);
			}
		}
		
		return logFiles;
		
	}

	/**
	 * 
	 * @param stageName
	 * @param timestamp
	 * @return row key based on stageNum, stageName and timestamp
	 */
	private String getRowKey(String stageName, long timestamp) {
		return String.format("%s.%d", stageName, timestamp); 
	}

	/**
	 * 
	 * @param stageName
	 * @param timestamp
	 * @param taskID
	 * @return row key based on stageNum, stageName, timestamp and taskID
	 */
	private String getRowKey(String stageName, long timestamp,
			String taskID) {
		return String.format("%s.%d.%s", stageName, timestamp, taskID);
	}

	/**
	 * Helper function to get the column family for a file
	 * 
	 * @param filename
	 * @return column family name (output, error, log, or other), or null if the 
	 * filename is invalid
	 */
	private String getFamily(String filename) {
		String fam = OsPath.getLastExtension(filename);
		if (fam == null) {
			return null;
		}
		else if (filename.equals("stdout")) {
			return "out";
		}
		else if (filename.equals("stderr")) {
			return "error";
		}
		else if (filename.equals("syslog")) {
			return "sys";
		}
		else if (fam.equals("out") || fam.equals("error") || fam.equals("log")) {
			// Special file type
			return fam;
		}
		else {
			return "other";
		}
	}
	
	/**
	 * Get task ID for a MapReduce row
	 * 
	 * @param rowKey row key
	 * @return task ID, or null if no task ID was found
	 */
	private String getTaskId(String rowKey) {
		String[] parts = rowKey.split("\\.");
		if (parts.length < 3) {
			return null;
		}
		else {
			// Row keys have the following parts: stageNum-stageID, timestamp, and taskID
			return parts[2];
		}
	}
	
}
