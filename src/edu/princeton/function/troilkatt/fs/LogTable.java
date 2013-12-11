package edu.princeton.function.troilkatt.fs;

import java.util.ArrayList;
import org.apache.log4j.Logger;


import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.hbase.LogTableSchema;
import edu.princeton.function.troilkatt.pipeline.StageException;

/**
 * LogTable implement log file archiving. This is a superclass.
 * 
 */
public class LogTable {	
	// There is one table per pipeline and the same name is used both for the
	// pipeline and and the table
	protected String tableName;	
	// Table schema and useful utility functions
	public LogTableSchema schema;	
	
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
	public LogTable(String pipelineName) throws PipelineException {		
		logger = Logger.getLogger("troilkatt.logtable." + pipelineName);
		tableName = "troilkatt-log-" + pipelineName;
		
		schema = new LogTableSchema(tableName);
	}
	
	/**
	 * Destructor
	 */
	public void finalize() {
		
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
	public int putLogFiles(String stageName, long timestamp, ArrayList<String> logFiles) throws StageException {
		throw new RuntimeException("Not implemented");
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
		throw new RuntimeException("Not implemented");
	}
	

	/**
	 * Check if a logfile exists. This function is mostly used for testing and debugging.
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param logFilename file to check
	 * @return true if a logfile exists, false otherwise
	 * @throws StageException 
	 */
	public boolean containsFile(String stageName, long timestamp, String logFilename) throws StageException {
		throw new RuntimeException("Not implemented");
	}
	
}
