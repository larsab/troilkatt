package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class HbaseSource extends Source {
	protected HTable table;
	protected String whereColumnFamily;
	protected String whereColumnQualifier;
	protected Pattern wherePattern;
	protected String selectColumnFamily;
	protected String selectColumnQualifier;
	
	/**
	 * Constructor.
	 * 
	 * For arguments description see the superclass.
	 *
	 * @param arguments [0] tableName
	 *                  [1] family:column queried
	 *                  [2] regular expression used to select rows
	 *                  [3] family:column the field returned
	 */
	public HbaseSource(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, String localRootDir,
			String hdfsStageMetaDir, String hdfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
				
		String[] argsParts = splitArgs(this.args);
		if (argsParts.length != 4) {
			logger.error("Invalid arguments: ");
			for (String p: argsParts) {
				logger.error("\t" + p);
			}
			throw new StageInitException("Invalid number of arguments: expected 4, got " + argsParts.length);
		}
		
		String tableName = argsParts[0];
		try {
			Configuration hbConf = HBaseConfiguration.create();
			table = new HTable(hbConf, tableName);
		} catch (TableNotFoundException e) {
			logger.error("Table given as argument does not exist:", e);
			throw new StageInitException("Table given as argument does not exist:" + tableName);
		} catch (MasterNotRunningException e) {			
			logger.error("Master is not running", e);
			throw new StageInitException("HBaseMaster is not running");
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper connection failed: ", e);
			throw new StageInitException("Could not connect to ZooKeeper");
		} catch (IOException e) {			
			logger.error("Could not initialized Hbase table: ", e);
			throw new StageInitException("Could not open table" + tableName);
		}  
		
		String[] whereParts = argsParts[1].split(":");
		if (whereParts.length != 2) {
			throw new StageInitException("Invalid column argument (use the format familiy:qualifier" + argsParts[1]);
		}
		whereColumnFamily = whereParts[0];
		whereColumnQualifier = whereParts[1];
		
		try {
			wherePattern = Pattern.compile(argsParts[2]);
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + args, e);
			throw new StageInitException("Invalid filter pattern: " + args);
		}
		
		String[] selectParts = argsParts[3].split(":");
		if (selectParts.length != 2) {
			throw new StageInitException("Invalid column argument (use the format familiy:qualifier" + argsParts[3]);
		}
		selectColumnFamily = selectParts[0];
		selectColumnQualifier = selectParts[1];
	}
	
	/**
	 * Retrieve a set of files to be processed by a pipeline. This function is periodically 
	 * called from the main loop.
	 * 
	 * @param metaFiles list of meta filenames that have been downloaded to the meta directory.
	 * Any new meta files are added to tis list
	 * @param logFiles list for storing log filenames.
	 * @param timestamp of Troilkatt iteration.
	 * @return list of output files in HDFS.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		
		// Scanner to iterate over all rows
		Scan scan = new Scan();	
		scan.addColumn(Bytes.toBytes(whereColumnFamily), Bytes.toBytes(whereColumnQualifier));
		scan.addColumn(Bytes.toBytes(selectColumnFamily), Bytes.toBytes(selectColumnQualifier));
		scan.setMaxVersions(1);
		
		ArrayList<String> outputFiles = new ArrayList<String>();

		ResultScanner scanner;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			logger.error("Could not create Hbase table scanner", e);
			throw new StageException("Could not create Hbase table scanner");
		}
		
		for (Result res: scanner) {
			byte[] whereBytes = res.getValue(Bytes.toBytes(whereColumnFamily), Bytes.toBytes(whereColumnQualifier));
			if (whereBytes == null) {
				logger.warn("Ignoring row that does not include where column: " + whereColumnFamily + ":" + whereColumnQualifier);
				continue;
			}
			String whereVal = Bytes.toString(whereBytes);
			
			byte[] selectBytes = res.getValue(Bytes.toBytes(selectColumnFamily), Bytes.toBytes(selectColumnQualifier));
			if (selectBytes == null) {
				logger.warn("Ignoring row that does not include select column: " + selectColumnFamily + ":" + selectColumnQualifier);
				continue;
			}
			String selectVal = Bytes.toString(selectBytes);
			
			Matcher matcher = wherePattern.matcher(whereVal);
			if (matcher.find()) {									
				outputFiles.add(selectVal);
			} 
		}
		
		return outputFiles;
	}
	
}
