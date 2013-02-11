package trash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.SinkTable;

/**
 * Detect updates in a sink. 
 */
public class SinkCrawler extends Source {
	protected String inputSink;	

	SinkTable sinkTable;

	/**
	 * @param arguments: sink to crawl
	 * 
	 * Refer to the Crawl for descriptions of the rest of the arguments.
	 */
	public SinkCrawler(String name, String arguments, String outputDir,
			String logDir, String tmpDir, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {
		super(name, arguments, outputDir, logDir, tmpDir, datasetTable,
				pipelineTable, troilkattProperties);

		/*
		 * Initialize global data structures
		 *
		 * Note! that input and output directory is the same for this crawler        
		 */
		inputSink = args;
		sinkTable = new SinkTable(pipelineTable.hdfsConfig, pipelineTable.hbConfig, troilkattProperties);
	}

	/**
	 * Check for new updates and generate outputfiles. This function is periodically called from the
	 * main loop (via the Crawl.crawl2() function)
	 *
	 * @param retrievedFiles: list of absolute filenames for new files retrieved
	 * @param deletedFiles: list of deleted files 
	 * @param logs: list of log files to save
	 * @param objects: hashmap where objects to be stored in Bigtable are saved (id -> object)   	
	 * @param timestamp: string with unique identifier for this crawl
	 *
	 * @return retrievedFiles, updatedFiles, deletedFiles, logs: are updated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void crawl(ArrayList<TroilkattFile> retrievedFiles, ArrayList<TroilkattFile> deletedFiles, ArrayList<String> logs, HashMap<String, Object> objects, long timestamp) {
		logger.info("Crawl at: " + timestamp);

		/* Filename to SHA-1 mapping. This data structure is used to detect new and 
		 * updated files in the directory. */
		System.out.println("DEBUG: Fingerprints object not downloaded by SinkCrawler");
		Object fpo = null;
		//Object fpo = pipelineTable.getObject(name, "fingerprints", timestamp);
		
		HashMap<String, byte[]> prevFPs;		
		if (fpo == null) {
			prevFPs = new HashMap<String, byte[]>();
		}
		else {
			prevFPs = (HashMap<String, byte[]>) fpo;
		}				
		HashMap<String, byte[]> newFPs = new HashMap<String, byte[]>();

		// Download the filelist for the files currently in the sink            
		ArrayList<TroilkattFile> sinkFiles = sinkTable.getAllFiles(inputSink, -1);
		logger.debug(String.format("Downloaded sink fingerprint map with %d entries", sinkFiles.size()));      		

		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {
			logger.fatal("Could not create HDFS FileSystem object");
			logger.fatal(e1.toString());
			throw new RuntimeException(e1);
		}	
		
		ArrayList<String> sinkFilenames = new ArrayList<String>();
		for (TroilkattFile tf: sinkFiles) {
			String hdfsName = tf.getHDFSFilename();
			sinkFilenames.add(hdfsName);
			byte[] currentFP = null;
			//try {
				//currentFP = hdfs.getFileChecksum(new Path(hdfsName)).getBytes();
			//} catch (IOException e) {
			//	logger.fatal("Could not get checksum for: " + hdfsName);
			//	logger.fatal(e.toString());
			//	throw new RuntimeException(e);
			//}			
			
			if (! prevFPs.containsKey(hdfsName)) {
				tf.setDir("none");
				retrievedFiles.add(tf);
				newFPs.put(hdfsName, currentFP);
			}
			else {
				// Verify that the file content has not changed				
				byte[] oldFP = prevFPs.get(hdfsName);
				if (! Arrays.equals(currentFP, oldFP)) { // is changed
					tf.setDir("none");
					retrievedFiles.add(tf);		
				}
				// else no changes
			}
		}  
		
		for (String k: prevFPs.keySet()) {
			if (! sinkFilenames.contains(k)) {
				deletedFiles.add(new TroilkattFile(k, 
						troilkattProperties.get("troilkatt.tfs.root.dir"), 
						this.outputDir, 
						logger) );
			}
		}

		try {
			hdfs.close();
		} catch (IOException e) {
			logger.fatal("Could not close HDFS FileSystem object");
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}
		
		logger.debug("New files: " + retrievedFiles.size());
		logger.debug("Deleted files: " + deletedFiles.size());
		
		// There are no log files
		objects.put("fingerprints", newFPs);
	}
	
	/**
	 * Helper function to downaload a file from HDFS.
	 * 
	 * @param tf: TroilkattFile handle of file to download
	 * @return: true if file was downloaded, false otherwise.
	 */
	public boolean downloadFile(TroilkattFile tf) {
		DataTable.downloadFile(tf, tf.getDir(), this.conf, this.logger);
		return true;
	}
}
