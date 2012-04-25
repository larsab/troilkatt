package trash;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.SinkTable;
import edu.princeton.function.troilkatt.tables.Table;
import edu.princeton.function.troilkatt.utils.OsPath;

/**
 * Check a file for GSE identifiers used to retrieve the set of SOFT files to process.
 */
public class GeoFileCrawler extends Source {	
	// Directory where files are downloaded
	String inputFilename = null;
	
	String geoSink = null;
	SinkTable sinkTable = null;
	
	/**
	 * @param arguments: geo sink, and input filename
	 * For the remaining parameters see the superclass documentation.
	 */
	public GeoFileCrawler(String name, String arguments, String outputDir,
			String logDir, String tmpDir, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {				
		super(name, arguments, outputDir, logDir, tmpDir, datasetTable,
				pipelineTable, troilkattProperties);
		
		String[] argParts = arguments.split(" ");
		if (argParts.length != 2) {
			logger.fatal("Invalid arguments: %s");
			logger.fatal("Arguments should be: geo-sink-name input-filename");
			throw new RuntimeException("Invalid arguments");
		}
		geoSink = argParts[0];		
		inputFilename =  setTroilkattVariables(argParts[1], null, "crawl");
				
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
	@Override
	public void crawl(ArrayList<TroilkattFile> retrievedFiles, ArrayList<TroilkattFile> deletedFiles, ArrayList<String> logs, HashMap<String, Object> objects, long timestamp) {
		logger.info("Crawl at: " + timestamp);
		
		/*
		 * Read the list of files to process from the input file
		 */
		logger.debug("Read and parse input file");
		String[] gses;
		BufferedWriter newFp;
		BufferedWriter nfFp;
		try {
			gses = Table.readTextFile(inputFilename);
			newFp = new BufferedWriter(new FileWriter(new File(OsPath.join(logDir, "new"))));		
			nfFp = new BufferedWriter(new FileWriter(new File(OsPath.join(logDir, "not_in_sink"))));
		} catch (IOException e) {
			logger.fatal("Could not open input file or one of the output files");
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}				
		
		logger.debug("Get geo sink file list");
		ArrayList<TroilkattFile> sinkFiles = sinkTable.getAllFiles(geoSink, -1);					

		for (String gse: gses) {
			try {
				gse = gse.trim().replace("\n", "");

				boolean found = false;
				for (TroilkattFile tf: sinkFiles) {
					String filename = tf.getFilename();
					if (filename.contains(gse)) {
						tf.setDir(outputDir);
						retrievedFiles.add(tf);					
						newFp.write(gse + "\n");				
						found = true;
						break;
					}
				}

				if (! found) {
					nfFp.write(gse + "\n");
				}
			} catch (IOException e) {
				logger.fatal("Could not write to log file");
				logger.fatal(e.toString());
				throw new RuntimeException(e);
			}
		}
		
		// No files are deleted
		
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}
		
		// There are no objects returned
	}
}
