package trash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.ExperimentData;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.Table;
import edu.princeton.function.troilkatt.tables.TableFactory;

/**
 * Check a info file for new PCL files to process
 */
public class GeoInfoCrawler extends Source {	
	private String infoFilename = null;
	
	// Key: GSD filename, Entry: info_file line for that filename
	private HashMap<String, String> infoMap = null;
	
	// Datset table for PCL files
	ExperimentData pclTable = null;
	
	// List of organisms to output
	String[] organisms = null;
	
	/**
	 * @param arguments: 
	 *  [0] info filename
	 *  [1] dataset table type
	 *  [2] dataset table name
	 *  [3] organism (comma separated list)
	 * For the remaining parameters see the superclass documentation.
	 */
	@SuppressWarnings("unchecked")
	public GeoInfoCrawler(String name, String arguments, String outputDir,
			String logDir, String tmpDir, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {				
		super(name, arguments, outputDir, logDir, tmpDir, datasetTable,
				pipelineTable, troilkattProperties);
		
		String[] argsParts = arguments.split(" ");
		if (argsParts.length < 4) {
			logger.fatal("Invalid arguments: " + arguments);
			throw new RuntimeException("Invalid arguments: " + arguments);
		}
		
		infoFilename =  setTroilkattVariables(argsParts[0], null, "crawl");
		pclTable = (ExperimentData) TableFactory.newTable(argsParts[1], argsParts[2], 
				conf, hbConf, troilkattProperties, logger);
		
		String organismsStr = null;
		for (int i = 3; i < argsParts.length; i++) {
			if (organismsStr == null) {
				organismsStr = argsParts[i];
			}
			else {
				organismsStr = organismsStr + " " + argsParts[i];
			}
		}
			
		organisms = organismsStr.toLowerCase().split(",");		
		
		//Object o = pipelineTable.getObject(name, "infoMap", -1);
		Object o = null;
        if (o == null) {
        	infoMap = new HashMap<String, String>();
        }
        else {
        	infoMap = (HashMap<String, String>)o;
        }
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
		
		//System.err.println("Warning: ignoring infoMap");
		//infoMap = new HashMap<String, String>();
		
		/*
		 * Read the list of GSD files the input file
		 */
		logger.debug("Read and parse info file");
			
		String[] lines = null;
		try {
			lines = Table.readTextFile(infoFilename);	
		} catch (IOException e) {
			logger.fatal("Could not read from file: " + infoFilename);
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}	
			
		for (int i = 1; i < lines.length; i++) {			
			String dataLine = lines[i];
			String[] cols = dataLine.split("\t");
			if (cols.length != 24) {
				logger.fatal("Invalid column count in .info file: " + cols.length);
				throw new RuntimeException("Invalid column count in .info file: " + cols.length);
			}
			
			String id = cols[0];
			String org = cols[2].toLowerCase();
			
			for (String o: organisms) {
				if (! o.equals(org)) { // not in list of organisms to crawl for
					continue;
				}

				if ((! infoMap.containsKey(id)) || 
						(infoMap.containsKey(id) && (! infoMap.get(id).equals(dataLine)))) {

					logger.debug("Adding new PCL file: " + id);

					String filename = id.split("\\.")[0] + ".pcl";

					String hdfsFilename = pclTable.getHDFSFilename(filename);
					if (hdfsFilename != null) { // New or changed entry							
						retrievedFiles.add(new TroilkattFile(hdfsFilename, 
								troilkattProperties.get("troilkatt.tfs.root.dir"),
								outputDir,
								logger));
						infoMap.put(id, dataLine);
					}	
					else {
						logger.warn(String.format("File %s not found in HDFS table: %s", id, pclTable.getName()));
					}					
				}
				break;				
			}
		}
		
		// No files are deleted
		
		// There are no logfiles		
		
		// The updated infoMap object is saved
		objects.put("infoMap", infoMap);
	}
	
	/**
	 * Helper function to downaload a file from the dataset table.
	 * 
	 * @param tf: TroilkattFile handle of file to download
	 * @return: true if file was downloaded, false otherwise.
	 */
	public boolean downloadFile(TroilkattFile tf) {
		pclTable.downloadFile(tf, tf.getDir());
		return true;		
	}
}
