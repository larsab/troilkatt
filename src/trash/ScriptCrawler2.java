package trash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.pipeline.ScriptPerDir;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.utils.OsPath;

/**
 * Class for executing a crawler implemented as a Troilkatt Python script.
 * 
 * This class differs from ScriptCrawler in that it is intended to be used when
 * initializing a repository so it does not calculate SHA-1 value for downloaded files.
 */
public class ScriptCrawler2 extends Source {
	// ScriptPerDir has many useful functions for initializing the directories needed
	// and to execute the script
	ScriptPerDir dirScript = null;
	
	// Directory where files are downloaded
	String downloadDir;
	
	/**
	 * @param args: script, download dir, and script specific arguments
	 * 
	 * For other arguments refer do documentation for Stage constructor. 
	 */
	public ScriptCrawler2(String name, String arguments, String outputDir,
			String logDir, String tmpDir, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {
		super(name, arguments, outputDir, logDir, tmpDir, datasetTable,
				pipelineTable, troilkattProperties);
		
		// Creating the ScriptPerDir object will initialize the directories and
		// prepare the script for execution
		dirScript = new ScriptPerDir(name, args, stageDir, logDir, tmpDir,
				prevStage, datasetTable, pipelineTable, troilkattProperties);
		
		downloadDir = OsPath.join(stageTmpDir, "download");
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
		/*
		 * Execute script
		 */
		logger.info("Crawl at: " + timestamp);
		
	    
        // Replace TROILKATT symbols
        String dirCmd = setTroilkattVariables(dirScript.cmd, downloadDir, "crawl");
        
        /*
		 * The input files are already in the output directory. However the input objects
		 * must be downlaoded from Hbase.
		 */
		if (dirScript.saveObjects) {
			dirScript.setLocalFSDirs(timestamp);
			dirScript.initObjectDirs(timestamp);
			pipelineTable.downloadStageObjects(this.name, dirScript.objectInputDir, -1);
			dirScript.deleteJavaObjects(dirScript.objectInputDir);
			dirCmd = dirScript.setObjectDirVariables(dirCmd);
		}
    
        /* During the execution this thread is blocked.
		 * Note that the output and error messages are not logged unless specified by the
		 * arguments string */
		 executeCmd(dirCmd);
		 
		 /*
		  * The outputfiles are put in the output directory and will be saved as usual. Modified
		  * stage specific objects are put in the object output directory and will be returned
		  * for storage (by the getObjects() function).
		  */		 
		 dirScript.getObjects(objects);
		 logger.debug("Script created " + objects.size() + " objects");
		 
		 /*
		  * Save files in output directory
		  */
		 
		// Get list of new files in the output directory
		String currentFiles[] = getRecursiveFileList(outputDir, false);
		

		Vector<String> newFiles = new Vector<String>();
		for (String f: currentFiles) {					
			newFiles.add(f);			
		}  

		/* 
		 * Update return data structures 
		 */
		for (String f: newFiles) {
			retrievedFiles.add(this.createTroilkattFile(outputDir, f));					
		}		
		// No files are deleted
		
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}		
	}
}
