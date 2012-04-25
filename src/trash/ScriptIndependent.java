package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;

/**
 * Execute an independent script that:
 * 1. Reads the input files
 * 2. Executes a script (once even if multiple datasets are processed).
 * 3. Outputs the input files
 */
public class ScriptIndependent extends ScriptPerDir {	
	
	/**
	 * @param args: script to execute
	 * For the remaining arguments refer to the superclass
	 */
	public ScriptIndependent(String name, String args, String stageDir, String logDir,
			String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable,
			TroilkattProperties troilkattProperties, boolean processAll,
			boolean processNew, boolean processUpdated, boolean processDeleted) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties, processAll, processNew,
				processUpdated, processDeleted);		
	}
	
	/**
	 * Function called to filter all files produced by the previous step.
	 * 
	 * @param outputFiles: an array list where the fileanme of all output files produced by this stage
	 *   are saved.
	 * @param logs: an array list where all logfile namesproduced by this stage are saved.
	 * @param objects: a map used to return objects to be saved in Hbase.
	 * 
	 * @return outputFiles and logs: modified as described above.
	 */
	public void processAll(ArrayList<TroilkattFile> outputFiles, ArrayList<String> logs, HashMap<String, Object> objects) {    
		logger.debug("Process all files");

		process("all");

		for (TroilkattFile tf: prevStage.allFiles) {
			outputFiles.add(tf);
		}
		for (String s: getOutputFiles(false)) {
			outputFiles.add(createTroilkattFile(outputDir, s));
		}        
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}
		getObjects(objects);
	}   
	
	/**
	 * Function called to process new files produced by the previous step.
	 *
	 * The default implementation does the following:
	 * 1. Get the list of the updated files
	 * 2. Call process() using the updated files list as argument
	 * 3. Return lists of files added to the output and log directory
	 *
	 * @param outputFiles: an array list where the fileanme of all output files produced by this stage
	 *   are saved.
	 * @param logs: an array list where all logfile namesproduced by this stage are saved.
	 * @param objects: a map used to return objects to be saved in Hbase.
	 * 
	 * @return outputFiles, and logs: modified as described above.
	 */
	public void processNew(ArrayList<TroilkattFile> outputFiles, ArrayList<String> logs, HashMap<String, Object> objects) {
		logger.debug("Process new files");				
				
		process("new");

		for (TroilkattFile tf: prevStage.newFiles) {
			outputFiles.add(tf);
		}		
		// Ok to return the same output and logfile in multiple processX functions since 
		// duplicates will be removed later        
		for (String s: getOutputFiles(false)) {
			outputFiles.add(createTroilkattFile(outputDir, s));
		}        
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}		
		getObjects(objects);
	}

	/**
	 * Function called to process updated files produced by the previous step.
	 * 
	 * The default implementation does the following:
	 * 1. Get the list of the updated files
	 * 2. Call process() using the updated files list as argument
	 * 3. Return lists of files added to the output and log directory
	 *
	 * @param outputFiles: an array list where the fileanme of all output files produced by this stage
	 *   are saved.
	 * @param logs: an array list where all logfile namesproduced by this stage are saved.
	 * @param objects: a map used to return objects to be saved in Hbase.
	 * 
	 * @return outputFiles and logs: modified as described above.
	 */
	public void processUpdated(ArrayList<TroilkattFile> outputFiles, ArrayList<String> logs, HashMap<String, Object> objects) {
		logger.debug("Process updated files");

		process("updated");

		for (TroilkattFile tf: prevStage.updatedFiles) {
			outputFiles.add(tf);
		}        

		// Ok to return the same output and logfile in multiple processX functions since 
		// duplicates will be removed later        
		for (String s: getOutputFiles(false)) {
			outputFiles.add(createTroilkattFile(outputDir, s));
		}        
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}
		getObjects(objects);
	}
    
	/**
	 * Function called to handle files deleted in the previous step.
	 *    
	 * There is no information available about which input files are related to which
	 * output files, so this function does not do anything.
	 *      
	 */
	public void processDeleted(ArrayList<TroilkattFile> deletedFiles, ArrayList<String> logs, HashMap<String, Object> objects) {
		logger.debug("Process deleted files");

		process("deleted");

		for (TroilkattFile tf: prevStage.deletedFiles) {
			deletedFiles.add(tf);
		}       

		// Ok to return the same output and logfile in multiple processX functions since 
		// duplicates will be removed later            
		for (String s: getLogfiles(true)) {
			logs.add(s);
		}
		getObjects(objects);                                     
	}
        
	/**
	 * This function is called to process allFiles[], newFiles[],  updatedFiles[], and deletedFiles[]
	 *
	 * Process data by executing the specified program with the specified argument.
	 *
	 * @param files: list of filenames to process
	 * @param fileset: fileset being processed ('all', 'new', 'updated' or 'deleted')
	 */
	public void process(String fileset) {	    
		// Replace TROILKATT symbols
        String dirCmd = setTroilkattVariables(cmd, 
        		troilkattProperties.get("troilkatt.dir"), 
        		fileset);
        
        /*
		 * The input files are already in the output directory. However the input objects
		 * must be downlaoded from Hbase.
		 */
		if (saveObjects) {
			initObjectDirs(timestamp);
			pipelineTable.downloadStageObjects(this.name, objectInputDir, -1);
			dirCmd = setObjectDirVariables(dirCmd);
		}
    
        /* During the execution this thread is blocked
		 * Note that the output and error messages are not logged unless specified by the
		 * arguments string */
		 executeCmd(dirCmd);
		 
		 /*
		  * The outputfiles are put in the output directory and will be saved as usual. Modified
		  * stage specific objects are put in the object output directory and will be returned
		  * for storage (by the getObjects() function implemented in this subclass and called
		  * by the processX() functions.
		  */
	}                            

}
