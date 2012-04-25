package edu.princeton.function.troilkatt.pipeline;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.Table;

/**
 * Merge MapReduce reducer output files
 */
public class MergeReducerResults extends Stage {
	private String outputFilename = null;
	
	/**
	 * This stage does not have any arguments.
	 *
	 * @param args: output filename
	 * @param: see description for super-class
	 */
	public MergeReducerResults(String name, String args, String stageDir,
			String logDir, String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable,
			TroilkattProperties troilkattProperties, boolean processAll,
			boolean processNew, boolean processUpdated, boolean processDeleted) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties, processAll, processNew,
				processUpdated, processDeleted);	
		outputFilename = args;
	}

	
	/**
	 * Function called to process data.
	 * 
	 * @param files: list of filenames to process
	 * @param fileset: fileset being processed ('all', 'new', 'updated' or 'deleted')
	 *
	 * @return: null
	 */    	
	@Override
	public void process(String files[], String fileset) {        
		Arrays.sort(files);
		
        if ((files != null) && (files.length > 0)) {        	 
    		try {
    			BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
    			for (String f: files) {
    				String[] lines = Table.readTextFile(f);				
    				for (String l: lines) {
    					os.write(l);
    				}
    			}
				os.close();
    		} catch (IOException e) {
    			logger.fatal("Could not write to output file: " + outputFilename);
    			logger.fatal(e.toString());
    			throw new RuntimeException(e);
    		}	
        }
	}
}
