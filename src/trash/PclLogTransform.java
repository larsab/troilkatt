package edu.princeton.function.troilkatt.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.utils.OsPath;

public class PclLogTransform extends Stage {	
	protected String statsFilename;
	
	protected String prepareCmd;
	protected String calcValueStatsCmd;
	protected String createMissingValuesCmd;
	protected String logTransformCmd;
	
	/**
    * This stage does not have any arguments.
    *
    * @param: see description for super-class
    */
	public PclLogTransform(String name, String args, String stageDir,
			String logDir, String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable,
			TroilkattProperties troilkattProperties, boolean processAll,
			boolean processNew, boolean processUpdated, boolean processDeleted) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties, processAll, processNew,
				processUpdated, processDeleted);
		initStage();
	}

	public PclLogTransform(String name, String args, String stageDir,
			String logDir, String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties);
		initStage();
	}

	/**
	 * Constructor helper function.
	 */
	private void initStage() {
		this.statsFilename = "stats.txt";
		this.prepareCmd = "rm TROILKATT.LOG_DIR/" + statsFilename;
		this.calcValueStatsCmd = String.format("ruby TROILKATT.UTILS/calcValueStats.rb TROILKATT.INPUT_DIR/TROILKATT.FILE >> TROILKATT.LOG_DIR/%s 2> TROILKATT.LOG_DIR/calcValueStats.TROILKATT.FILESET.error", statsFilename);
		this.createMissingValuesCmd = "ruby TROILKATT.UTILS/createMissingValues.rb TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR/TROILKATT.FILE.mv 3 > TROILKATT.LOG_DIR/createMissingValues.TROILKATT.FILESET.output 2> TROILKATT.LOG_DIR/createMissingValues.TROILKATT.FILESET.error";
		this.logTransformCmd = "perl TROILKATT.UTILS/log_transform.pl TROILKATT.OUTPUT_DIR/TROILKATT.FILE.mv TROILKATT.OUTPUT_DIR/TROILKATT.FILE.mv.log 2 2 > TROILKATT.LOG_DIR/logTransform.TROILKATT.FILESET.output 2> TROILKATT.LOG_DIR/logTransform.TROILKATT.FILESET.error";
	}	                   
    
	/**
	 * Function called to process deleted files produced by the previous step.
	 * 
	 * The subclass must implement this function.
	 *      
	 * * @return (deletedFiles, logs, objects):
	 *   deletedFiles: list of absolute filenames of files deleted in the output directory
	 *   logs: list of log files that should be saved in Hbase.
	 *   objects: dictionary with id -> object mappings for Python objects that should be saved 
	 *   in Hbase.    
	 */
	public void processDeleted(ArrayList<TroilkattFile> deletedFiles, ArrayList<String> logs, HashMap<String, Object> objects) {
		logger.fatal("processDeleted() not implemented for PCL log transform");
		throw new RuntimeException("processDeleted() not implemented for PCL log transform");
	}
    
	/**
	 * Function called to process data.
	 * 
	 * @param files: list of filenames to process
	 * @param fileset: fileset being processed ('all', 'new', 'updated' or 'deleted')
	 *
	 * @return: null
	 */    
	public void process(String files[], String fileset) {        
        if (files == null) {
            return;
        }
        else if (files.length == 0) {
            return;
        }
        
        String dirPrepareCmd = setTroilkattVariables(prepareCmd, 
        		prevStage.getOutputDir(), fileset);
        String dirCalcValueStatsCmd = setTroilkattVariables(calcValueStatsCmd, 
        		prevStage.getOutputDir(), fileset);
        String dirCreateMissingValuesCmd = setTroilkattVariables(createMissingValuesCmd, 
        		prevStage.getOutputDir(), fileset);
        String dirLogTransformCmd = setTroilkattVariables(logTransformCmd, 
        		prevStage.getOutputDir(), fileset);
        
        // Prepare for statistics calculation (delete any existing data in stats.txt file)
        executeCmd(dirPrepareCmd);
        
        // Do the statistics calculation for all files 
        for (String f: files) {
            String fileCalcValueStatsCmd = setTroilkattFilename(dirCalcValueStatsCmd, f);
            executeCmd(fileCalcValueStatsCmd);
        }

        // Parse the stats.txt file produced by executing the above commands
        String statsPath = OsPath.join(logDir, statsFilename);
        HashMap<String, Float> fmax = parseStatsFile(statsPath);

        // Whether to do the next two steps depend on the results of the statistics calculation    
        for (String f: files) {
        	if (! fmax.containsKey(f)) {
        		logger.warn(String.format("File %s not found in stats file: %s", f, statsPath));
        		//throw new RuntimeException("Stat calculation failed");
        		continue;
        	}
        	float maxV = fmax.get(f).floatValue();
        	
        	if (maxV > 50) {
        		String fileCreateMissingValuesCmd = setTroilkattFilename(dirCreateMissingValuesCmd, f);
        		executeCmd(fileCreateMissingValuesCmd);

        		String fileLogTransformCmd = setTroilkattFilename(dirLogTransformCmd, f);
        		executeCmd(fileLogTransformCmd);
        	}
        	else {                    
        		String dst = OsPath.join(outputDir, OsPath.basename(f) + ".log");
        		logger.debug("Copy " + f + " to " + dst);
        		try {
					OsPath.copy(f, dst);
				} catch (IOException e) {
					logger.fatal(String.format("Could not copy file %s to %s", f, dst));
					logger.fatal(e.toString());
					throw new RuntimeException("File copy failed");
				}
        	}
        }
	}

	/**
    * Parse stats.txt file created by the calcValueStats script
    *
    * @param filename: the stats.txt file
    * 
    * @return: HashMap<String, Float> with filename -> max value mappings
    */
    HashMap<String, Float> parseStatsFile(String filename) {
        logger.debug("Parse stats file: " + filename);
        
        String[] lines;
		try {
			lines = readTextFile(filename);
		} catch (IOException e) {
			logger.fatal("Could not read stats file: " + filename);
			logger.fatal(e.toString());
			throw new RuntimeException("Could not read stats file: " + filename);
		}
        HashMap<String, Float> fmax = new HashMap<String, Float>();             
        
        for (String l: lines) {
            int rangeStart = l.indexOf('[');
            int rangeEnd = l.lastIndexOf(']');                        
            String key = l.split("\t")[0];
            
            if ((rangeStart != -1) && (rangeEnd != -1)) {
                String rangeSubStr = l.substring(rangeStart + 1, rangeEnd);
                String [] rangeParts = rangeSubStr.split(",");
                if (rangeParts.length != 2) {
                	logger.warn("stats file range error in line: " + l);
                	continue;
                	//throw new RuntimeException("stats file parse error");
                }
                
                Float maxV = new Float(rangeParts[1]);
                System.out.println("put: " + key + ", " + maxV);
                fmax.put(key, maxV);
            }
            /* else: all other lines are ignored */
        }
                
        return fmax;
    }
}
