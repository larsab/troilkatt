package trash;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.utils.OsPath;

/**
 * Run a map-reduce program implemented as a java class that handles it's own output. 
 * That is it saves the meta-data in the pipeline table.
 */
@Deprecated
public class MapReduceAppNoOutput extends MapReduce {
	protected String mapreduceApp = null;
	/**
	 * The first word in the arguments specify the stage to be run and its arguments.
	 * 
	 * @param args: 
	 * @param: see description for ExecutePerFile class
	 */
	public MapReduceAppNoOutput(String name, String args, String stageDir, String logDir,
			String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable,
			TroilkattProperties troilkattProperties, boolean processAll,
			boolean processNew, boolean processUpdated, boolean processDeleted) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties, processAll, processNew,
				processUpdated, processDeleted);
		initStage();
	}

	public MapReduceAppNoOutput(String name, String args, String stageDir, String logDir,
			String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties);
				
		initStage();
	}
	
	/**
	 * Constructor helper function
	 */
	private void initStage() {
        String[] argsParts = args.split(" ");
        if (argsParts.length < 1) {
        	logger.fatal("Stage type to execute is not specified");
        	throw new RuntimeException("Invalid arguments (no stageType)");
        }
        
        mapreduceApp = argsParts[0];
        stageArgs = null;
        for (int i = 1; i < argsParts.length; i++) {
        	String p = argsParts[i];

        	if (p.equals(">")) {
        		p = "TROILKATT.REDIRECT_OUTPUT";
        	}
        	else if (p.equals("2>")) {
        		p = "TROILKATT.REDIRECT_ERROR";
        	}
        	else if (p.equals("<")) {
        		p = "TROILKATT.REDIRECT_INPUT";
        	}

        	p = p.replace(";", "TROILKATT.SEPERATE_COMMAND");

        	if (stageArgs == null) {
        		stageArgs = p;
        	}
        	else {
        		stageArgs = stageArgs + " " + p;
        	}
        }
	}
	
	/**
	 * Wrapper function called by the main loop to process the data
	 * 
	 * @param timestamp: timestamp used for Hbase updates.
	 */
	@Override
	public void process2(long timestamp) {
		logger.debug("Start process2() at " + timestamp);

		// Set iteration specific local variables        
		this.timestamp = timestamp;
		setLocalFSDirs(timestamp);
		hdfsOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), 
				"output/" + name + "-" + timestamp);

		logger.debug("Do processing for stage: " + name);
		if (doProcessAll) {
			doMapReduce("all");            
		}
		if (doProcessNew) {
			doMapReduce("new");     
		}   
		if (doProcessUpdated) {
			doMapReduce("updated");            
		}        
		if (doProcessDeleted) {
			doMapReduce("deleted");
		}

		// The output files and meta-data are already saved by the MapReduce program
		
		// Logs and objects are saved as redirects
		
		// No cleanup necessary
		
		logger.debug("Process2() done at " + timestamp);
	}
	
	/**
	 * Run the MapReduce program and update file lists.
	 * 
	 * @param filelist: filelist to process
	 */
	public void doMapReduce(String filelist) {
		String localDir = OsPath.join(troilkattProperties.get("troilkatt.dir"), "mapreduce");	
		localDir = OsPath.join(localDir, getName() + "/" + timestamp);
		
		/*
		 * Run ExecuteStage map-reduce program
		 */
		String mapReduceCmd = troilkattProperties.get("troilkatt.mapreduce.jar") + " " + mapreduceApp + " " +
		"-libjars /nhome/larsab/skarntyde/troilkatt-java/bin,/nhome/larsab/apps/hbase-0.20.0/hbase-0.20.0.jar,/nhome/larsab/lib/java/commons-cli.jar,/nhome/larsab/apps/zookeeper-3.2.1/zookeeper-3.2.1.jar,/nhome/larsab/lib/java/log4j-1.2.jar " +
		troilkattProperties.getConfigFile() + " " +	                // [0]
		getName() + " " +                                           // [1]
		prevStage.getName() + " " +                                 // [2]
		filelist + " " +                                            // [3]
		getDatasetTableType() + ":" + getDatasetTableName() + " " + // [4]
		hdfsOutputDir + " " +                                       // [5]
		timestamp + " " +                                           // [6]
		localDir;                                                   // [7]
		
		if (stageArgs != null) {
			mapReduceCmd = mapReduceCmd +  " " + stageArgs;
		}		
		
		System.out.println(String.format("Execute: %s", mapReduceCmd));
		logger.debug("Execute: " + mapReduceCmd);
		Process child;
		try {
			child = Runtime.getRuntime().exec(mapReduceCmd);
		} catch (IOException e) {
			logger.fatal("IOException: " + e.toString());
			throw new RuntimeException(e);
		}

		/* 
		 * Wait until the cmd completes
		 */
		System.out.println("Waiting for command to complete");
		logger.debug("Waiting for command to complete");
		try {
			child.waitFor();
		} catch (InterruptedException e) {
			logger.fatal("Interrupted: " + e.toString());
			throw new RuntimeException(e);
		}				
		
		/*
		 * Save mapreduce stdout and stderr
		 */		
		String inputLogfile = OsPath.join(logDir, "mapreduce-" + filelist + ".output");
		String errorLogfile = OsPath.join(logDir, "mapreduce-" + filelist + ".error");
		System.out.println("Saving mapreduce stdout and stderror in: " + logDir);
		logger.debug("Saving mapreduce stdout and stderror in: " + logDir);
		logger.debug("Additional logfiles are in: " + OsPath.join(localDir, "log"));
		
		BufferedReader stdInput = new BufferedReader(new 
				InputStreamReader(child.getInputStream()));

		BufferedReader stdError = new BufferedReader(new 
				InputStreamReader(child.getErrorStream()));
												
		try {
			String s;
			
			// read the output from the command
			PrintWriter logInput = new PrintWriter(new FileWriter(inputLogfile, true));
			while ((s = stdInput.readLine()) != null) {
				logInput.println(s);
			}
			logInput.close();
			
			// read any errors from the attempted command
			PrintWriter logError = new PrintWriter(new FileWriter(errorLogfile, true));
			while ((s = stdError.readLine()) != null) {
				logError.println(s);
			}
			logError.close();
			
		} catch (IOException e) {
			logger.fatal("Failed to save command std or error output");
			logger.fatal(e.toString());
			logger.fatal(e.getStackTrace().toString());
			throw new RuntimeException(e);
		}
		
		/*
		 * Makre sure the program exited normally
		 */
		if (child.exitValue() != 0) {
			logger.warn("Command failed: " + mapReduceCmd);
			logger.warn("Exit value: " + child.exitValue());
			// User provided programs may fail in unexpected manners, the processing however
			// should continue
			
			//DEBUG: throw new RuntimeException("Failed to execute command: " + child.exitValue());
		}
				
		/*
		 * Get updated filelists. Only the filelists are downloaded, the files
		 * are not downloaded until needed
		 */
		allFiles = pipelineTable.getFilelist(getName(), "all", timestamp);
		newFiles = pipelineTable.getFilelist(getName(), "new", timestamp);
		updatedFiles = pipelineTable.getFilelist(getName(), "updated", timestamp);
		deletedFiles = pipelineTable.getFilelist(getName(), "deleted", timestamp);
	}
}
