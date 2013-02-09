package edu.princeton.function.troilkatt.pipeline;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.Level;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

import edu.princeton.function.troilkatt.fs.OsPath;

/** 
 * Superclass to execute a stage as a MapReduce job. The MapReduce job to execute should be 
 * a subclass of edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce
 * 
 * The subclass MapReduceStage can be used to execute a stage in parallel using MapReduce.
 */
public class SGE extends Stage {
	// The stage arguments
	protected String stageArgs; 
	// File with input arguments to SGE executed program
	public String argsFilename;
	// Command used to run MapReduce job
	protected String sgeDir;
	protected String sgeCmd;
	
	/**
	 * Constructor.
	 * 
	 * @param args stage arguments
	 * @param see description for ExecutePerFile class
	 */
	public SGE(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args,
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		
		sgeDir = troilkattProperties.get("troilkatt.globalfs.sge.dir");			
		argsFilename = OsPath.join(sgeDir, "input.args");
				
		sgeCmd = " " + argsFilename;
	}
	
	/**
	 * The downloadInputFiles is overridden such that the input files are not downloaded.
	 * Instead the SGE tasks will do this.
	 */
	@Override
	public ArrayList<String> downloadInputFiles(ArrayList<String> hdfsFiles) throws StageException {
		return hdfsFiles;
	}
	
	/**
	 * The save output files is overridden such that no files are saved. Instead the SGE tasks will do this.
	 * 
	 */
	@Override
	public ArrayList<String> saveOutputFiles(ArrayList<String> hdfsFiles, long timestamp) throws StageException {
		return hdfsFiles;
	}
	
	/**
	 * Helper function to create an arguments file. The filename is given by the argsFilename global
	 * variable.
	 * 
	 * edu.princeton.function.troilkatt.sge.ExecuteStage.readSHEArgsfile is the corresponding method
	 * to read the arguments file
	 * 
	 * @param inputFiles of input files to process
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timstamp for this iteration
	 * @return none
	 * @throws StageException if file could not be created
	 */
	public void writeSGEArgsFile(String nfsTmpOutputDir, String nfsTmpLogDir, long timestamp) throws StageException {
		// Note! write order of lines must match read order in TroilkattMapReduce.readMapReduceArgsFile
		try {
			PrintWriter out = new PrintWriter(new FileWriter(argsFilename));
			out.println("configuration.file = " + troilkattProperties.getConfigFile());
			out.println("pipeline.name = " + pipelineName);			
			out.println("stage.name = " + stageName);			
			out.println("stage.args = " + stageArgs);
			out.println("nfs.output.dir = " + nfsTmpOutputDir); // tmp storage for output files
			out.println("compression.format = " + compressionFormat);
			out.println("storage.time = " + storageTime);
			out.println("nfs.log.dir = " + nfsTmpLogDir); // tmp storage for log files		
			out.println("nfs.meta.dir = " + hdfsMetaDir); // shared among all tasks
			String localSgeDir;
			try {
				localSgeDir = troilkattProperties.get("troilkatt.localfs.sge.dir");
			} catch (TroilkattPropertiesException e) {
				logger.fatal("Invalid properies file: " + e.getMessage());
				throw new StageException("Could not create input arguments file");
			}
			out.println("sge.pipeline.dir = " + OsPath.join(localSgeDir, "pipeline"));
			out.println("sge.tmp.dir = " + OsPath.join(localSgeDir, "tmp"));
			//out.println("mapred.output.dir = " + OsPath.join(mapreduceDir, "output"));
			//out.println("mapred.meta.dir = " + OsPath.join(mapreduceDir, "meta"));
			//out.println("mapred.global-meta.dir = " + OsPath.join(mapreduceDir, "global-meta"));
			//out.println("mapred.tmp.dir = " + OsPath.join(mapreduceDir, "tmp"));			
			
			//Logger lp = logger;
			Level level = logger.getLevel(); //null;			
			//while (lp != null) {
			//	level = lp.getLevel();
			//	if (level != null) {
			//		break;
			//	}
			//	else {
			//		lp = lp.getParent();
			//	}
			//}			
			String logLevelName;
			if (level == null) { // could not find level: set to info as fallback
				logger.warn("Could not find logging level");
				logLevelName = "info";
			}
			else {
				logLevelName = level.toString();
			}
			
			out.println("logging.level = " + logLevelName);			
			out.println("timestamp = " + timestamp);
			
			//out.println("soft.max.memory.mb = " + troilkattMaxVMem);
			//out.println("hard.max.memory.mb = " + taskMaxVMem);			
			out.close();
		} catch (IOException e1) {
			logger.fatal("Could not create input arguments file: " + e1.getMessage());
			throw new StageException("Could not create input arguments file");
		}
	}
	
	/**
	 * Handle MapReduce job output files. These are stored in the stage's HDFS tmp directory,
	 * and it may contain named files, reduce output files (part-r-*), and other hadoop
	 * specific files.
	 * 
	 * This function ignores map output files (part-m-*) and hadoop internal files such as
	 * _logs/* or _SUCESS.
	 * 
	 * This function timstamps, moves, and potentially compress all files in the HDFS tmp directory 
	 * to the DFS output directory, except the Hadoop internal files specified in the global variable 
	 *  hadoopInternalOutputFiles
	 * 
	 * It is possible to overwrite this function in order to for example merge and rename the
	 * reduce output files, or to save map output files 
	 * 
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timestamp timestamp to add to output files
	 * @return list of output files saved in the HDFS output directory.
	 * @throws StageException 
	 */
	protected void moveSGELogFiles(String nfsTmpLogDir, ArrayList<String> logFiles) throws StageException {	
		ArrayList<String> tmpFiles;
		try {
			tmpFiles = tfs.listdir(nfsTmpLogDir);			
		} catch (IOException e) {
			logger.fatal("Could not read list of outputfiles in NFS: " + e.toString());
			throw new StageException("Could not read list of outputfiles in HDFS");
		}
		
		if (tmpFiles == null) {
			return;
		}
		
		for (String f: tmpFiles) {			
			String relName = OsPath.absolute2relative(f, nfsTmpLogDir);
			String newName = OsPath.join(stageLogDir, relName);
			String dirName = OsPath.dirname(newName);
			
			if (! OsPath.isdir(dirName)) {
				if (! OsPath.mkdir(dirName)) {
					logger.warn("Could not create directory: " + dirName);
					logger.warn("Skipping log file: " + f);
					continue;
				}
			}
			
			if (OsPath.rename(f, newName) == false) {
				logger.warn("Could not move log file to: " + newName);
				logger.warn("Skipping log file: " + f);
				continue;
			}
			
			logFiles.add(newName);			
		}		
	}

	/**
	 * Run the MapReduce program and update file lists.
	 * 
	 * @param inputFiles list of HDFS input files to process
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files
	 * @throws StageException if stage cannot be executed
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {						
		
		// Temporary SGE output directory on NFS
		String nfsTmpOutputDir = OsPath.join(hdfsTmpDir, getStageID() + "-" + timestamp + "/output");
		// Root for task log files
		String nfsTmpLogDir = OsPath.join(hdfsTmpDir, getStageID() + "-" + timestamp + "/log");
		
		// Create arguments file for MapReduce Job-task
		writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, timestamp);
		
		// Redirect output and execute MapReduce job
		String outputLogfile = OsPath.join(stageLogDir, "sge.output");
		String errorLogfile = OsPath.join(stageLogDir, "sge.error");
		int rv = Stage.executeCmd(sgeCmd + " > " + outputLogfile + " 2> " + errorLogfile, logger);

		// Always update log files even if job crashes
		updateLogFiles(logFiles);
		if (rv != 0) {
			logger.warn("Mapreduce job failed with error code: " + rv);
			throw new StageException("Mapreduce job failed");
		}
		
		// Move all log files to a single directory on local fs
		moveSGELogFiles(nfsTmpLogDir, logFiles);
		
		// Update list of meta and log files 
		updateMetaFiles(metaFiles);		
		
		try {
			return tfs.listdir(nfsTmpOutputDir);
		} catch (IOException e) {
			logger.error("Could not list output dir" + e);
			throw new StageException("Could not list output dir");
		}
	}
}
        
        
