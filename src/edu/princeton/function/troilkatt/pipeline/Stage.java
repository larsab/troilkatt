package edu.princeton.function.troilkatt.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.utils.Utils;

/**
 * Super-class for all stage modules. Note that MapReduce programs should use
 * MapReduceStage instead.
 * 
 * The main loop in Troilkatt calls the process2() function in this class that does the
 * following:  
 * 1. Download all files from TFS to local GS 
 * 2. Call process functions that must be implemented by the sub-class to process the data
 * 3. Save all files in log, meta, and output directory to TFS 
 * 4. Call cleanup() in previous stage
 * 
 * Subclasses that inherit from Stage must implement:
 * - process() in order to process the output files
 * 
 * Subclasses may override:
 * - process2() if the default: download, process, save files needs to be changed
 * - recover() if stage specific recovery should be done
 * - cleanup() for stage specific cleanup
 */
public class Stage {
	// Constants
	public static final String META_COMPRESSION = "tar.gz"; // compression of meta-data files
	public static final String LOG_COMPRESSION = "tar.gz";  // compression of log files
	
	// Troilkatt symbols
	public static final String[] VALID_TROILKATT_SYMBOLS = {
		"TROILKATT.INPUT_DIR", "TROILKATT.OUTPUT_DIR",
		"TROILKATT.LOG_DIR", "TROILKATT.META_DIR", "TROILKATT.TMP_DIR", "TROILKATT.DIR",
		"TROILKATT.BIN", "TROILKATT.UTILS", "TROILKATT.GLOBALMETA_DIR",
		"TROILKATT.SCRIPTS", "TROILKATT.REDIRECT_OUTPUT", "TROILKATT.REDIRECT_ERROR",
		"TROILKATT.REDIRECT_INPUT", "TROILKATT.SEPERATE_COMMAND",
		"TROILKATT.FILE_NOEXT", "TROILKATT.FILE", "TROILKATT.JAR", "TROILKATT.CLASSPATH", 
		"TROILKATT.MONGODB_SERVER_HOST", "TROILKATT.MONGODB_SERVER_PORT"};
	
	/* Each stage has three logfiles: filelist.log, output.log, and error.log.
	 * The filelist.log automically created and contains a list of output files
	 * created by each stage (including meta and logfiles).
	 * The output.log and error.log can be used by a stage to write log information.
	 * In addition the main troilkatt.log can be written to using the logger interface. 
	 */
	public final static String filelistLogFilename = "filelist.log";
	
	// The stage name consist of the stage number and name specified in the
	// configuration file. The format is stageNum-name, where stage num is
	// three digits with leading zeros (example: 003-foo)
	public String stageName;
	// Properties specified in pipeline configuration file		
	public String args;	
	public String compressionFormat;
	public int storageTime; // in days
	
	protected String pipelineName;
	
	// Directories on local FS
	//protected String pipelineStageDir;
	protected String globalMetaDir;
	// Set in process2() for each iteration
	public String stageInputDir;
	public String stageLogDir;
	public String stageMetaDir;	
	public String stageTmpDir;
	public String stageOutputDir;
	
	// Directories in HDFS/NFS and Hbase tables/ log tar directory
	public String tfsOutputDir;	//
	public LogTable logTable;
	//protected String tfsPipelineMetaDir;
	protected String tfsGlobalMetaDir;
	// Set in the constructor
	public String tfsMetaDir;	
	public String tfsTmpDir;
		
	protected TroilkattProperties troilkattProperties;
	public Logger logger;	
	protected TroilkattFS tfs;
	
	/**
	 * Constructor 
	 *
	 * @param stageNum stage number in pipeline.
	 * @param name name of the stage.
	 * @param args stage specific arguments.
	 * @param outputDirectory output directory in TFS. The directory name is either relative to
	 * the troilkatt root data directory, or absolute (starts with either "/" or "hdfs://")
	 * @param compressionFormat compression to use for output files
	 * @param storageTime persistent storage time for output files in days. If -1 files
	 * are stored forever. If zero files are deleted immediately after pipeline execution is done.
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param tfsStageMetaDir meta file directory for this stage in tfs.
	 * @param tfsStageTmpDir tmp directory for this stage in tfs  (can be null).
	 * @param pipeline reference to the pipeline this stage belongs to.
	 * @throws TroilkattPropertiesException if there is an error in the Troilkatt configuration file
	 * @throws StageInitException if the stage cannot be initialized
	 */
	public Stage(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		
		this(stageNum, name, args, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);
		
		// this.args is initialized below
		if ((outputDirectory == null) || outputDirectory.isEmpty()) {
			this.tfsOutputDir = null;
			logger.warn("Output directory not set for stage: " + name);						
			this.compressionFormat = null;
			this.storageTime = 0;
		}
		else {
			if (outputDirectory.startsWith("/") || outputDirectory.startsWith("hdfs:/")) { // is absolute
				this.tfsOutputDir = outputDirectory;
			}
			else { // is relative to root
				this.tfsOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
						OsPath.join("data", outputDirectory));
			}
			
			// Create directory if it does not exist
			try {
				tfs.mkdir(this.tfsOutputDir);
			} catch (IOException e) {
				throw new StageInitException("Could not create output directory for stage: " + this.tfsOutputDir);
			}
						
			if (! TroilkattFS.isValidCompression(compressionFormat)) {
				throw new StageInitException("Invalid compression format: " + compressionFormat);
			}
			
			this.compressionFormat = compressionFormat;
			if (storageTime < -1) {
				throw new StageInitException("Invalid storage time: " + storageTime);
			}
			this.storageTime = storageTime;
			
		}
	}

	/**
	 * Constructor for stages that do not produce output files (such as sinks) 
	 *
	 * @param stageNum stage number in pipeline.
	 * @param name name of the stage.
	 * @param args stage specific arguments.
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param tfsStageMetaDir meta file directory for this stage in tfs.
	 * @param tfsStageTmpDir tmp directory for this stage in tfs  (can be null).
	 * @param pipeline pipeline reference to the pipeline this stage belongs to.	 
	 * @throws TroilkattPropertiesException if there is an error in the Troilkatt configuration file.
	 * @throws StageInitException if the stage cannot be initialized
	 */
	public Stage(int stageNum, String name, String args,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		
		name = name.trim();
		logger = Logger.getLogger("troilkatt.stage-" + name); 
		this.stageName = String.format("%03d-%s", stageNum, name);		
		// this.args is initialized below
		
		this.tfsOutputDir = null;
		this.compressionFormat = null;
		this.storageTime = 0;
				
		this.troilkattProperties = pipeline.troilkattProperties;
		this.tfs = pipeline.tfs;
		this.logTable = pipeline.logTable;
		this.pipelineName = pipeline.name;		
		
		globalMetaDir = troilkattProperties.get("troilkatt.globalfs.global-meta.dir");
		if (! OsPath.isdir(globalMetaDir)) {
			if (OsPath.mkdir(globalMetaDir) == false) {
				logger.fatal("Could not create global-meta directory: " + globalMetaDir);
				throw new StageInitException("Could not create global-meta directory: " + globalMetaDir);
			}
		}
		tfsGlobalMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "global-meta");		
				
		setCreateLocalFSDirs(localRootDir);
		try {
			setCreateTFSDirs(tfsStageMetaDir, tfsStageTmpDir);
		} catch (IOException e) {
			logger.fatal("Could not set (or create) TFS directories: " + e);
			throw new StageInitException("Could not set or create TFS directories for stage");
		}
		
		// Set troilkatt variables in arguments string
		// Note! must be initialized last since it depends on many of the other variables
		if (args == null) {
			this.args = null;
		}
		else {
			this.args = setTroilkattSymbols(args);
		}
	}
	
	/**
	 * Wrapper function called to process data that does the following:
	 * 
	 * 1. Download all files from TFS to local GS 
	 * 2. Call process functions that must be implemented by the sub-class to process the data
	 * 3. Save all files in log, meta, and output directory to TFS 
	 * 4. Delete tmp files
 	 *
 	 * @param inputTFSFiles list of input files to process. The list contains HDFS filenames. 
	 * @param timestamp timestamp added to output files.
	 * @return list of output HDFS filenames
	 * @throws StageException 
	 */
	public ArrayList<String> process2(ArrayList<String> inputTFSFiles, long timestamp) throws StageException {
		logger.debug("Start process2() at " + timestamp);

		// Download input and meta files
		ArrayList<String> inputFiles = downloadInputFiles(inputTFSFiles);
		ArrayList<String> metaFiles = downloadMetaFiles();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		// Do processing		
		ArrayList<String> outputFiles = null;
		StageException eThrown = null; // set to true in case of excpeption in process()
		ArrayList<String> tfsOutputFiles = null;
		try {
			outputFiles = process(inputFiles, metaFiles, logFiles, timestamp);
			// Only save output and meta files if job succeeded
			if (tfsOutputDir != null) {
				tfsOutputFiles = saveOutputFiles(outputFiles, timestamp);
			}
			else { // no output files should be saved
				logger.warn("Stage does not produce any output data");
				tfsOutputFiles = new ArrayList<String>();
			}
			saveMetaFiles(metaFiles, timestamp);
			logger.debug("Process2() done at " + timestamp);
		} catch (StageException e) {
			eThrown = e;
		}
 
		// Always save log files and do cleanup
		saveLogFiles(logFiles, timestamp);
		cleanupLocalDirs();
		cleanupTFSDirs();
		
		if (eThrown != null) {
			// Log files saved so we can now throw exception
			throw eThrown;
		}
				
		return tfsOutputFiles;
	}

	/**
	 * Recover from a crashed iteration. The default recovery just re-processes all files. 
	 * If needed, subclasses should implement stage specific recovery functions. 
	 * 
	 * @param inputFiles list of input files to process.
	 * @param timestamp timestamp added to output files.
	 * @return list of output files
	 * @throws StageException 	 
	 */
	public  ArrayList<String> recover( ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.debug("Recover stage at "  + timestamp);
		
		if ((tfsOutputDir != null) && (! tfsOutputDir.isEmpty())) {
			// Read files from previous iteration
			try {
				return tfs.listdirT(tfsOutputDir, timestamp);
			} catch (IOException e) {
				logger.fatal("Could not list files in outputdirectory: " + tfsOutputDir, e);
				throw new StageException("Could not recover files from last iteration");
			}
		}
		else {
			// Must re-do operation
			return process2(inputFiles, timestamp);
		}
	}
	
	/**
	 * Set local FS directory names, and create directories if they do not exist.
	 * 
	 * This function is only called from the constructor
	 * 
	 * @param localFSRootDir root directory on local FS for temporal files
	 * @throws StageInitException if a local FS directory could not be created.
	 */
	private void setCreateLocalFSDirs(String localFSRootDir) throws StageInitException {		
		if (! OsPath.isdir(localFSRootDir)) {
			if (OsPath.mkdir(localFSRootDir) == false) {
				logger.fatal("Could not create pipeline directory: " + localFSRootDir);
				throw new StageInitException("Could not create localFSRootDir directory: " + localFSRootDir);
			}
		}
		
		// Set stage direcotries
		stageInputDir = OsPath.join(localFSRootDir, String.format("%s/input", stageName));
		stageLogDir = OsPath.join(localFSRootDir, String.format("%s/log", stageName));
		stageOutputDir = OsPath.join(localFSRootDir, String.format("%s/output", stageName));
		stageMetaDir = OsPath.join(localFSRootDir, String.format("%s/meta", stageName));
		stageTmpDir = OsPath.join(localFSRootDir, String.format("%s/tmp", stageName));
		
		// Create directories if necessary
		String[] dirs = {stageInputDir, stageLogDir, stageOutputDir, stageMetaDir, stageTmpDir};
		for (String d: dirs) {		
			// If needed delete old data
			if (OsPath.isdir(d)) {				
				OsPath.deleteAll(d);
			}
			
			// and create new empty directory
			if (! OsPath.mkdir(d)) {
				logger.fatal("Failed to create directory on local FS: " + d);
				throw new StageInitException("Failed to create directory on local FS: " + d);
			}
		}		
	}

	/**
	 * Set HDFFS directory names, and create directories if they do not exist. 
	 * 
	 * This function is only called from the constructor
	 * 
	 * @param tfsStageMetaDir tfs metafile directory for this stage 
	 * @param tfsStageTmpDir tfs tmp directory for this stage (can be null)
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	private void setCreateTFSDirs(String tfsStageMetaDir, String tfsStageTmpDir) throws IOException, TroilkattPropertiesException {
		//tfsMetaDir = OsPath.join(tfsPipelineMetaDir, String.format("%03d-%s", stageNum, name));
		tfsMetaDir = tfsStageMetaDir;
		tfs.mkdir(tfsMetaDir);
		
		//tfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		tfsTmpDir = tfsStageTmpDir;
		
		if (tfsTmpDir != null) {
			// Delete any old files
			if (tfs.isdir(tfsTmpDir)) {
				tfs.deleteDir(tfsTmpDir);
			}
			tfs.mkdir(tfsTmpDir);
		}
	}

	/**
	 * Copy input files from TFS to local FS.
	 *  
	 * Note! All files in TFS will be put in the same directory on local FS even if they are
	 * in subdirectories in TFS
	 *  
	 * @param tfsFiles list of files to download from tfs
	 * @return list of local filenames (absolute filenames)
	 * @throws StageException if one or more files could not be downloaded
	 */
	public ArrayList<String> downloadInputFiles(ArrayList<String> tfsFiles) throws StageException {
		ArrayList<String> localFiles = new ArrayList<String>();
		for (String f: tfsFiles) {
			String ln;
			try {
				ln = tfs.getFile(f, stageInputDir, stageTmpDir, stageLogDir);
			} catch (IOException e) {
				logger.fatal("Could not download file: " + e.toString());
				throw new StageException("Could not download file: " + f);
			}
			if (ln == null) {
				throw new StageException("Could not copy file from TFS: " + f);
			}
			localFiles.add(ln);
		}
		return localFiles;
	}
	
	/**
	 * Copy  meta files from TFS to local FS.
	 *  
	 * @return list of downloaded filenames (local FS), or an empty list if the stage
	 * does not have any metadata files.
	 * @throws StageException if metafiles could not be downloaded
	 */
	public ArrayList<String> downloadMetaFiles() throws StageException {
		try {
			String newestMetaDir = tfs.getNewestDir(tfsMetaDir);
			if (newestMetaDir == null) {
				logger.info("No meta-data for stage");
				return new ArrayList<String>();
			}
			else {
				ArrayList<String> metaFiles = tfs.getDirFiles(OsPath.join(tfsMetaDir, newestMetaDir), stageMetaDir, stageLogDir, stageTmpDir);		
				if (metaFiles == null) {
					logger.fatal("Could not download meta file for stage");
					throw new StageException("Could not download meta file for stage");
				}
				return metaFiles;
			}
		} catch (IOException e) {
			logger.fatal("Could not download meta file for stage: " + e.toString());
			throw new StageException("Could not download meta file for stage");
		}	
	}

	/**
	 * Function called to process data. Sub-classes must implement this function.    
	 * 
	 * @param inputFiles list of input files to process.
	 * @param metaFiles list of meta files.
	 * @param logFiles list for storing log files.
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.fatal("Super-class process() called");
		throw new RuntimeException("process() not implemented in subclass");
	}

	/**
	 * Function called to save the output files created by this stage.
	 * 
	 * @param localFiles list of files on local FS to save
	 * @return list of filenames in TFS
	 * @throws StageException if one or more files could not be saved
	 */
	 public ArrayList<String> saveOutputFiles(ArrayList<String> localFiles, long timestamp) throws StageException {
		 ArrayList<String> tfsFiles= new ArrayList<String>();
		
		 if (tfsOutputDir == null) {
			 throw new RuntimeException("saveOutputFiles called for a stage where output directory is not set");
		 }
		 
		 if (localFiles.size() == 0) {
			 logger.warn("No files to save");			
		 }
		 else {
			 logger.info(String.format("Save %d files to TFS", localFiles.size()));		

			 // Save output files to the output directory specified in the 
			 for (String f: localFiles) {
				 String tfsName = tfs.putLocalFile(f, tfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp); 
				 if (tfsName == null) {
					 throw new StageException("Could not copy output file to TFS: " + f);
				 }
				 tfsFiles.add(tfsName);
			 }
		 }

		 return tfsFiles;
	 }
	
	/**
	 * Function called to this stages meta files.
	 * 
	 * @param localFiles list of files on local FS to save
	 * @return none
	 * @throws StageException if one or more files could not be saved 
	 */
	public void saveMetaFiles(ArrayList<String> metaFiles, long timestamp) throws StageException {
		if (metaFiles.size() > 0) {
			if (tfs.putLocalDirFiles(tfsMetaDir, timestamp, metaFiles, META_COMPRESSION, stageLogDir, stageTmpDir) == false) {
				throw new StageException("Could not save meta files");
			}
		}
	}
	
	/**
	 * Function called to save the log files produced by this stage.
	 * 
	 * @param localFiles list of files on local FS to save
	 * @return number of files saved
	 * @throws StageException if one or more files could not be saved 
	 */
	protected int saveLogFiles(ArrayList<String> logFiles, long timestamp) throws StageException {
		if (logFiles.size() > 0) {
			// The logfiles are saved in the tmp directory
			//if (tfs.putDirFiles(tfsLogDir, timestamp, logFiles, LOG_COMPRESSION, stageLogDir, stageTmpDir) == false) {
			//	throw new StageException("Could not save log files");
			//}
			int nSaved = logTable.putLogFiles(stageName, timestamp, logFiles);
			if (nSaved != logFiles.size()) {
				logger.warn("Could not save all log files");				
			}
			return nSaved;
		}
		return 0;
	}

	/**
	 * This function is called when the processing is done. It deleted the content of directories created on the local
	 * file system for this stage.
	 * 
	 * @throws StageException if all tmp files could not be deleted 
	 */
	public void cleanupLocalDirs() throws StageException {
		String[] dirs = {stageInputDir, stageLogDir, stageOutputDir, stageMetaDir, stageTmpDir};
		
		// Delete and then re-create directory
		for (String d: dirs) {
			if (! OsPath.isdir(d)) {
				logger.warn("Stage directory: " + d + " does not exist");				
			}
			else {
				if (OsPath.deleteAll(d) == false) {
					logger.warn("Could not delete directory: " + d);
					throw new StageException("Cleanup failed: could not delete directory: " + d);
				}
			}
			OsPath.mkdir(d);
		}	
	}
	
	/**
	 * This function is called when the processing is done. It deleted the content of the tmp directory created on TFS.
	 * 
	 * @throws StageException if all tmp files could not be deleted 
	 */
	protected void cleanupTFSDirs() throws StageException {
		try {
			if (tfs.isdir(tfsTmpDir)) {			
				if (tfs.deleteDir(tfsTmpDir) == false) {
					logger.warn("Could not delete TFS directory: " + tfsTmpDir);
					throw new StageException("Cleanup failed: could not delete TFS directory: " + tfsTmpDir);
				}
			}
		} catch (IOException e) {
			logger.warn("Could not delete TFS directory: " + e.toString());
			throw new StageException("Cleanup failed: I/O Exception while deleting tFS directory: " + tfsTmpDir);
		}
	}

	/**
	 * @return true if this is a crawler. Default is false. 
	 */
	public boolean isSource(){
		return false;
	}

	/**
	 * Return a list of all files in this stages output directory on local FS
	 *
	 * @return list of filenames in the output directory
	 * @throws StageException if directory cannot be listed
	 */
	protected ArrayList<String> getOutputFiles() throws StageException { 
		String[] files = OsPath.listdirR(stageOutputDir, logger);
		if (files == null) {
			logger.fatal("Could not list content of output directory: " + stageOutputDir);
			throw new StageException("Could not list output directory");
		}
		
		return Utils.array2list(files);
	}
	
	/**
	 * Update a list of all files in this stages meta file directory
	 *
	 * @param metaFiles list of meta files
	 * @return none
	 * @throws StageException if directory cannot be listed
	 */
	protected void updateMetaFiles( ArrayList<String> metaFiles) throws StageException { 
		String[] files = OsPath.listdirR(stageMetaDir, logger);
		if (files == null) {
			logger.fatal("Could not list content of meta directory: " + stageMetaDir);
			throw new StageException("Could not list meta directory");
		}
		
		for (String f: files) {
			if (metaFiles.contains(f) == false) {
				metaFiles.add(f);
			}
		}
	}
	
	
	/**
	 * Update the log files list by adding all files in the stageLogDir to the list
	 *
	 * @param logFiles list where logfile names are saved
	 * @return none
	 * @throws StageException  if directory cannot be listed
	 */
	protected void updateLogFiles(ArrayList<String> logFiles) throws StageException {
		String[] files = OsPath.listdirR(stageLogDir, logger);		
		if (files == null) {
			logger.fatal("Could not list content of log directory: " + stageLogDir);
			throw new StageException("Could not list log directory");
		}
		
		for (String f: files) {
			if (logFiles.contains(f) == false) {
				logFiles.add(f);
			}
		}
	}	
	
	/**
	 * Replace TROILKATT. substrings with per process variables.
	 * 
	 * Note that TROILKAT.FILE* symbods are not replaced
	 *
	 * @param argsStr string that contains TROILKATT substrings to be replaced    
	 * @return args string with TROILKATT substrings replaced 
	 * @throws TroilkattPropertiesException 
	 */
	protected String setTroilkattSymbols(String argsStr) throws TroilkattPropertiesException {		
		
		/*
		 * Note! There is a corresponding method in TroilkattMapReduce
		 */
		
		// Stage specific
		String newStr = argsStr.replace("TROILKATT.INPUT_DIR", 
					OsPath.normPath(stageInputDir, logger));
		newStr = newStr.replace("TROILKATT.OUTPUT_DIR", 
					OsPath.normPath(stageOutputDir, logger));
		newStr = newStr.replace("TROILKATT.LOG_DIR",				                 
					OsPath.normPath(stageLogDir, logger));
		newStr = newStr.replace("TROILKATT.META_DIR", 
				OsPath.normPath(stageMetaDir, logger));
		newStr = newStr.replace("TROILKATT.TMP_DIR", 
				OsPath.normPath(stageTmpDir, logger));
		
		// Set global and command line symbols
		newStr = Stage.setCommonTroilkattSymbols(newStr, troilkattProperties, logger);

		return newStr;
	}	        
	
	/**
	 * Helper function to set TROILKATT symbols. It is called by the setTroilkattSymbols method
	 * in respectively Stage and TroilkattMapReduce. This method should not be called explicitly, 
	 * rather subclasses should use the before mentioned methods.
	 * 
	 * @param argsStr string that contains TROILKATT substrings to be replaced    
	 * @param prop Troilkatt properties
	 * @param log log4j logger
	 * @return args string with TROILKATT substrings replaced 
	 * @throws TroilkattPropertiesException 
	 */
	public static String setCommonTroilkattSymbols(String argsStr, TroilkattProperties prop, Logger log) throws TroilkattPropertiesException {
		// Global
		String newStr = argsStr.replace("TROILKATT.DIR", 
				OsPath.normPath(prop.get("troilkatt.localfs.dir"), log));
		newStr = newStr.replace("TROILKATT.BIN", 
				OsPath.normPath(prop.get("troilkatt.localfs.binary.dir"), log));
		newStr = newStr.replace("TROILKATT.UTILS", 
				OsPath.normPath(prop.get("troilkatt.localfs.utils.dir"), log));
		newStr = newStr.replace("TROILKATT.GLOBALMETA_DIR", 
				OsPath.normPath(prop.get("troilkatt.globalfs.global-meta.dir"), log));
		newStr = newStr.replace("TROILKATT.SCRIPTS", 
				OsPath.normPath(prop.get("troilkatt.localfs.scripts.dir"), log));
		newStr = newStr.replace("TROILKATT.JAR", prop.get("troilkatt.jar"));
		newStr = newStr.replace("TROILKATT.CLASSPATH", prop.get("troilkatt.classpath"));
		newStr = newStr.replace("TROILKATT.MONGODB_SERVER_HOST", prop.get("troilkatt.troilkatt.mongodb.server.host"));
		newStr = newStr.replace("TROILKATT.MONGODB_SERVER_PORT", prop.get("troilkatt.troilkatt.mongodb.server.port"));

		// Command line argument helpers
		newStr = newStr.replace("TROILKATT.REDIRECT_OUTPUT", ">");
		newStr = newStr.replace("TROILKATT.REDIRECT_ERROR", "2>");
		newStr = newStr.replace("TROILKATT.REDIRECT_INPUT", "<");
		newStr = newStr.replace("TROILKATT.SEPERATE_COMMAND", ";");
		
		return newStr;
	}

	/**
	 * Replace TROILKATT.FILE and TROILKATT.FILE_NOEXT substrings respectively with the 
	 * filename or the filename without any extensions. Also replace TROILKATT.TIMESTAMP
	 *
	 * @param argStr string that contains the substring to replace
	 * @param inputFile input filename (absolute)
	 * @param timestamp
	 * @return string where file and iteration specific TROILKATT symbols are replaced
	 */ 
	protected String setTroilkattFilenameSymbols(String argStr, String inputFile) {
		String basename = OsPath.basename(inputFile);
		String noext = basename.split("\\.")[0];

		String newStr = argStr.replace("TROILKATT.FILE_NOEXT", noext);
		newStr = newStr.replace("TROILKATT.FILE", basename);				

		return newStr;
	}
	
	/**
	 * Replace TROILKATT.TIMESTAMP with the iteration specific timestamp
	 *
	 * @param argStr string that contains the substring to replace
	 * @param timestamp
	 * @return string where file and iteration specific TROILKATT symbols are replaced
	 */ 
	protected String setTroilkattTimestampSymbols(String argStr, long timestamp) {
		return argStr.replace("TROILKATT.TIMESTAMP", String.valueOf((timestamp)));
	}

	/**
	 * REPLACE TROILKATT.LOCAL with given string
	 *
	 * @param argStr: string that contain the TROILKATT.LOCAL to replace
	 * @param localStr: new string
	 *
	 * @return: string where TROILKATT.LOCAL is replaced with 'localStr'.
	 */
	//public String setTroilkattLocal(String  argStr, String localStr) {              
	//	return argStr.replace("TROILKATT.LOCAL", localStr);
	//}
	
	/**
	 * Execute a command (*nix only)
	 * 
	 * @param cmd command to execute 
	 * @return child program exit value, or -1 if child program could not be executed.
	 */	
	public static int executeCmd(String cmd, Logger logger) {		
		/* 
		 * It is necessary to execute the command using a shell in order to (easily) redirect
		 * stdout and stderr to a user specified file.
		 */
		String cmdV[] = {"/bin/bash", "-ic", cmd};
		//System.out.printf("Execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]);
		logger.info(String.format("Execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]));
		//System.out.println("Execute: " + cmdV[2]);
		Process child;
		try {
			child = Runtime.getRuntime().exec(cmdV);
			int exitValue = 0;
					
			/* Wait until the cmd completes */
			logger.debug("Waiting for command to complete");			
			child.waitFor();
			exitValue = child.exitValue();
							
			if (exitValue != 0) {
				logger.warn("Exit value for the executed command was: " + exitValue);					
			}
			
			return exitValue;
		} catch (IOException e) {
			logger.warn(String.format("Failed to execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]));
			logger.warn(e.toString());
			// User scripts may fail, but the processing should continue
			//throw new RuntimeException("Failed to execute command");			
		} catch (InterruptedException e) {
			logger.warn("Wait for child to complete was interrupted");
			logger.warn(e.toString());
			logger.warn(e.getStackTrace().toString());
		}
		
		return -1;
	}
	
	/**
	 * Check if a TROILKATT symbols string is valid
	 */
	public boolean isValidTroilkattSymbol(String symbol) {
		for (String v: VALID_TROILKATT_SYMBOLS) {
			if (v.equals(symbol)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Return a unique identifier for this stage of the form pipelineName-stageNum-stageName
	 */
	public String getStageID() {
		return pipelineName + "-" + stageName;
	}
	
	/**
	 * Split a String of arguments into a list of Strings seperated by whitespace. It also takes into
	 * account dashs, such that words within single-dashses are not split. 
	 * 
	 * For example "foo 'bar baz' bongo", returns ["foo", "bar baz", "bongo"]
	 *  
	 * @param arg arguments string to be split
	 * @return list of arguments
	 * @throws StageInitException 
	 */
	public String[] splitArgs(String arg) throws StageInitException {
		ArrayList<String> argList = new ArrayList<String>();
		
		// Start of word or substring
		int startIndex = -1;
		
		// If inSubstring is true we are currently reading a substring where whitespace should
		// be ignored
		boolean inSubstring = false;
		
		// Start in whitespce such that the start index of the first word will be the first
		// non-whitespace character in the arguments string
		boolean inWhitespace = true;
		
		// Find whitespace seperated words and substrings
		for (int i = 0; i < arg.length(); i++) {
			char c = arg.charAt(i);
			if (inWhitespace) { // Find next non-whitespace character
				if (Character.isWhitespace(c)) {
					continue;
				}
				
				// Non-whitespace character found
				startIndex = i;
				inWhitespace = false;
				if (arg.charAt(startIndex) == '\'') {
					inSubstring = true;
				}
			}
			else if (inSubstring) { // Find next single-dash
				if (c == '\'') {
					argList.add( arg.substring(startIndex + 1, i) ); // Remove ''
					// search for the start of the next word or substring
					inWhitespace = true;
					inSubstring = false;
				}
			}
			else {
				if (Character.isWhitespace(c)) { // end of word is found
					argList.add( arg.substring(startIndex, i) );
					// search for the next word
					inWhitespace = true;
				}
			}
		}
		// Add last word or substring
		if (inSubstring) { // invalid argument
			logger.fatal("Could not find closing ' in : " + arg);
			throw new StageInitException("Invalid argument: closing ' not found");
		}
		else if (! inWhitespace) {
			argList.add( arg.substring(startIndex) );
		}
		
		return argList.toArray(new String[argList.size()]);
	}
}

