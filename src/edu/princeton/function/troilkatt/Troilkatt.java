package edu.princeton.function.troilkatt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.LogTableTar;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.fs.TroilkattGS;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.utils.Utils;
import gnu.getopt.Getopt;

/**
 * Class with main loop for troilkatt
 */
public class Troilkatt {
	public Map<String, String> DEFAULT_ARGS = new HashMap<String, String>();	
	
	/* Status file object */
	protected TroilkattStatus status; // initialized in run()
	
	/* Root logger */
	public Logger logger;		

	/**
	 * Constructor
	 */
	public Troilkatt() {
		/*
		 * Default command line arguments.
		 */
		DEFAULT_ARGS.put("configFile", "conf/troilkatt.xml");
		DEFAULT_ARGS.put("datasetFile", "conf/pipelines");
		DEFAULT_ARGS.put("logProperties", "log4j.properties");
		DEFAULT_ARGS.put("skip", "none");
		DEFAULT_ARGS.put("only", "all");
	}

	/**
	 * Print usage description
	 */
	protected void usage(String progName) {
		System.out.println(progName + " [options]\n");
		System.out.println("Options:");
		System.out.printf("\t-c FILE    Specify configuration FILE to use (default: %s).\n", DEFAULT_ARGS.get("configFile"));
		System.out.printf("\t-d FILE    Specify dataset FILE to use (default: %s).\n", DEFAULT_ARGS.get("datasetFile"));
		System.out.printf("\t-l FILE    log4j.properties FILE (default: %s).\n", DEFAULT_ARGS.get("logging"));
		System.out.printf("\t-s MODE    Set MODE to recovery to skip recovery, or MODE to cleanup to skip cleanup (default: none)\n.");
		System.out.printf("\t-o MODE    Set MODE to recovery to only run recovery, or MODE to cleanup to only run cleanup (default: all)\n.");
		System.out.printf("\t-h         Display command line options.\n");
		System.out.println("The configuration file allows setting more options.");
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments. Note, that most options are specified in the troilkatt.xml configuration
	 * file.
	 *
	 * @param args command line arguments (sys.argv[1:]).
	 * @return arguments in a hashmap. For non-specified arguments the default values are
	 * returned.
	 */
	protected HashMap<String, String> parseArgs(String args[]) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		Getopt g = new Getopt("troilkatt", args, "hc:d:l:s:o:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'd':
				argDict.put("datasetFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logProperties", g.getOptarg());
				break;
			case 's':				
				argDict.put("skip", g.getOptarg().toLowerCase());
				break;
			case 'o':				
				argDict.put("only", g.getOptarg().toLowerCase());
				break;
			case 'h':
				usage("Troilkatt");
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if ((! argDict.get("skip").equals("none")) && 
				(! argDict.get("skip").equals("recovery")) && 
						(! argDict.get("skip").equals("cleanup"))) {
			System.err.println("Invalid argument for -s: must be either 'none', 'recovery' or 'cleanup'");
			System.exit(2);
		}
		if ((! argDict.get("only").equals("all")) &&
				(! argDict.get("only").equals("recovery")) &&
						(! argDict.get("only").equals("cleanup"))) {
			System.err.println("Invalid argument for -o: must be either 'all', 'recovery' or 'cleanup'");
			System.exit(2);
		}
		if (argDict.get("skip").equals(argDict.get("only"))) {
			System.err.println("Invalid argument: both -s and -o set to: " + argDict.get("skip"));
			System.exit(2);
		}
		
		return argDict;
	}

	/**
	 * Get properties from configuration file
	 * 
	 * @param filename configuration file
	 * @return initialized TroilkattProperties object
	 */
	public static TroilkattProperties getProperties(String filename) {
		TroilkattProperties troilkattProperties = null;
	
		try {
			troilkattProperties = new TroilkattProperties(filename);
		} catch (TroilkattPropertiesException e) {
			e.printStackTrace();
			System.err.println(e.getMessage());
			throw new RuntimeException("Failed to create troilkatt properties object");
		}
	
		return troilkattProperties;
	}

	/**
	 * Setup the logging module.
	 *
	 * @param levelName minimum logging level. All messages with importance higher than this are 
	 *  logged. Supported levels are finest, info, warning, and severe. If specified finer and 
	 *  fine levels be mapped to finest. 
	 *  
	 *  Note! the public variable logger is not initialized since this function may be called from other 
	 *  classes.
	 *  
	 *    
	 * @param logProperties the log properties file
	 * @throws IOException if logfile cannot be opened.
	 */
	public static void setupLogging(String logProperties) {
		PropertyConfigurator.configure(logProperties);
		
		//System.err.println("Logging initialized: log is stored in: " + logname);
		System.err.printf("Logging initialized using the %s log4j properties file\n", logProperties);
	}

	/**
	 * Setup TFS based on persistent.storage property.
	 * 
	 * Note! in case of IOException the program quits.
	 * 
	 * @param troilkattProperties initialized properties structure
	 * @return TFS handle
	 * 
	 * @throws TroilkattPropertiesException
	 */
	public TroilkattFS setupTFS(TroilkattProperties troilkattProperties) throws TroilkattPropertiesException {
		TroilkattFS tfs = null;
		try {			
			String persistentStorage = troilkattProperties.get("troilkatt.persistent.storage");
			if (persistentStorage.equals("hadoop")) {
				tfs = new TroilkattHDFS(new Configuration());
			}
			else if (persistentStorage.equals("nfs")) {
				tfs = new TroilkattNFS();
			}
			else if(persistentStorage.equals("gestore")) {
				tfs = new TroilkattGS(new Configuration());
                        }
			else {
				logger.fatal("Invalid valid for persistent storage, tried storage " + persistentStorage);
				throw new TroilkattPropertiesException("Invalid value for persistent storage property");
			}			
			
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.println("Could not get handle for TFS" + e1.toString());			
			System.exit(-1);
		}	
	
		return tfs;
	}

	/**
	 * Check the status of the last execution.
	 *
	 * @param stageID stage to get status for 
	 * @return 0 if last execution completed successfully. Otherwise the timestamp 
	 *  of the last execution is returned.
	 * @throws IOException 
	 */
	protected long getLastTroilkattStatus() throws IOException {
		String lastStatus = status.getLastStatus("Troilkatt"); 
		if (lastStatus != null) {
			if (! lastStatus.equals("done")) {
				return status.getLastStatusTimestamp("Troilkatt");
			}
			else {
				return 0;
			}
		}
		else {
			System.err.println("Could not read last status from status file.");			
			if (Utils.getYesOrNo("Continue without doing recovery", true)) {
				return 0;
			}
			else {
				System.exit(0);
				return -1; // to remove warning
			}			  
		}
	}
	
	/**
	 * Make sure Troilkatt directories exists on master node.
	 * 
	 * Note that logging may not have been setup so all error messages are written to stderr
	 *
	 * @param troilkattProperties initialized properties object
	 * @param tfs initialized TFS handle
	 * @return true if all directories exists and false otherwise
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	public boolean verifyTroilkattDirs(TroilkattProperties troilkattProperties, TroilkattFS tfs) throws IOException, TroilkattPropertiesException {
		String[] localDirs = {"troilkatt.localfs.dir",
				"troilkatt.localfs.log.dir",			
				"troilkatt.globalfs.global-meta.dir",
				"troilkatt.localfs.mapreduce.dir",
				"troilkatt.localfs.binary.dir",
				"troilkatt.localfs.utils.dir",
				"troilkatt.localfs.scripts.dir"};
		
		String[] tfsDirs = {"troilkatt.tfs.root.dir"};
		
		for (String p: localDirs) {
			String d = troilkattProperties.get(p);
			if (! OsPath.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);
				return false;
			}
		}
		
		for (String p: tfsDirs) {
			String d = troilkattProperties.get(p);
			if (! tfs.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);				
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Create directories used by Troilkatt on master node.
	 * 
	 * Note that logging is not yet setup so all error messages are written to stderr
	 * 
	 * @param troilkattProperties initialized properties object
	 * @param tfs initialized TFS handle
	 * @throws TroilkattPropertiesException 
	 * @throws IOException 
	 */
	public void createTroilkattDirs(TroilkattProperties troilkattProperties, TroilkattFS tfs) throws TroilkattPropertiesException, IOException {
		
		String[] localDirs = {"troilkatt.localfs.dir",
				"troilkatt.localfs.log.dir",			
				"troilkatt.globalfs.global-meta.dir",
				"troilkatt.localfs.mapreduce.dir",
				"troilkatt.localfs.sge.dir"};
		
		String[] tfsDirs = {"troilkatt.tfs.root.dir"};
		
		for (String p: localDirs) {
			String d = troilkattProperties.get(p);
			if (! OsPath.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);				
				OsPath.mkdir(d);
				System.err.println("Warning: directory created: " + d);
				if (! OsPath.isdir(d)) {
					throw new IOException("Could not create directory: " + d);
				}
			}
		}
		
		for (String p: tfsDirs) {
			String d = troilkattProperties.get(p);
			if (! tfs.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);				
				tfs.mkdir(d);
				System.err.println("Warning: tfs directory created: " + d);
				if (! tfs.isdir(d)) {
					throw new IOException("Could not create directory: " + d);
				}
			}
		}	
	}
	
	/**
	 * Copy  global meta files from tfs to local FS, and ensure that all 
	 * files are readable by other (such as MapReduce) users.
	 * 
	 * @param tfs initialized TFS handle
	 * @param tfsGlobalMetaDir directory on tfs to download
	 * @param localfsGlobalMetaDir directory on local FS where files are downloaded to
	 * @param logDir logfile directory
	 * @param tmpDir temp file directory
	 * @throws IOException 
	 */
	protected void downloadGlobalMetaFiles(TroilkattFS tfs,
			String tfsGlobalMetaDir, String localfsGlobalMetaDir,
			String logDir, String tmpDir) throws IOException {		
		String newestGlobalMetaDir = tfs.getNewestDir(tfsGlobalMetaDir);
		if (newestGlobalMetaDir == null) {
			logger.warn("No global meta data!");				
		}
		else {
			ArrayList<String> globalMetaFiles = tfs.getDirFiles(OsPath.join(tfsGlobalMetaDir, newestGlobalMetaDir), localfsGlobalMetaDir, logDir, tmpDir);		
			if (globalMetaFiles == null) {
				logger.fatal("Could not download global meta files");
				throw new IOException("Could not download global meta files");
			}

			/*
			 * Set file permission such that all files are readable by all users.
			 * This is necessary to ensure that the MapReduce tasks can access the
			 * global metadir
			 * 
			 * This must be done by a Python script since Java does not support setting 
			 * the necessary file status bits. 
			 */
			String cmd = "chmod -R o+r " + OsPath.join(localfsGlobalMetaDir, "*");
			int rv = Stage.executeCmd(cmd, logger);
			if (rv == -1) {
				logger.fatal("Could not execute the change file permissions script");
				throw new IOException("Could not execute the change file permissions script");
			}
		}

	}
	
	/**
	 * Recursive version of listAllLeafDirs
	 * 
	 * @param tfs initialized TFS handle
	 * @param curDir current directory
	 * @param leafDirs list where leaf directories are added
	 * @return none
	 */
	private void addLeafDirsR(TroilkattFS tfs,
			String curDir, ArrayList<String> leafDirs) {
		ArrayList<String> files;
		try {
			files = tfs.listdir(curDir);
		} catch (IOException e) {
			logger.error("Could not list directory: " + curDir, e);
			return;
		}
		
		if (files == null) {
			logger.error("Could not list directory (null): " + curDir);
			return;
		}
		
		boolean isLeaf = true;
		for (String f: files) {		
			try {
				if (tfs.isdir(f)) {
					isLeaf = false; // This directory is not a leaf
					// Check subdir
					addLeafDirsR(tfs, f, leafDirs);
				}
			} catch (IOException e) {
				logger.error("Could not check file " + f , e);
			}
		}
		
		if (isLeaf) {
			leafDirs.add(curDir);
		}
	}
	
	
	/**
	 * Helper function to list all leaf sub directories
	 * 
	 * @param tfs initialized TFS handle
	 * @param tfsRootDir root directory
	 * @return list of leaf sub directories
	 */
	protected ArrayList<String> listAllLeafDirs(TroilkattFS tfs, String tfsRootDir) {		
		ArrayList<String> leafDirs = new ArrayList<String>();
		addLeafDirsR(tfs, tfsRootDir, leafDirs);
		return leafDirs;
	}

	/**
	 * Create all pipelines specified in dataset file
	 *
	 * @param datasetFile: file specifying dataset names to be crawled
	 * @param troilkattProperties: troilkatt properties object
	 * @param tfs tfs handle
	 * @param logger: callers logger instance
	 *  
	 * @return list of pipelines
	 * @throws PipelineException if a pipeline configuration file could not be parsed
	 * @throws TroilkattPropertiesException 
	 */
	public ArrayList<Pipeline> openPipelines(String datasetFile, 
			TroilkattProperties troilkattProperties,  
			TroilkattFS tfs,
			Logger logger) throws PipelineException, TroilkattPropertiesException {
	
		ArrayList<Pipeline> datasets = new ArrayList<Pipeline>();
		BufferedReader inputStream;
	
		try {
			inputStream = new BufferedReader(new FileReader(datasetFile));
	
			while (true) {
				/*
				 * Parse entry
				 */
				String pipelineFile = inputStream.readLine();
				if (pipelineFile == null) {
					break;
				}
	
				if (pipelineFile.startsWith("#")) {
					continue;
				}
				
				pipelineFile = pipelineFile.trim();
				if (pipelineFile.indexOf(".xml") == -1) {
					inputStream.close();
					logger.fatal("All pipelines should be named using their .xml file: " + pipelineFile);
					throw new PipelineException("Pipeline name error: " + pipelineFile);
				}
	
				/*
				 * Find pipeline name
				 */
				String pipelineName;
				try {
					pipelineName = OsPath.basename(pipelineFile).split("\\.")[0];
				} catch (ArrayIndexOutOfBoundsException e) {
					inputStream.close();
					logger.fatal("Could not parse dataset name: " + pipelineFile, e);
					throw new PipelineException("Pipeline name error: " + pipelineFile);
				}
				logger.info("Create pipeline: " + pipelineName);
					
				/*
				 * Create log table (per pipeline)
				 */
				LogTable lt = null;
				String persistentStorage = troilkattProperties.get("troilkatt.persistent.storage");
				if (persistentStorage.equals("hadoop")) {
					lt = new LogTableHbase(pipelineName, HBaseConfiguration.create());
				}
				else if (persistentStorage.equals("nfs")) {
					String sgeDir = troilkattProperties.get("troilkatt.globalfs.sge.dir");
					
					String localTmpDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), pipelineName);
					if (! OsPath.isdir(localTmpDir)) {
						if (! OsPath.mkdir(localTmpDir)) {
							logger.fatal("Could not create directory: " + localTmpDir);
							inputStream.close();
							throw new PipelineException("mkdir " + localTmpDir + " failed");
						}
					}

					String localLogDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.log.dir"), "logtar");
					if (! OsPath.mkdir(localLogDir)) {
						logger.fatal("Could not create directory: " + localLogDir);
						inputStream.close();
						throw new PipelineException("mkdir " + localLogDir + " failed");
					}
					String globalLogDir = OsPath.join(sgeDir, "logtar");
					if (! OsPath.mkdir(globalLogDir)) {
						logger.fatal("Could not create directory: " + globalLogDir);
						inputStream.close();
						throw new PipelineException("mkdir " + globalLogDir + " failed");
					}
					lt = new LogTableTar(pipelineName, tfs, globalLogDir, localLogDir, localTmpDir);
				}
				else if (persistentStorage.equals("gestore")) {
					lt = new LogTableHbase(pipelineName, HBaseConfiguration.create());
					//logger.warn("No logging table created");
				}
				else {
					logger.fatal("Invalid valid for persistent storage");
					inputStream.close();
					throw new PipelineException("Invalid valid for persistent storage");
				}
				
				Pipeline p;
				try {
					p = new Pipeline(pipelineName,
							pipelineFile,
							troilkattProperties,
							tfs, lt);
					datasets.add(p);
				} catch (StageInitException e) {
					inputStream.close();
					logger.fatal("Could not create a stage in pipeline: " + e);
					throw new PipelineException("Could not create a stage in pipeline");
				}
					
			}			
			inputStream.close();
		} catch (IOException e) {
			logger.fatal("Could not parse dataset file: " + datasetFile, e);			
			throw new PipelineException("Could not parse dataset file: " + datasetFile);
		}
	
		return datasets;
	}
	
	/**
	 * Troilkatt main loop
	 * 
	 * @param argv command line arguments
	 * @throws IOException for exceptions during status file update
	 * @throws TroilkattPropertiesException 
	 */
	public void run(String argv []) throws IOException, TroilkattPropertiesException {
		/*
		 * Parse command line arguments
		 */
		HashMap<String, String> args = parseArgs(argv);
		

		/*
		 * Do troilkatt initialization
		 */
		//String logLevel = args.get("logging"); 
		//if (logLevel.equals("debug") || logLevel.equals("info") || logLevel.equals("trace")) {
		//System.out.println("Parse configuration file: " + args.get("configFile"));
		//}

		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		
		/*
		 * Setup filesystem and directories
		 */
		TroilkattFS tfs = setupTFS(troilkattProperties);
		
		createTroilkattDirs(troilkattProperties, tfs);
		if (verifyTroilkattDirs(troilkattProperties, tfs) == false) {
			System.err.println("One or more invalid directories.");
			return;
		}
		
		/*
		 * Setup logging
		 */		
		//String troilkattLogdir = troilkattProperties.get("troilkatt.localfs.log.dir");
		setupLogging(args.get("logProperties"));		
		logger = Logger.getLogger("troilkatt");
		logger.fatal("\n\n\nNEW SESSION\n\n\n");
		//String timeStr = getTimeStr();		
		//logger.fatal("Started at: " + timeStr);
		
		logger.info("skip: " + args.get("skip"));
		logger.info("only: " + args.get("only"));
		
		/*
		 * Initialize all datasets to be updated
		 */			
		ArrayList<Pipeline> pipelines = null;
		try {
			pipelines = openPipelines(args.get("datasetFile"), 
					troilkattProperties,       									
					tfs,
					logger);
		} catch (PipelineException e) {
			logger.fatal("Could not create pipelines: " + e);			
			throw new RuntimeException("Pipeline parse configuration error: " +  e);			
		}		

						
		/*
		 * Setup temporary directories for the main thread
		 */
		String tfsRootDir = troilkattProperties.get("troilkatt.tfs.root.dir");
		String tfsGlobalMetaDir = OsPath.join(tfsRootDir, "global-meta");
		String localfsGlobalMetaDir = troilkattProperties.get("troilkatt.globalfs.global-meta.dir");
		OsPath.mkdir(localfsGlobalMetaDir);
		String rootLogDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.log.dir"), "troilkatt");
		OsPath.mkdir(rootLogDir);
		String localfsRootDir = troilkattProperties.get("troilkatt.localfs.dir");
		String rootTmpDir = OsPath.join(localfsRootDir, "tmp/troilkatt");
		OsPath.mkdir(rootTmpDir);
				
		/*
		 * Note! Global meta-data is not downloaded before recovery is run, since a crashed
		 * execution did not save the global-meta data. Downloading may therefore overwrite
		 * changes added by steps that may not be run during recovery. 
		 */
		
		/*
		 * Check if last iteration completed and do recovery if not.
		 * The first check is for the skip-recovery command line argument.
		 */
		status = new TroilkattStatus(tfs, troilkattProperties);
		long lastTimestamp = getLastTroilkattStatus();
		if (args.get("skip").equals("recovery") || args.get("only").equals("cleanup")) {
			if (lastTimestamp != 0) {
				logger.warn("Skipping recovery");
			}
			else {
				logger.info("No need for recovery");
			}
		}
		else if (lastTimestamp != 0) {
			logger.info("Doing recovery");
			/*
			 * Set state to recover
			 */
			logger.info("Last execution did not complete: perform recovery");
			status.setStatus("Troilkatt", lastTimestamp, "recover");                

			/*
			 * Do recovery
			 */			
			for (Pipeline p: pipelines) {
				logger.debug("Recover: pipeline "  + p);
				if (p.recover(lastTimestamp, status) == false) { // Recovery failed
					logger.error("Recovery failed for pipeline: " + p.name);
					return;
				}
			}
			
			/*
			 * Save updates to global meta
			 */
			System.out.println("listDir: " + localfsGlobalMetaDir);
			String[] globalFSFiles = OsPath.listdirR(localfsGlobalMetaDir);			
			if (globalFSFiles == null) {
				throw new IOException("Could not list files global meta directoy");
			}
			System.out.println("got " + globalFSFiles.length + " files");
			if (tfs.putLocalDirFiles(tfsGlobalMetaDir, lastTimestamp, Utils.array2list(globalFSFiles), "tar.gz", rootLogDir, rootTmpDir) == false) {
				throw new IOException("Could not save global meta files");
			}

			/*
			 * Update status
			 */
			logger.debug("Update status");
			status.setStatus("Troilkatt", lastTimestamp, "done");
			status.saveStatusFile();
		}
		else {
			logger.info("No need for recovery");
		}

		/*
		 * Main loop: update all pipelines and do cleanup
		 */
		//sleepTime = int(troilkattProperties.get('troilkatt.crawl.interval')) * 60 * 60
		boolean firstTime = true;
		while (true) {                    
			/*
			 * Get timestamp for this iteration
			 */
			long timestamp = TroilkattStatus.getTimestamp();			
			
			/*
			 * Download global-meta from stable storage
			 */		
			if (firstTime && (! args.get("only").equals("cleanup"))) {
				// Only need to download global meta data after first loop			
				downloadGlobalMetaFiles(tfs,
						tfsGlobalMetaDir,
						localfsGlobalMetaDir,
						rootLogDir,
						rootTmpDir);
				firstTime = false;
			}
			
			/*
			 * Update
			 */
			if (args.get("only").equals("all")) { // otherwise either only recovery or cleanup should be run
			
				/*
				 * Log state
				 */				    				    	    		       	
				logger.info("Start new iteration at: " + status.timeLong2Str(timestamp) + " (timestamp = " + timestamp + ")");
				status.setStatus("Troilkatt", timestamp, "start");
				
				/*
				 * Make sure all files in global meta are readable by all user (that is 
				 * MapReduce jobs). Also make similar tests for the scirpts and binary
				 * directories.
				 */
	
				/*
				 * Run pipelines
				 */
				logger.info("Updating " + pipelines.size() + " pipelines");
				for (Pipeline p: pipelines) {
					if (p.update(timestamp, status) == false) {
						logger.error("Update failed for pipeline: " + p.name);
						return;
					}
				}
				
				/*
				 * Save updates to global meta
				 */
				String[] globalFSFiles = OsPath.listdirR(localfsGlobalMetaDir);
				if (globalFSFiles == null) {
					throw new IOException("Could not list files global meta directoy");
				}
				if (tfs.putLocalDirFiles(tfsGlobalMetaDir, timestamp, Utils.array2list(globalFSFiles), "tar.gz", rootLogDir, rootTmpDir) == false) {
					throw new IOException("Could not save global meta files");
				}
				status.saveStatusFile();
				
				/*
				 * Log state
				 */  			
				status.setStatus("Troilkatt", timestamp, "done");
				status.saveStatusFile();
			}
			else {
				logger.info("Skipping pipeline update");
			}

			/*
			 * Do cleanup
			 */
			//DEUBG: alway skip cleanp
			logger.warn("CLEANUP DISABLED FOR DEBUGGING");
			args.put("skip", "cleanup");
			
			if (args.get("skip").equals("cleanup") || args.get("only").equals("recovery")) {
				logger.info("Skipping cleanup");
			}
			else {
				ArrayList<String> allDirs = listAllLeafDirs(tfs, OsPath.join(tfsRootDir, "data"));
				allDirs.addAll(listAllLeafDirs(tfs, OsPath.join(tfsRootDir, "meta")));
				for (Pipeline p: pipelines) {
					logger.info("Clean: pipeline "  + p.name);
					ArrayList<String> cleanedDirs = p.cleanup(timestamp);
					// Keep track of which dirs have been cleaned
					for (String cd: cleanedDirs) {
						allDirs.remove(cd);
					}
				}
				// Remaining dirs in the list have not been cleaned
				for (String nc: allDirs) {
					logger.warn("Directory not cleaned (not in any pipeline): " + nc);
				}
				// Cleanup log and tmp dirs on frontend and all workers
				logger.info("Clean: " + rootLogDir);
				if (OsPath.deleteAll(rootLogDir) == false) {
					logger.warn("Could not delete tmp directory: " + rootLogDir);
				}
				OsPath.mkdir(rootLogDir);
				logger.info("Clean: " + rootTmpDir);
				if (OsPath.deleteAll(rootTmpDir) == false) {
					logger.warn("Could not delete tmp directory: " + rootTmpDir);
				}
				OsPath.mkdir(rootTmpDir);
			}
			
			if (! args.get("only").equals("all")) { // Only one iteration should be ru
				break;
			}
			// else continue
			
			// DEBUG: ALWAYS RUN JUST A SINGLE ITERATION
			break;
			//time.sleep(sleepTime)
		}

		logger.fatal("Troilkatt was shut down");
	}

	/**
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		Troilkatt t = new Troilkatt();
		try {			
			t.run(args); 	    
			System.out.println("Troilkatt shutdown");
		} catch (IOException e) {
			System.err.println("Troilkatt execution failed");
			e.printStackTrace();
		} catch (TroilkattPropertiesException e) {
			System.err.println("Troilkatt properties error: " + e);
			e.printStackTrace();
		}	
	}
}









