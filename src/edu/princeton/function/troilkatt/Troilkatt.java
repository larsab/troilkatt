/**
 * Main loop for troilkatt
 */
package edu.princeton.function.troilkatt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.utils.Utils;

import gnu.getopt.Getopt;

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
				tfs = new TroilkattHDFS();
			}
			else if (persistentStorage.equals("nfs")) {
				tfs = new TroilkattNFS();
			}
			else {
				logger.fatal("Invalid valid for persistent storage");
				throw new TroilkattPropertiesException("Invalid value for persistent storage property");
			}			
			
		} catch (IOException e1) {
			System.err.println("Could not get handle for TFS" + e1.toString());
			e1.printStackTrace();
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
		
		String[] hdfsDirs = {"troilkatt.hdfs.root.dir"};
		
		for (String p: localDirs) {
			String d = troilkattProperties.get(p);
			if (! OsPath.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);
				return false;
			}
		}
		
		for (String p: hdfsDirs) {
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
				"troilkatt.localfs.mapreduce.dir"};
		
		String[] hdfsDirs = {"troilkatt.hdfs.root.dir"};
		
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
		
		for (String p: hdfsDirs) {
			String d = troilkattProperties.get(p);
			if (! tfs.isdir(d)) {
				System.err.printf("Warning: directory %s does not exist on local file system: %s\n", p, d);				
				tfs.mkdir(d);
				System.err.println("Warning: HDFS directory created: " + d);
				if (! tfs.isdir(d)) {
					throw new IOException("Could not create directory: " + d);
				}
			}
		}	
	}
	
	/**
	 * Copy  global meta files from HDFS to local FS, and ensure that all 
	 * files are readable by other (such as MapReduce) users.
	 * 
	 * @param tfs initialized TFS handle
	 * @param hdfsGlobalMetaDir directory on HDFS to download
	 * @param localfsGlobalMetaDir directory on local FS where files are downloaded to
	 * @param logDir logfile directory
	 * @param tmpDir temp file directory
	 * @throws IOException 
	 */
	protected void downloadGlobalMetaFiles(TroilkattFS tfs,
			String hdfsGlobalMetaDir, String localfsGlobalMetaDir,
			String logDir, String tmpDir) throws IOException {		
		String newestGlobalMetaDir = tfs.getNewestDir(hdfsGlobalMetaDir);
		if (newestGlobalMetaDir == null) {
			logger.warn("No global meta data!");				
		}
		else {
			ArrayList<String> globalMetaFiles = tfs.getDirFiles(OsPath.join(hdfsGlobalMetaDir, newestGlobalMetaDir), localfsGlobalMetaDir, logDir, tmpDir);		
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
			logger.error("Could not list directory: " + curDir);
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
	 * @param hdfsRootDir root directory
	 * @return list of leaf sub directories
	 */
	protected ArrayList<String> listAllLeafDirs(TroilkattFS tfs, String hdfsRootDir) {		
		ArrayList<String> leafDirs = new ArrayList<String>();
		addLeafDirsR(tfs, hdfsRootDir, leafDirs);
		return leafDirs;
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
			pipelines = Pipeline.openPipelines(args.get("datasetFile"), 
					troilkattProperties,       									
					tfs,
					logger);
		} catch (PipelineException e) {
			logger.fatal("Could not create pipelines");			
			throw new RuntimeException("Pipeline parse configuration error: ",  e);			
		}		

						
		/*
		 * Setup temporary directories for the main thread
		 */
		String hdfsRootDir = troilkattProperties.get("troilkatt.hdfs.root.dir");
		String hdfsGlobalMetaDir = OsPath.join(hdfsRootDir, "global-meta");
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
			if (tfs.putLocalDirFiles(hdfsGlobalMetaDir, lastTimestamp, Utils.array2list(globalFSFiles), "tar.gz", rootLogDir, rootTmpDir) == false) {
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
						hdfsGlobalMetaDir,
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
				if (tfs.putLocalDirFiles(hdfsGlobalMetaDir, timestamp, Utils.array2list(globalFSFiles), "tar.gz", rootLogDir, rootTmpDir) == false) {
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
				ArrayList<String> allDirs = listAllLeafDirs(tfs, OsPath.join(hdfsRootDir, "data"));
				allDirs.addAll(listAllLeafDirs(tfs, OsPath.join(hdfsRootDir, "meta")));
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









