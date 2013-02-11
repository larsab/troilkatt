package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.HashMap;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.utils.Utils;
import gnu.getopt.Getopt;

public class ExecuteMapReduceJob extends TroilkattClient {	
	protected static final String clientName = "ExecuteMapReduceJob";

	// File with input arguments to MapReduce program
	public String argsFilename;
	

	public ExecuteMapReduceJob() throws TroilkattPropertiesException {
		// Additional default arguments
		DEFAULT_ARGS.put("name", "mrclient-000-job");
		DEFAULT_ARGS.put("stageArgs", "");
		DEFAULT_ARGS.put("hdfsOutputDir", "mrclient/output");
		DEFAULT_ARGS.put("hdfsMetaDir", "mrclient/meta");
		DEFAULT_ARGS.put("compressionFormat", "gz");
		DEFAULT_ARGS.put("storageTime", "-1");
		DEFAULT_ARGS.put("localRootDir", OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), "mrclient"));

		// argsFilename is set after parseArgs()
		// jarFile is set after parseArgs
		// mapReduceCmd is set afte parseArgs 		
	}

	/**
	 * Print usage information.
	 */
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] [args]\n\n" +
				"Arguments: MAIN_CLASS INPUT_FILE_LIST\n" +
				"\tMAIN_CLASS      Main class of the hadoop job in the troilkatt.jar.\n" +
				"\tINPUT_FILE_LIST File with list of HDFS input files.\n" + 
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-n NAME       Pipeline name, stage number, and stagen name to use in the form" +
				"                pipeline-XXX-stage (default: %s)\n" + 
				"\t-a ARGS       Stage NAME to use (for example. 003-soft2pcl, default: %s).\n" +
				"\t-o DIR        HDFS output DIRectory (default: %s).\n" +
				"\t-z FORMAT     Compression FORMAT to use for output files (default: %s).\n" +
				"\t-s DIR        Storage time for output files (default: %s).\n" +
				"\t-t TIMESTAMP  Specify a timestamp for which to retrieve files for (default: newest).\n" +
				"\t-m DIR        HDFS meta DIRectory (default: %s).\n" +
				"\t-d DIR        DIRectory on local file system (default: %s).\n" + 				
				"\t-h            Display command line options.", 
				progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logging"),
				DEFAULT_ARGS.get("name"), DEFAULT_ARGS.get("stageArgs"), 
				DEFAULT_ARGS.get("hdfsOutputDir"), DEFAULT_ARGS.get("hdfsMetaDir"),
				DEFAULT_ARGS.get("localRootDir")));		
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv command line arguments including the program name (sys.argv[0])
	 * @return a map with arguments
	 * @throws IOException 
	 */
	protected HashMap<String, String> parseArgs(String[] argv, String progName) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));		


		String[] nameParts = DEFAULT_ARGS.get("name").split("-");
		argDict.put("pipelineName", nameParts[0]);
		argDict.put("stageNum", nameParts[1]);
		argDict.put("stageName", nameParts[2]);
		argDict.put("stageArgs", DEFAULT_ARGS.get("stageArgs"));
		argDict.put("hdfsOutputDir",DEFAULT_ARGS.get("hdfsOutputDir"));
		argDict.put("hdfsMetaDir",DEFAULT_ARGS.get("hdfsMetaDir"));
		argDict.put("localRootDir", DEFAULT_ARGS.get("localRootDir"));
		argDict.put("compressionFormat", DEFAULT_ARGS.get("compressionFormat"));
		argDict.put("storageTime", DEFAULT_ARGS.get("storageTime"));

		Getopt g = new Getopt("troilkatt", argv, "hc:l:n:a:o:d:z:s:m");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logging", g.getOptarg());
				break;
			case 'n':				
				nameParts = g.getOptarg().split("-");
				if (nameParts.length != 3) {
					System.err.println("Invalid name: the format should be: pipelineName-stageNum-stageName");
					System.exit(0);
				}
				argDict.put("pipelineName", nameParts[0]);
				try {
					Integer.valueOf(nameParts[1]);
				} catch (NumberFormatException e) {
					System.err.println("Invalid stage number (must be integer): " + nameParts[1]);
					System.exit(2);
				}	
				argDict.put("stageNum", nameParts[1]);
				argDict.put("stageName", nameParts[2]);
				break;
			case 'a':
				argDict.put("stageArgs", g.getOptarg());
				break;
			case 'o':
				argDict.put("hdfsOutputDir", g.getOptarg());
				break;
			case 'z':
				String cf = g.getOptarg();
				if (! TroilkattFS.isValidCompression(cf)) {
					System.err.println("Invalid output compression format: " + cf);
					System.exit(2);
				}
				argDict.put("compressionFormat", cf);				
				break;
			case 's':
				String st = g.getOptarg();
				try {
					Integer.valueOf(st);
				} catch (NumberFormatException e) {
					System.err.println("Invalid storage time (must be integer): " + st);
					System.exit(2);
				}				
				argDict.put("storageTime", st);				
				break;
			case 't':
				String timestampStr = g.getOptarg();
				// Make sure it is a valid timestamp
				try {
					Long.valueOf(timestampStr);
				} catch (NumberFormatException e) {
					System.err.println("Invalid timestamp in arguments (must be long): " + timestampStr);
					System.exit(2);
				}
				argDict.put("timestamp", timestampStr);
				break;
			case 'm':
				String dir = g.getOptarg();
				try {
					if (! tfs.isdir(dir)) {
						System.err.println("Warning; Not a valid HDFS directory");
					}
				} catch (IOException e) {
					System.err.println("HDFS I/O exception: " + e.getMessage());
					System.exit(2);
				}
				argDict.put("hdfsMetaDir", dir);				
				break;
			case 'd':
				argDict.put("localRootDir", g.getOptarg());				
				break;
			case 'h':
				usage(progName);
				System.exit(2);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}

		if (argv.length - g.getOptind() < 2) {
			usage(progName);
			System.exit(2);
		}

		argDict.put("mainClass",  argv[g.getOptind()]);
		String inputFilelist = argv[g.getOptind() + 1];
		argDict.put("inputFilelist", inputFilelist);	

		if (! OsPath.isfile(argDict.get("inputFilelist"))) {
			System.err.println("Input filelist is not a file: " + inputFilelist);
		}

		return argDict;
	}
	
	/**
	 * Run a Troilkatt MapReduce job
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		/*
		 * Setup
		 */
		// Setup client will call parseArgs which initialized the args hashmap
		setupClient(argv, clientName);
		
		// read input file list
		String[] inputFiles = null;
		try {
			inputFiles = FSUtils.readTextFile(args.get("inputFilelist"));
		} catch (IOException e) {
			logger.fatal("Could not read fomr input filelist file: ", e);
			System.exit(1);
		}		
		
		/*
		 * Create a dummy pipeline with a mapreduce stage used to execute the program
		 */						
		// dummy pipeline
		Pipeline pipeline = null;
		try {
			pipeline = new Pipeline(args.get("pipelineName"), troilkattProperties, tfs);
		} catch (PipelineException e) {
			logger.fatal("Could not create dummy pipeline: ", e);
			System.exit(1);
		}						
		
		String hdfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");		
		long timestamp = Long.valueOf(args.get("timestamp"));
		
		// MapRed stage
		MapReduce mrs = null;
		try {
			mrs = new MapReduce(Integer.valueOf(args.get("stageNum")), args.get("stageName"), 
					troilkattProperties.get("troilkatt.jar") + " " + args.get("mainClass") + args.get("stageArgs"),
					args.get("hdfsOutputDir"), args.get("compressionFormat"), Integer.valueOf(args.get("storageTime")), 
					args.get("localRootDir"), args.get("hdfsMetaDir"), hdfsTmpDir,
					pipeline);
		} catch (StageInitException e) {
			logger.fatal("Could not create dummy MapReduce stage: ", e);
			System.exit(1);
		}
		
		/*
		 * Execute job, save log files, and output files
		 */
		try {
			mrs.process2(Utils.array2list(inputFiles), timestamp);
		} catch (StageException e) {
			logger.fatal("Could not execute MapReduce job: ", e);
			System.exit(1);
		}
	}


	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {		
		try {
			ExecuteMapReduceJob df = new ExecuteMapReduceJob();
			df.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(1);
		}
	}

}
