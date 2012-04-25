package edu.princeton.function.troilkatt.clients;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import gnu.getopt.Getopt;

public class GetLogFiles extends TroilkattClient {
	
protected static final String clientName = "GetLogFiles";
	
	/**
	 * Constructor
	 */
	public GetLogFiles() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] pipeline-name stage-name output-dir\n\n" + 
				"Required:\n" +
				"\tpipeline-name: pipeline name (same as configuration file without .xml).\n" +
				"\tstage-name:    stage name including stage number.\n" +
				"\toutput-dir:    directory where log files are copied to.\n\n" +
				"Options:\n" +				
				"\t-t TIMESTAMP  Specify a timestamp for which to retrieve log files for (default: newest).\n" +
				"\t-m            Set if stage is a MapReduce stage for which all task log files should also\n" +
				"                be retrieved (default: not set)\n" +
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-h            Display command line options.", progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logging")));
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv command line arguments including the program name (sys.argv[0])
	 * @return a map with arguments
	 */
	@Override
	protected HashMap<String, String> parseArgs(String[] argv, String progName) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		// Set defaults
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));		
		argDict.put("timestamp",  "newest");
		argDict.put("mapReduce", "false");

		Getopt g = new Getopt("troilkatt", argv, "hmc:l:t:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logging", g.getOptarg());
				break;
			case 't':
				String timestampStr = g.getOptarg();
				// Make sure it is a valid timestamp
				try {
					Long.valueOf(timestampStr);
				} catch (NumberFormatException e) {
					System.err.println("Invalid timestamp in arguments: " + timestampStr);
					System.exit(2);
				}
				argDict.put("timestamp", timestampStr);
				break;
			case 'm':
				argDict.put("mapReduce", "true");
				break;
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if ((argv.length - g.getOptind()) < 3) {
			usage(progName);
			System.exit(2);
		}

		argDict.put("pipelineName",  argv[g.getOptind()]);
		argDict.put("stageName",  argv[g.getOptind() + 1]);
		argDict.put("outputDir",  argv[g.getOptind() + 2]);

		return argDict;
	}

	/**
	 * Copy files from data directory to local filesystem.
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
				
		String outputDir = args.get("outputDir");
		if (! OsPath.isdir(outputDir)) {
			if (OsPath.mkdir(outputDir) == false) {
				System.out.println("Directory does not exist and could not be created: " + outputDir);
				System.exit(2);
			}
		}
		
		String pipelineName = args.get("pipelineName");
		String stageName = args.get("stageName");
		long timestamp = -1;
		if (! args.get("timestamp").equals("newest")) {
			timestamp = Long.valueOf(args.get("timestamp"));
		}		
		boolean isMR = args.get("mapReduce").equals("true");
		
		Configuration hbConf = HBaseConfiguration.create();
		LogTable lt = null;
		try {
			lt = new LogTable(pipelineName, hbConf);
		} catch (PipelineException e) {
			logger.error("Could not create log table handle", e);
			System.out.println("Could not create log table handle");
			System.exit(2);
		}
		
		try {
			if (isMR) {
				lt.getMapReduceLogFiles(stageName, timestamp, outputDir);			
			}
			else {
				lt.getLogFiles(stageName, timestamp, outputDir);
			}
		} catch (StageException e) {
			logger.error("Could not download all log files", e);
			System.out.println("Could not download all log files");
			System.exit(2);
		}
		
		System.out.println("Log files downloaded to " + outputDir);
	}
	
	/**
	 * @param args command line arguments. See usage() for details 
	 */
	public static void main(String[] args) {		
		GetLogFiles getter = new GetLogFiles();
		try {
			getter.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}		
	}

}
