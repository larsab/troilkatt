package edu.princeton.function.troilkatt.clients;

import java.util.HashMap;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.OsPath;
import gnu.getopt.Getopt;

/**
 * View logs for a pipeline. 
 */
public class LogViewer extends Troilkatt {
	private String progName = "LogViewer";
	public LogViewer() {
		super();
	}

	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */
	public void usage(String progName) {
		System.out.println(String.format("python %s dataset [-eoa] [options]\n\n" +
				"Required:\n" +
				"\tdataset: name of dataset/pipeline to download to logfiles for. The name should be the\n" +
				"\tdatasets .xml file (for example datasets/test.xml).\n\n" +    
				"Options:\n" +
				"\t-a DIRECTORY  Download all logfiles and store these in DIRECTORY\n" +
				"\t-e            Only download *.error logs and print these to standard output\n" +
				"\t-o            Only downlaod *.output logs and print these to standard output\n" +
				"\t-d DATE       Specify a DATE for the file versions to download. The date must be in\n" +
				"\t              the following format: YYYY-MM-DD-HH-mm. The version downloaded contains\n" +
				"\t              all logs added/updated before the specified date/time. If the date is not\n" +
				"\t              specified the newest version of all files are downloaded.\n" +
				"\t-t TIMESTAMP  Specify the file version to download as the integer TIMESTAMP used in Hbase.\n" +
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-v LEVEL      Specify logging level to use {debug, info, warning, error, or critical}\n" +
				"\t              (default: %s).\n" +
				"\t-h            Display command line options.", progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logging")));
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv: command line arguments including the program name (sys.argv[0])
	 * @return: a map with arguments
	 */
	public HashMap<String, String> parseArgs(String[] argv) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		if (argv.length < 3) {
			usage("java LogViewer");
			System.exit(2);
		}

		// Set defaults
		argDict.put("dataset",  argv[0]);
		argDict.put("outputDir",  null);
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));
		argDict.put("timestamp",  String.valueOf(-1));
		argDict.put("errorMode",  String.valueOf(false));
		argDict.put("outputMode", String.valueOf(false));

		Getopt g = new Getopt("troilkatt", argv, "hoec:d:t:a:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {
			case 'a':
				System.out.println(argDict.get("outputDir"));
				System.out.println(g.getOptarg());
				argDict.put("outputDir", g.getOptarg());
				System.out.println(argDict.get("outputDir"));
				break;			
			case 'e':
				argDict.put("errorMode", String.valueOf(true));
				break;
			case 'o':
				argDict.put("outputMode", String.valueOf(true));
				break;
			case 'd':				
				argDict.put("timestamp", String.valueOf(timeStr2Int(g.getOptarg())));				
				break;
			case 't':
				argDict.put("timestamp", g.getOptarg());
				break;
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logging", g.getOptarg());
				break;
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}

		return argDict;
	}


	/**
	 * Download logfiles for a dataset/pipeline.
	 *
	 * @param argv: sys.argv
	 */
	public void run(String[] argv) {

		/*
		 * Parse arguments and configuration file
		 */
		HashMap<String, String>args = parseArgs(argv);
		String datasetName = args.get("dataset");
		String outputDir = OsPath.join(args.get("outputDir"), datasetName);
		long timestamp = Long.valueOf(args.get("timestamp"));

		if (outputDir == null) {
			System.err.println("Output directory not specified");
			System.exit(-1);
		}

		/* 
		 * Setup Hbase and logging
		 */    
		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		setupLogging(args.get("logging"), troilkattProperties.get("troilkatt.logdir"), "logViewer.log");
		logger = Logger.getLogger("troilkatt.logViewer");
		setupHbase(troilkattProperties, true, false);

		/* 
		 * Setup dataset
		 */    
		Pipeline dataset = Pipeline.openDataset(datasetName, troilkattProperties, pipelineTable, sinkTable, hdfsConfig, hbConfig, logger);           

		/*
		 * Get/print files
		 */
		if (Boolean.valueOf(args.get("errorMode"))) {        
			dataset.printLogfiles(timestamp, ".*\\.error");
		}
		else if (Boolean.valueOf(args.get("outputMode"))) {
			dataset.printLogfiles(timestamp, ".*\\.output");
		}
		else {
			// Create output directory if needed
			System.out.println("Info: the logfiles are stored in outputDir:" + outputDir);
			logger.info("The sink files are stored in outputDir: " + outputDir);                
			if (! OsPath.isdir(outputDir)) {
				if (! OsPath.mkdir(outputDir)) {
					logger.fatal("Could not create output directory: " + outputDir);
					throw new RuntimeException("mkdir failed");
				}
			}

			dataset.downloadLogfiles(outputDir, timestamp);
		}
	}

	/**
	 * @param args: see decription in usage()
	 */
	public static void main(String[] args) {
		LogViewer lv = new LogViewer();
		lv.run(args);
	}
}
