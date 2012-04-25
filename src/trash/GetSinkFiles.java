package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFile2;
import gnu.getopt.Getopt;

public class GetSinkFiles extends Troilkatt {
	private String progName = "GetSinkFiles";
	
	public GetSinkFiles() {
		super();
	}

	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */	
	public void usage(String progName) {
		System.out.println(String.format("%s sink output-directory [options]\n\n" + 
				"Required:\n" +
				"\tsink: name sink to download.\n"+
				"\toutput-directory: local directory where downloaded files are stored.\n\n" +
				"Options:\n" +
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
	@Override
	public HashMap<String, String> parseArgs(String[] argv) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		if (argv.length < 2) {
			usage(progName);
			System.exit(2);
		}

		// Set defaults
		argDict.put("sink",  argv[0]);
		argDict.put("outputDir",  argv[1]);
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));
		argDict.put("timestamp",  String.valueOf(-1));

		Getopt g = new Getopt("troilkatt", argv, "hc:d:t:v:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {			
			case 'd':				
				argDict.put("timestamp", String.valueOf(timeStr2Int(g.getOptarg())));				
				break;
			case 't':
				argDict.put("timestamp", g.getOptarg());
				break;
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'v':
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
	 * Download files in a sink to a directory on the local filesystem
	 *
	 * @param argv: sys.argv
	 */
	public void run(String[] argv) {
		/*
		 * Parse arguments and configuration file
		 */
		HashMap<String, String>args = parseArgs(argv);
		String sinkName = args.get("sink");
		String outputDir = OsPath.join(args.get("outputDir"), sinkName);
		long timestamp = Long.valueOf(args.get("timestamp"));

		if (outputDir == null) {
			System.err.println("Output directory not specified");
			System.exit(-1);
		}

		/* 
		 * Setup Hbase and logging
		 */    
		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		setupLogging(args.get("logging"), troilkattProperties.get("troilkatt.logdir"), "getSinkFiles.log");
		logger = Logger.getLogger("troilkatt.logViewer");
		setupHbase(troilkattProperties, false, true);

		/*
		 * Download and save files
		 */    
		System.out.println("Info: the sink files are stored in outputDir: " + outputDir);	    
		if (timestamp == -1) {
			logger.debug("The timestamp is: "  + timestamp);
		}
		else {
			logger.debug("No timestamp downloading latest version");
		}

		// Get list of all files in directory
		ArrayList<TroilkattFile2> sinkFiles = sinkTable.getAllFiles(sinkName, timestamp);
		if (sinkFiles.size() == 0) {
			System.out.println("WARNING: No files found for sink: " + sinkName);
			logger.warn("No files found for sink: " + sinkName);
		}

		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(this.hdfsConfig);
		} catch (IOException e1) {
			logger.fatal("Could not create HDFS FileSystem object");
			logger.fatal(e1.toString());
			throw new RuntimeException(e1);
		}

		for (TroilkattFile2 tf: sinkFiles) {
			Path srcPath = new Path(tf.getHDFSFilename());
			Path dstPath = new Path(OsPath.join(outputDir, tf.getFilename()));
			try {
				hdfs.copyToLocalFile(false, srcPath, dstPath);
			} catch (IOException e) {
				logger.fatal("Could not copy file " + srcPath.toString() + " from HFDS to local filesystem: " + dstPath.toString());
				throw new RuntimeException("File copy failed");
			}
		}

		try {
			hdfs.close();
		} catch (IOException e) {
			logger.fatal("Could not close HDFS FileSystem object");
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param args: see decription in usage()
	 */
	public static void main(String[] args) {
		GetSinkFiles sv = new GetSinkFiles();
		sv.run(args);
	}
}
