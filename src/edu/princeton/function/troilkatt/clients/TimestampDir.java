package edu.princeton.function.troilkatt.clients;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.TroilkattStatus;
import gnu.getopt.Getopt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Convert a directory to Troilkatt format by adding timestamps and compression (none) to files
 *
 */
public class TimestampDir extends TroilkattClient {
	private String clientName = "TimestampDir";
	
	public TimestampDir() {
		super();
	}

	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-directory\n\n" + 
				"Required:\n" +
				"\thdfs-dir: HDFS directory with files to timestamp.\n\n"+				
				"Options:\n" +		
				"\t-t TIMESTMP   Specify timestamp to use (default: current time).\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-h            Display command line options.", 
				progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logProperties")));
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv: command line arguments including the program name (sys.argv[0])
	 * @return: a map with arguments
	 */
	@Override
	protected HashMap<String, String> parseArgs(String[] argv, String progName) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		// Set defaults		
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging", DEFAULT_ARGS.get("logProperties"));		
		argDict.put("compression", "none");

		Getopt g = new Getopt("troilkatt", argv, "hc:l:t:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logProperties", g.getOptarg());
				break;
			case 't':
				argDict.put("timestamp", g.getOptarg());
				break;			
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if (argv.length - g.getOptind() < 1) {
			usage(progName);
			System.exit(2);
		}
		argDict.put("hdfsDir",  argv[g.getOptind()]);

		return argDict;
	}		
	
	/**
	 * Timestamp HDFS directory.
	 *
	 * @param argv: sys.argv
	 * @throws TroilkattPropertiesException 
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
	
		System.out.println("Cleanup directory: " + args.get("hdfsDir"));
		String hdfsDir = getDirectory(args.get("hdfsDir"));
		if (hdfsDir == null) {
			System.exit(0);
		}
		
		long timestamp = 0;		
		if (args.containsKey("timestamp")) {
			timestamp = Long.valueOf(args.get("timestamp"));
		}
		else {
			timestamp = TroilkattStatus.getTimestamp();
		}
		
		/*
		 * Get list of HDFS-directory files.
		 */				
		logger.info("Read contents of HDFS directory: " + hdfsDir);
		ArrayList<String> currentFiles = null;
		try {
			currentFiles = tfs.listdirR(hdfsDir);
		} catch (IOException e) {
			logger.fatal("Could not list directory: " + e.toString());
			System.exit(-1);
		}
		if (currentFiles == null) {
			logger.fatal("Could not list directory: " + hdfsDir);
			System.exit(-1);
		}
		
		/*
		 * Add timestamp and compression format (none)
		 */		
		String[] newNames = new String[currentFiles.size()]; 
		for (int i = 0; i < currentFiles.size(); i++) {			
			newNames[i] = currentFiles.get(i) +  "." + timestamp + ".none";			
		}
		
		/*
		 * Rename files
		 */		
		int nRenamed = 0;
		int nErrors = 0;
		for (int i = 0; i < newNames.length; i++) { 
			String oldName = currentFiles.get(i);
			try {
				logger.info("Rename file: " + oldName + " to " + newNames[i]);
				if (tfs.renameFile(oldName, newNames[i]) == false) {
					logger.warn("Could not rename file: " + oldName);
				}
				nRenamed++;
			} catch (IOException e1) {
				logger.warn("Could rename file " + oldName + " to " + newNames[i]);
				logger.warn(e1.toString());
				nErrors++;
			}
		}
		
		System.out.printf("Converted %d of %d files to Troilkatt format\n", nRenamed, currentFiles.size());
		System.out.printf("%d files could not be renemed\n", nErrors);		
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		TimestampDir dt = new TimestampDir();
		try {
			dt.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Configuration file error");
		}
	}
}
