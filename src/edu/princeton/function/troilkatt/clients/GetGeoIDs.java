package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import gnu.getopt.Getopt;

public class GetGeoIDs extends TroilkattClient {
	protected static final String clientName = "GetGeoIDs";

	/**
	 * Constructor
	 */
	public GetGeoIDs() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir\n\n" + 
				"Required:\n" +
				"\thdfs-dir: directory with IDs to list\n" +				
				"Options:\n" +				
				"\t-t TIMESTAMP  Specify a timestamp for which to retrieve files for (default: newest).\n" +
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-i            Ignore platform.\n" + 
				"\t-h            Display command line options.", 
				progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logProperties")));
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
		argDict.put("logging",    DEFAULT_ARGS.get("logProperties"));		
		argDict.put("timestamp",  "newest");
		argDict.put("ignorePlatform",  "false");

		Getopt g = new Getopt("troilkatt", argv, "hic:l:t:");
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
			case 'i':
				argDict.put("ignorePlatform",  "true");
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
	 * List Geo IDs for the files in an HDFS directory
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
		
		String hdfsDir = getDirectory(args.get("hdfsDir"));	
		
		ArrayList<String> files = null;
		try {			
			if (args.get("timestamp").equals("newest")) {
				files = tfs.listdirN(hdfsDir);
			}
			else {
				long timestamp = Long.valueOf(args.get("timestamp"));
				files = tfs.listdirT(hdfsDir, timestamp);			
			}
		} catch (IOException e) {
			logger.fatal("Could not list directory: " + hdfsDir, e);
			System.out.println("Could not list directory: " + hdfsDir);
		}
				
		if (files != null) {
			Collections.sort(files);
			
			if (args.get("ignorePlatform").equals("false")) {
				for (String f: files) {	
					System.out.println(FilenameUtils.getDsetID(f, true));
				}
			}
			else {
				for (String f: files) {	
					System.out.println(FilenameUtils.getDsetID(f, false));
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GetGeoIDs df = new GetGeoIDs();
		try {
			df.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}
	}

}
