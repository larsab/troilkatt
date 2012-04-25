package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import gnu.getopt.Getopt;

/**
 * Get the newest version of all files in a directory
 */
public class GetFiles extends TroilkattClient {
	protected static final String clientName = "GetFiles";
	
	/**
	 * Constructor
	 */
	public GetFiles() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir local-dir\n\n" + 
				"Required:\n" +
				"\thdfs-dir: directory with file to get\n" +
				"\tlocal-dir: directory where file is copied to\n\n" +
				"Options:\n" +				
				"\t-t TIMESTAMP  Specify a timestamp for which to retrieve files for (default: newest).\n" +
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
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
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if (argv.length - g.getOptind() < 2) {
			usage(progName);
			System.exit(2);
		}

		argDict.put("hdfsDir",  argv[g.getOptind()]);
		argDict.put("outputDir",  argv[g.getOptind() + 1]);

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
		
		String hdfsDir = getDirectory(args.get("hdfsDir"));	
		String outputDir = args.get("outputDir");
		if (! OsPath.isdir(outputDir)) {
			if (OsPath.mkdir(outputDir) == false) {
				System.out.println("Directory does not exist and could not be created: " + outputDir);
				System.exit(2);
			}
		}
		
		String tmpDir = "/tmp/troilkatt-getfile";
		if (! OsPath.isdir(tmpDir)) {
			if (OsPath.mkdir(tmpDir) == false) {
				System.out.println("Tmp directory could not be created: " + tmpDir);
				System.exit(2);
			}
		}
		
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
			for (String f: files) {	
				try {
					tfs.getFile(f, outputDir, tmpDir, tmpDir);
				} catch (IOException e1) {
					logger.fatal("Could not get file: " + f, e1);
					System.out.println("Could not get file: " + e1);	
				}
			}
		}
		
		// Cleanup
		OsPath.deleteAll(tmpDir);
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		GetFiles df = new GetFiles();
		try {
			df.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}
	}

}
