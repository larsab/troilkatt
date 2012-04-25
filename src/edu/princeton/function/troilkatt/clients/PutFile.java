package edu.princeton.function.troilkatt.clients;

import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.TroilkattStatus;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import gnu.getopt.Getopt;

/**
 * Copy file to troilkatt data dir
 */
public class PutFile extends TroilkattClient {
	protected static final String clientName = "PutFile";
	
	/**
	 * Constructor
	 */
	public PutFile() {
		super();
		DEFAULT_ARGS.put("compressionFormat", "none");
		DEFAULT_ARGS.put("timestamp", "current");
	}

	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.printf("%s [options] local-file hdfs-directory\n\n" + 
				"Required:\n" +
				"\tlocal-file: File to put to HDFS\n" +
				"\thdfs-dir:   HDFS directory where the file is copied to.\n\n" +				
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-t TIMESTAMP  Timestamp to add to file (default: current time).\n" +
				"\t-z COMPRESSION Compression format to use (default: %s)\n" +
				"\t-h            Display command line options.", 
				progName, 
				DEFAULT_ARGS.get("configFile"), 
				DEFAULT_ARGS.get("logProperties"), 
				DEFAULT_ARGS.get("compressionFormat"));
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
		argDict.put("logProperties", DEFAULT_ARGS.get("logProperties"));
		argDict.put("timestamp", DEFAULT_ARGS.get("timestamp"));	
		argDict.put("compressionFormat", DEFAULT_ARGS.get("compressionFormat"));

		Getopt g = new Getopt("troilkatt", argv, "hc:l:t:z:");
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
			case 'z':
				String cf = g.getOptarg();
				if (! TroilkattFS.isValidCompression(cf)) {
					System.err.println("Not a valid compression format: " + cf);
					System.exit(2);
				}
				argDict.put("compressionFormat", cf);
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
		argDict.put("localFile", argv[g.getOptind()]);
		argDict.put("hdfsDir", argv[g.getOptind() + 1]);
		
		return argDict;
	}
	
	/**
	 * Copy a file from the local file system to the storage on HDFS.
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
		
		String hdfsDir = getDirectory(args.get("hdfsDir"));
		if (hdfsDir == null) {
			System.exit(2);
		}
		
		String localFile = args.get("localFile");
		if (! OsPath.isfile(localFile)) {
			System.err.println("Not a valid file: " + localFile);
			System.exit(2);
		}
	
		long timestamp = TroilkattStatus.getTimestamp(); // current
		if (args.get("timestamp").equals("current")) { 
			// user has defined timestap to use
			timestamp = Long.valueOf(args.get("timestamp"));
		}
		
		String hdfsFile = tfs.putLocalFile(localFile, hdfsDir, 
				tmpDir, tmpDir, args.get("compressionFormat"), timestamp);
		if (hdfsFile == null) {
			System.err.println("Could not copy file: " + localFile);
			System.exit(2);
		} 
		System.out.println("Copied to HDFS file: " + hdfsFile);		
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		PutFile dc = new PutFile();
		try {
			dc.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}
	}
}
