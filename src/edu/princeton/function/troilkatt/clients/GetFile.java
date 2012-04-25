/**
 * 
 */
package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import gnu.getopt.Getopt;

/**
 * Copy a file from the troilkatt data directory to the local file system
 *
 */
public class GetFile extends TroilkattClient {
	protected static final String clientName = "GetFile";
	
	/**
	 * Constructor
	 */
	public GetFile() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir hdfs-file local-dir\n\n" + 
				"Required:\n" +
				"\thdfs-dir: directory that contains the file to get\n" +
				"\thdfs-file: file to get (may exclude timestamp and compression)\n"+
				"\tlocal-dir: directory where file is copied to\n\n" +
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +				
				"\t-h            Display command line options.", progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logProperties")));
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


		Getopt g = new Getopt("troilkatt", argv, "hc:l:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logProperties", g.getOptarg());
				break;
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if (argv.length - g.getOptind() < 3) {
			usage(progName);
			System.exit(2);
		}

		argDict.put("hdfsDir",  argv[g.getOptind() + 0]);
		argDict.put("file",  argv[g.getOptind() + 1]);
		argDict.put("outputDir",  argv[g.getOptind() + 2]);

		return argDict;
	}
	
	/**
	 * Get a list of files that match the provided filename pattern
	 * 
	 * @param hdfsDir directory with files
	 * @param filename filename that may include timestamp and compression
	 * @return filename that match the provided pattern, or null if no file could be found
	 */
	protected String getFile(String hdfsDir, String filename) {
		String basename = OsPath.basename(filename);
		
		// Attempt to guess if filename contains timestamp and compression
		boolean includesCompression = true;
		String compression = tfs.getFilenameCompression(filename);
		if (TroilkattFS.isValidCompression(compression) == false) {
			includesCompression = false;
		}
		
		boolean includesTimestamp = true;
		long timestamp = tfs.getFilenameTimestamp(filename);
		String name = tfs.getFilenameName(filename);
		if (timestamp == -1) { // not a valid timestamp in filename
			// May still contain a valid timestamp
			try {
				timestamp = Long.valueOf(OsPath.getLastExtension(filename));
				name = filename.substring(0, filename.lastIndexOf(".")); // strip timestamp
			} catch (NumberFormatException e) {
				// Expected
			}			
		}
		if (timestamp == -1) { // still not valid
			includesTimestamp = false;
		}	
		
		try {
			if (includesCompression && includesTimestamp) {
				// Find exact match
				ArrayList<String>  allFiles = tfs.listdirR(hdfsDir);
				for (String f: allFiles) {
					// Full match required
					if (OsPath.basename(f).equals(filename)) { 
						return f;
					}
				}
			}
			else if (includesTimestamp) {
				ArrayList<String>  allFiles = tfs.listdirR(hdfsDir);
				for (String f: allFiles) {
					// Special case: timestamps must match
					long tf = tfs.getFilenameTimestamp(f);
					String tn = tfs.getFilenameName(f);
					if ((tf == timestamp) && (name.equals(tn))) {
						return f;
					}

					// But it is also possible that the integer extension was not a timestamp
					// so we also attempt to match it as a filename
					if (tn.equals(basename)) {
						return f;
					}
				}
			}
			else {
				ArrayList<String>  newestFiles = tfs.listdirN(hdfsDir);
				for (String f: newestFiles) {
					// neither timestamp nor compression included
					String tn = tfs.getFilenameName(f);
					if (name.equals(tn)) {
						return f;
					}
				}
			}
		} catch (IOException e) {
			logger.error("Could not list directory: " + hdfsDir, e);
			return null;
		}
		
		// Not found
		return null;
	}
	
	/**
	 * Cleanup HDFS directory.
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
				
		String file = getFile(hdfsDir, args.get("filename"));
		if (file == null) {
			System.out.println("No files found");			
		}
		else {
			try {
				tfs.getFile(file, outputDir, tmpDir, tmpDir);
			} catch (IOException e1) {
				logger.fatal("Could not get file: " + file, e1);
				System.out.println("Could not get file: " + e1);	
			}
		}		
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		DeleteFile df = new DeleteFile();
		try {
			df.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
		}
	}
}
