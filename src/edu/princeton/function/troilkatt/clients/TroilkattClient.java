package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.HashMap;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import gnu.getopt.Getopt;

/**
 * Client superclass
 */
public class TroilkattClient extends Troilkatt {
	// Set in setupClient
	protected HashMap<String, String> args;
	protected TroilkattProperties troilkattProperties;
	
	// Directory for tmp files
	// The directory is created in the constructor and deleted in the destructor	
	protected String tmpDir = "/tmp/troilkatt-client"; 
	
	/**
	 * Constructor
	 */
	public TroilkattClient() {
		// Initializes tfs and DEFAULT_ARGS
		super();
				
		if (! OsPath.isdir(tmpDir)) {
			if (OsPath.mkdir(tmpDir) == false) {
				System.out.println("Tmp directory could not be created: " + tmpDir);
				System.exit(2);
			}
		}
	}
	
	/**
	 * Destructor
	 */
	public void finalize() {
		OsPath.deleteAll(tmpDir);
	}
	
	/**
	 * Print usage information.
	 */
	protected void usage(String progName) {
		System.out.println(String.format("%s [options]\n\n" + 	
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t              (default: %s).\n" +
				"\t-h            Display command line options.", progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logging")));
	}
	
	/**
	 * Setup client: parse arguments, property file, and setup logging
	 * 
	 * @param argv sys.argv
	 * @return none, but initialized global variables.
	 * @throws TroilkattPropertiesException 
	 * @throws TroilkattPropertiesException if properties file cannot be parsed.
	 */
	protected void setupClient(String argv[], String progName) throws TroilkattPropertiesException {
		args = parseArgs(argv, progName);	
		troilkattProperties = getProperties(args.get("configFile"));
		tfs = setupTFS(troilkattProperties);
		setupLogging(args.get("logProperties"));	
		logger = Logger.getLogger("troilkatt." + progName);	
		
	}
	
	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv command line arguments including the program name (sys.argv[0])
	 * @return a map with arguments
	 */
	protected HashMap<String, String> parseArgs(String[] argv, String progName) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));		

		Getopt g = new Getopt("troilkatt", argv, "hc:l:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
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
	 * Get and check a directory name
	 * 
	 * @param hdfsDir directory name that may be absolute or relative to data dir
	 * @return absolute directory name, or null if it is an invalid directory name.
	 */
	protected String getDirectory(String hdfsDir) {
		if (! hdfsDir.startsWith("/")) { // is relative to data dir
			String hdfsDataDir;
			try {
				hdfsDataDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "data");
			} catch (TroilkattPropertiesException e) {
				logger.error("Invalid troilkatt properties", e);
				return null;
			}
			hdfsDir = OsPath.join(hdfsDataDir, hdfsDir);
		}
		
		try {
			if (tfs.isdir(hdfsDir) == false) {
				logger.error("Not a valid directory: " + hdfsDir);
				return null;
			}
		} catch (IOException e1) {			
			logger.error("Could not check directory", e1);
			return null;
		}
		
		return hdfsDir;
	}
}
