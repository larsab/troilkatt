package edu.princeton.function.troilkatt.clients;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import gnu.getopt.Getopt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

/**
 * Remove all old versions of files with multiple timestamps in a HDFS directory. 
 */
public class CleanupDir extends TroilkattClient {
	protected static final String clientName = "DirCleanup";
	
	/**
	 * Constructor
	 */
	public CleanupDir() {
		super();
	}

	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-directory\n\n" + 
				"Required:\n" +
				"\thdfs-dir: HDFS directory to clean.\n\n"+				
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
		argDict.put("logProperties",  DEFAULT_ARGS.get("logProperties"));		

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
		
		if (argv.length - g.getOptind() < 1) {
			usage(progName);
			System.exit(2);
		}
		argDict.put("hdfsDir",  argv[g.getOptind()]);			

		return argDict;
	}
		
	/**
	 * Cleanup HDFS directory.
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
		
		System.out.println("Cleanup directory: " + args.get("hdfsDir"));
		String hdfsDir = getDirectory(args.get("hdfsDir"));
		if (hdfsDir == null) {
			System.exit(0);
		}
	
		/*
		 * Get list of HDFS-directory files.
		 */		
		logger.info("Read contents of HDFS directory: " + hdfsDir);		
		ArrayList<String> files = null;
		try {
			files = tfs.listdirR(hdfsDir);
		} catch (IOException e) {
			logger.fatal("HDFS directory read failed: " + e.toString());
			System.err.println("Could not read content of directory: " + hdfsDir);
			System.exit(-1);
		}
		if (files == null) { 
			logger.fatal("HDFS directory read failed");
			System.err.println("Could not read content of directory: " + hdfsDir);
			System.exit(-1);
		}
				
		HashMap<String, Long> file2timestamp = new HashMap<String, Long>();
		HashMap<String, Path> file2path = new HashMap<String, Path>();
		ArrayList<Path> toDelete = new ArrayList<Path>();
		int nInvalid = 0;
		for (String f: files) {
			String basename = OsPath.basename(f);
			long timestamp = tfs.getFilenameTimestamp(f);
			String name = tfs.getFilenameName(f);
			if ((timestamp == -1) || (name == null)) {
				logger.warn("Invalid Troilkatt filename: " + basename);
				nInvalid++;
				continue;
			}												
			
			Path path = new Path(f);
			if (file2timestamp.containsKey(name)) { // multiple versions of same file exist
				long currentTs = file2timestamp.get(name);
				if (currentTs < timestamp) { // This file is newer
					// Delete previous file with same id
					toDelete.add(file2path.get(name));
					// ..and update data structures
					file2timestamp.put(name, timestamp);
					file2path.put(name, path);
				}
				else { // This file is older, and should be deleted
					toDelete.add(path);
				}				
			}
			else { // is first version of file so far
				file2timestamp.put(name, timestamp);
				file2path.put(name, path);
			}
		}
		
		int nDeleted = 0;
		int nErrors = 0;
		for (Path p: toDelete) {
			try {
				tfs.hdfs.delete(p, false);
				nDeleted++;
			} catch (IOException e1) {
				logger.warn("Could not delete file: " + p.toString());
				nErrors++;
			}
		}
		
		System.out.printf("Deleted %d of %d files\n", nDeleted, files.size());
		System.out.printf("Errors: %d invalid files, %d files could not be deleted\n", nInvalid, nErrors);
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		CleanupDir dc = new CleanupDir();
		try {
			dc.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}
	}
}
