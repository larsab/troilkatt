package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.utils.Utils;
import gnu.getopt.Getopt;

/**
 * Delete a file in troilkatt data directory
 *
 */
public class DeleteFile extends TroilkattClient {
	protected static final String clientName = "DeleteFile";
	
	/**
	 * Constructor
	 */
	public DeleteFile() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir hdfs-file\n\n" + 
				"Required:\n" +
				"\thdfs-dir: directory that contains file to delete\n" +
				"\thdfs-file: file to delete (may exclude timestamp and compression)\n\n"+				
				"Options:\n" +				
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
		
		if (argv.length - g.getOptind() < 2) {
			usage(progName);
			System.exit(2);
		}

		argDict.put("hdfsDir",  argv[g.getOptind()]);
		argDict.put("file",  argv[g.getOptind() + 1]);

		return argDict;
	}
	
	/**
	 * Get a list of files that match the provided filename pattern
	 * 
	 * @param hdfsDir directory with files
	 * @param filename filename that may include timestamp and compression
	 * @return list of filenames that match the provided pattern
	 */
	protected ArrayList<String> getFiles(String hdfsDir, String filename) {
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
		
		// Find matching files
		ArrayList<String> matchedFiles = new ArrayList<String>();
		
		ArrayList<String> allFiles = null;
		try {
			allFiles = tfs.listdirR(hdfsDir);
		} catch (IOException e) {
			logger.error("Could not list directory: " + hdfsDir, e);
			return null;
		}
		for (String f: allFiles) {
			if (includesCompression && includesTimestamp) {
				// Full match required
				if (OsPath.basename(f).equals(filename)) { 
					matchedFiles.add(f);
				}
			}
			else if (includesTimestamp) { 
				// Special case: timestamps must match
				long tf = tfs.getFilenameTimestamp(f);
				String tn = tfs.getFilenameName(f);
				if ((tf == timestamp) && (name.equals(tn))) {
					matchedFiles.add(f);
				}
				
				// But it is also possible that the integer extension was not a timestamp
				// so we also attempt to match it as a filename
				if (tn.equals(basename)) {
					matchedFiles.add(f);
				}
			}
			else {
				// neither timestamp nor compression included
				String tn = tfs.getFilenameName(f);
				if (name.equals(tn)) {
					matchedFiles.add(f);
				}
			}
		}
		
		return matchedFiles;
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
				
		ArrayList<String> files = getFiles(hdfsDir, args.get("filename"));
		if (files == null) {
			System.out.println("Failed to read directory");
			System.exit(0);
		}
		else if (files.size() == 0) {
			System.out.println("No files found");
			System.exit(0);
		}
		else if (files.size() == 1) {		
			String f =  files.get(0);
			if (Utils.getYesOrNo("Delete file: " +f, false) == false) {
				System.exit(0);
			}
			try {
				tfs.deleteFile(f);
			} catch (IOException e) {
				logger.fatal("Could not delete file: " + f, e);
				System.out.println("Could not delete file: " + f);	
			}
		}
		else {
			System.out.println("Found multiple files: ");
			for (String f: files) {
				System.out.println("\t" + f);
			}
			if (Utils.getYesOrNo("Delete ALL files", false)) {
				for (String f: files) {
					try {					
						tfs.deleteFile(f);
					} catch (IOException e) {
						logger.fatal("Could not delete file: " + f, e);
						System.out.println("Could not delete file: " + f);
					}
				}
			}
			else { // do not delete all
				for (String f: files) {
					try {
						if (Utils.getYesOrNo("Delete file: " + f , false)) {
							tfs.deleteFile(f);
						}
					} catch (IOException e) {
						logger.fatal("Could not delete file: " + f, e);
						System.out.println("Could not delete file: " + f);
					}
				}
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
			System.exit(2);
		}
	}
}
