package edu.princeton.function.troilkatt.clients;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFile2;
import edu.princeton.function.troilkatt.tables.FileTable;
import gnu.getopt.Getopt;

/**
 * Add files to the data directory.
 */
public class UploadDataFiles extends Troilkatt {	
	private String progName = "UploadDataFiles";
	
	public UploadDataFiles() {
		super();
	}
	
	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */	
	@Override
	public void usage(String progName) {
		System.out.println(String.format("%s [options] files...\n\n" + 
				"Required:\n" +
				"\tfiles: list of files to upload to the data directory.\n\n"+				
				"Options:\n" +
				"\t-d DATE       Specify a DATE used to timestamp the files to upload. The date must be\n" +
				"\t              in the following format: YYYY-MM-DD-HH-mm. If the date is not specified\n" +
				"\t              the current time is used for the timestamp.\n" +
				"\t-t TIMESTAMP  Specify a TIMESTAMP for the files to upload.\n" +
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
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:");	    	
		String timestr = df.format(new Date());		
		argDict.put("timestamp",  String.valueOf(timeStr2Int(timestr)));

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
		
		int firstFile = g.getOptind();
		String files = null;
		for (int i = firstFile; i < argv.length; i++) {
			if (i == firstFile) {
				files = argv[i]; 
			}
			else {
				files = files + "\t" + argv[i];
			}
		}
		argDict.put("files", files);

		return argDict;
	}
	
	/**
	 * Upload files to the meta-data directory
	 *
	 * @param argv: sys.argv
	 */
	public void run(String[] argv) {
		/*
		 * Parse arguments and configuration file
		 */
		HashMap<String, String>args = parseArgs(argv);
		long timestamp = Long.valueOf(args.get("timestamp"));

		if (args.get("files") == null) {
			System.err.println("No files to upload");
			System.exit(0);
		}
		String[] files = args.get("files").split("\t");
		
		for (String f: files) {
			System.out.println("Upload file: " + f);
		}

		/* 
		 * Setup Hbase and logging
		 */    
		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		setupLogging(args.get("logging"), troilkattProperties.get("troilkatt.logdir"), "uploadDataFiles.log");
		logger = Logger.getLogger("troilkatt.uploadDataFiles");
		setupHbase(troilkattProperties, false, true);
		
		String tableName = troilkattProperties.get("troilkatt.meta-data.tablename");
		String tableType = "file_table";
		String sinkName = troilkattProperties.get("troilkatt.meta-data.sink");
		    			    
		if (timestamp == -1) {
			logger.debug("The timestamp is: "  + timestamp);
		}
		else {
			logger.debug("No timestamp downloading latest version");
		}

		/*
		 * Upload files
		 */					
		
		ArrayList<TroilkattFile2> newFiles = new ArrayList<TroilkattFile2>();
		ArrayList<TroilkattFile2> updatedFiles = new ArrayList<TroilkattFile2>();
		ArrayList<TroilkattFile2> allFiles = sinkTable.getAllFiles(sinkName, -1);
		
		FileTable fileTable = new FileTable(hdfsConfig, hbConfig, tableType, tableName, troilkattProperties);
		for (String f: files) {
			TroilkattFile2 tf = new TroilkattFile2(OsPath.basename(f), 
					OsPath.dirname(f),
					troilkattProperties.get("troilkatt.tfs.root.dir"),
					tableName,
					fileTable.getFiletype(f),
					timestamp,
					fileTable.getCompression(f));
			
			if (allFiles.contains(tf)) {
				updatedFiles.add(tf);
			}
			else {
				newFiles.add(tf);
			}
		}
		
		fileTable.saveFiles(newFiles, updatedFiles, timestamp);
		
		/*
		 * Update sink
		 */
		sinkTable.update("meta-data", 				
				newFiles,
				updatedFiles, 
				new ArrayList<TroilkattFile2>(), 				
				new HashMap<String, String>(),
				timestamp);
		sinkTable.commit();
	}

	/**
	 * @param args: see decription in usage()
	 */
	public static void main(String[] args) {
		UploadDataFiles sv = new UploadDataFiles();
		sv.run(args);
	}
}
