package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.util.HashMap;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFile2;
import edu.princeton.function.troilkatt.tables.Table;
import gnu.getopt.Getopt;

public class SinkViewer extends Troilkatt {
	private String sinkName = null;
	private String outputDir = null;	
	private long timestamp; 
	private String progName = "SinkViewer";
	
	public SinkViewer() {
		super();
	}	
	
	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */	
	public void usage(String progName) {
		System.out.println(String.format("%s <download or print> sink [output-directory] [options]\n\n" + 
				"Required:\n" +
				"\tdownload or print: download if files should be downloaded, print if they " +
				"\t should only be printed to standard output" +
				"\tsink: name sink to download.\n"+
				"\toutput-directory: local directory where downloaded files are stored.\n\n" +
				"Options:\n" +
				"\t-f FILELIST   Specify a FILELIST to download or print (default: everything).\n" +
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
			usage("java");
			System.exit(2);
		}

		// Set defaults
		if (argv[0].equals("download")) {
			argDict.put("operation", "download");
			
			if (argv.length < 3) {
				System.err.println("Output directory must be specified for download");
				usage("java");
				System.exit(2);
			}
			
			argDict.put("outputDir",  argv[2]);
		}
		else if (argv[0].equals("print")) {
			argDict.put("operation", "print");
		}
		else {
			System.err.println("Invalid operation (must be download or print): " + argv[0]);
			usage("java");
			System.exit(2);
		}
		
		argDict.put("sink",  argv[1]);		
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging",    DEFAULT_ARGS.get("logging"));
		argDict.put("timestamp",  String.valueOf(-1));
		argDict.put("filelist",  "everything");

		Getopt g = new Getopt("troilkatt", argv, "hc:d:t:v:f:");
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
			case 'f':
				argDict.put("filelist", g.getOptarg());
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
	
	public void downloadLists(String filelist) {
		outputDir = OsPath.join(outputDir, sinkName);
		OsPath.mkdir(outputDir);
		if (outputDir == null) {
			System.err.println("Output directory not specified");
			System.exit(-1);
		}
		
		System.out.println("Info: the sink file lists are stored in outputDir: " + outputDir);
		
		if (filelist.equals("everyhtig") || filelist.equals("all")) {
			try {			
				Table.writeTextFile(OsPath.join(outputDir, "all"), 
						TroilkattFile2.troilkattfilearray2str(sinkTable.getAllFiles(sinkName, timestamp)));
			} catch (IOException e2) {
				logger.fatal("Could not download and write to local filesystem: all");
				logger.fatal(e2.toString());
				throw new RuntimeException(e2);
			}
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("new")) {
			try {
				Table.writeTextFile(OsPath.join(outputDir, "new"), 
						TroilkattFile2.troilkattfilearray2str(sinkTable.getNewFiles(sinkName, timestamp)));
			} catch (IOException e2) {
				logger.fatal("Could not download and write to local filesystem: new");
				logger.fatal(e2.toString());
				throw new RuntimeException(e2);
			}
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("updated")) {			
			try {
				Table.writeTextFile(OsPath.join(outputDir, "updated"), 
						TroilkattFile2.troilkattfilearray2str(sinkTable.getUpdatedFiles(sinkName, timestamp)));
			} catch (IOException e2) {
				logger.fatal("Could not download and write to local filesystem: updated");
				logger.fatal(e2.toString());
				throw new RuntimeException(e2);
			}
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("deleted")) {
			try {
				Table.writeTextFile(OsPath.join(outputDir, "deleted"), 
						TroilkattFile2.troilkattfilearray2str(sinkTable.getDeletedFiles(sinkName, timestamp)));
			} catch (IOException e2) {
				logger.fatal("Could not download and write to local filesystem: deleted");
				logger.fatal(e2.toString());
				throw new RuntimeException(e2);
			}
		}	
		
		if (filelist.equals("everyhtig")) {
			try {
				Table.writeTextFile(OsPath.join(outputDir, "fingerprints"), 
						Table.fpDict2str(sinkTable.getFingerprints(sinkName, timestamp)));
			} catch (IOException e2) {
				logger.fatal("Could not download and write to local filesystem: fingerprints");
				logger.fatal(e2.toString());
				throw new RuntimeException(e2);
			}
		}
	}
	
	public void printLists(String filelist) {
		logger.debug("Print files in list: " + filelist);
		
		if (filelist.equals("everyhtig") || filelist.equals("all")) {
			System.out.println(TroilkattFile2.troilkattfilearray2str(sinkTable.getAllFiles(sinkName, timestamp)));
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("new")) {
			System.out.println(TroilkattFile2.troilkattfilearray2str(sinkTable.getNewFiles(sinkName, timestamp)));			
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("updated")) {			
			System.out.println(TroilkattFile2.troilkattfilearray2str(sinkTable.getUpdatedFiles(sinkName, timestamp)));			
		}
		
		if (filelist.equals("everyhtig") || filelist.equals("deleted")) {
			System.out.println(TroilkattFile2.troilkattfilearray2str(sinkTable.getDeletedFiles(sinkName, timestamp)));
		}	
		
		if (filelist.equals("everyhtig")) {
			System.out.println(Table.fpDict2str(sinkTable.getFingerprints(sinkName, timestamp)));			
		}
	}
	
	/**
	 * Download logfiles for a sink.
	 *
	 * @param argv: sys.argv
	 */
	public void run(String[] argv) {
		/*
		 * Parse arguments and configuration file
		 */
		HashMap<String, String>args = parseArgs(argv);
		sinkName = args.get("sink");		
		outputDir = args.get("outputDir");
		timestamp = Long.valueOf(args.get("timestamp"));		

		/* 
		 * Setup Hbase and logging
		 */    
		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		setupLogging(args.get("logging"), troilkattProperties.get("troilkatt.logdir"), "sinkViewer.log");
		logger = Logger.getLogger("troilkatt.sinkViewer");
		setupHbase(troilkattProperties, false, true);

		/*
		 * Download and save file-lists
		 */    			   
		if (timestamp == -1) {
			logger.debug("The timestamp is: "  + timestamp);
		}
		else {
			logger.debug("No timestamp downloading latest version");
		}

		String filelist = args.get("filelist");
		if (args.get("operation").equals("download")) {
			downloadLists(filelist);
		}
		else {
			printLists(filelist);
		}
				
		System.err.println("Done");
	}

	/**
	 * @param args: see decription in usage()
	 */
	public static void main(String[] args) {
		SinkViewer sv = new SinkViewer();
		sv.run(args);
	}

}
