package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile2;

/**
 * This class synchronizes the sink meta data with the content of a HDFS directory. It is 
 *  used to fix a bug that overwrote the "all" filelist with the new filelist, or to update
 *  the sink content after doing a sink cleanup.
 * 
 * @author larsab
 *
 */
public class FixSink extends GetSinkFiles {

	public FixSink() {
		super();
	}	
	
	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */	
	@Override
	public void usage(String progName) {
		System.out.println(String.format("%s sink HDFS-directory [options]\n\n" + 
				"Required:\n" +
				"\tsink: name sink to fix.\n"+
				"\tHDFS-directory: HDFS directory with files that should be in sink\n\n" +
				"Options:\n" +			
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
		/* All arguments are similar to superclass except that the specified directory is
		 *  in HDFS rather than on the local filesystem.
		 */
		HashMap<String, String> argDict = super.parseArgs(argv);
		String hdfsDir = argDict.remove("outputDir");
		argDict.put("hdfsDir", hdfsDir);
		return argDict;
	}
	
	/**
	 * Synchronize sink with hdfs-directory content.
	 *
	 * @param argv: sys.argv
	 */
	public void run(String[] argv) {
		/*
		 * Parse arguments and configuration file
		 */
		HashMap<String, String>args = parseArgs(argv);
		String sinkName = args.get("sink");
		String hdfsDir = args.get("hdfsDir");	
		
		/* 
		 * Setup Hbase and logging
		 */    
		TroilkattProperties troilkattProperties = getProperties(args.get("configFile"));
		setupLogging(args.get("logging"), troilkattProperties.get("troilkatt.logdir"), "sinkViewer.log");
		logger = Logger.getLogger("troilkatt.fixSink");
		setupHbase(troilkattProperties, false, true);

		/*
		 * Get list of HDFS-directory files.
		 */		
		logger.info("Read contents of HDFS directory: " + hdfsDir);
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(this.hdfsConfig);
		} catch (IOException e1) {
			logger.fatal("Could not create HDFS FileSystem object");
			logger.fatal(e1.toString());
			throw new RuntimeException(e1);
		}
		
		ArrayList<Path> paths = new ArrayList<Path>();
		getFilelistRecursive(hdfs, args.get("hdfsDir"), paths);
		
		ArrayList<TroilkattFile2> allFiles = new ArrayList<TroilkattFile2>();
		for (Path p: paths) {
			String filename = p.toString();
			if (filename.contains("RAW.tar")) {
				allFiles.add( new TroilkattFile2(filename,
						hdfsDir, null, logger));
			}
		}
		
		try {
			hdfs.close();
		} catch (IOException e1) {
			logger.fatal("Could not close HDFS FileSystem object");
			logger.fatal(e1.toString());
		}
		
		/*
		 * Set sinktable
		 */    
		logger.info("Set filelists for sink: " + sinkName);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:");	    	
		String timestr = df.format(new Date());	    	    		        
		long timestamp = timeStr2Int(timestr);
		
		ArrayList<TroilkattFile2> emptyList =  new ArrayList<TroilkattFile2>();
		// newFiles is set to allFiles, updatedFiles and deletedFiles are both empty
		// fingerprints is also empty
		sinkTable.setFiles(sinkName, 
				allFiles, allFiles, emptyList, emptyList, 
				new HashMap<String, String>(), 
				timestamp);				
		
		System.out.println("Done");
	}

	/**
	 * @param args: see decription in usage()
	 */
	public static void main(String[] args) {
		FixSink fx = new FixSink();
		fx.run(args);
	}

}
