package edu.princeton.function.troilkatt.source;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;
import edu.princeton.function.troilkatt.tools.ParseException;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.Duplicate;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.OverlapSet;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.Superset;

/**
 * A source for the GEO GDS pipeline
 */
public class GeoGDSPipeline extends Source {
	// Maximum line size
	private final int MAX_LINE_SIZE = 1048576; // 1MB in bytes		
	
	// Buffer for lines read from the input file
	protected Text line;
	
	// Used to parse GSMOverlap output and to find datasets and samples to remove
	private GeoGSMOverlap parser;
	
	// PCL file directory in HDFs (given as argument to stage)
	protected String pclInputDir;
	// GSMOverlap output directory in HDFs (given as argument to stage)
	protected String gsmOverlapOutputDir;
	// Minimum number of samples per dataset/series (given as argument to stage)
	protected int minSamples;
	// Maximum number of overlapping samples per dataset/ series (given as argument to stage)
	protected int maxOverlap;
	// File where the found overlap information is written. The file is in the global meta
	// directory, and the filename is given as an argument to the stage
	protected String outputFilename;
	
	/**
	 * Constructor
	 * 
	 * @param args 
	 *   0: Directory with input PCL files
	 *   1: GSMOverlap output directory
	 *   2: minimum number of samples per dataset/series
	 *   3: maximum number of overlapping samples per dataset/ series
	 *   4: output filename relative to the global meta directory 
	 *   
	 * See superclass for remaining arguments
	 */
	public GeoGDSPipeline(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		
		super(name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		line = new Text(new byte[MAX_LINE_SIZE]);
		parser = new GeoGSMOverlap();
		
		String[] argsParts = args.split(" ");
		if (argsParts.length != 5) {
			logger.error("Invalid number of arguments: got: " + argsParts.length + " of 5 required");
			throw new StageInitException("Invalid stage arguments");
		}
		pclInputDir = argsParts[0];
		gsmOverlapOutputDir = argsParts[1];
		try {
			
		} catch (NumberFormatException e) {
			minSamples = Integer.valueOf(argsParts[2]);
			maxOverlap = Integer.valueOf(argsParts[3]);
			throw new StageInitException("Inavlid number in arguments: " + args);
		}
		outputFilename = OsPath.join(globalMetaDir, argsParts[4]);
		
	}
	
	/**
	 * Retrieve a set of files to be processed by a pipeline. This function is periodically 
	 * called from the main loop.
	 * 
	 * @param metaFiles list of meta filenames that have been downloaded to the meta directory.
	 * Any new meta files are added to tis list
	 * @param logFiles list for storing log filenames.
	 * @param timestamp of Troilkatt iteration.
	 * @return list of output files in HDFS.
	 * @throws StageException thrown if stage cannot be executed.
	 * @throws IOException 
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {

		/*
		 * Get list of PCL files
		 */
		ArrayList<String> pclInputFiles;
		try {
			pclInputFiles = tfs.listdirR(pclInputDir);
			if (pclInputFiles == null) {
				logger.fatal("Could not list directory: " + pclInputDir);
				throw new StageException("Could not list directory");
			}
		} catch (IOException e) {
			logger.fatal("Could not list directory: " + e.toString());
			throw new StageException("Could not list directory");
		}
				
		try {
			/*
			 * Create list of files produced in the last iteration of GSMOverlap
			 */
			ArrayList<String> files = tfs.listdirN(gsmOverlapOutputDir);
			if (files == null) {
				logger.fatal("Could not list GSMOverlap output directory: " + gsmOverlapOutputDir);
				throw new StageException("Could not list GSMOverlap output directory: " + gsmOverlapOutputDir);
			}
			// Find highest timestamp
			long maxTimestamp = -1;
			for (String f: files) {
				long t = tfs.getFilenameTimestamp(f);
			 	if (t > maxTimestamp) {
					maxTimestamp = t;				
				}
			}

			/*
			 * Read overlap information from GSMOverlap output files
			 */
			for (String f: files) {
				// Make sure only the files that belong to the newest iteration are used
				if (tfs.getFilenameTimestamp(f) != maxTimestamp) {
					continue;
				}

				LineReader lin = tfs.openLineReader(f, stageInputDir, stageTmpDir, stageLogDir);
				if (lin == null) {
					logger.fatal("Could not open input file: " + f);			
					throw new StageException("Could not open input file: " + f);
				}

				while (lin.readLine(line, MAX_LINE_SIZE) > 0) {
					String sLine = line.toString().trim();
					parser.addOverlapLine(sLine);								
				}
				lin.close();
			}
		} catch (IOException e) {
			logger.fatal("Could not read GSMOverlap output: " + e.toString());
			throw new StageException("Could not read GSMOverlap output: " + e.toString());
		} catch (ParseException e) {
			logger.fatal("Could not parse GSMOverlap output: " + e.toString());
			throw new StageException("Could not parse GSMOverlap output: " + e.toString());
		}
		
		/*
		 * Redue overlap
		 */
		parser.find(minSamples, maxOverlap);
		ArrayList<Duplicate> duplicates = parser.getDuplicates();
		ArrayList<Superset> supersets = parser.getSupersets();
		ArrayList<OverlapSet> removed = parser.getRemoved();
		HashSet<String> removedGids = parser.getRemovedIDs();
		HashMap<String, HashSet<String>> removedSamples = parser.getRemovedSamples();
		
		/*
		 * Write output used as arguments by the overlap filter stages
		 */
		try {			
			// output file
			BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
						
			os.write("Duplicates:\n");
			for (Duplicate d: duplicates) {
				os.write(d.gid1 + "\tduplicate of\t" + d.gid2 + "\n");			
			}
						
			os.write("\nSupersets:\n");
			for (Superset s: supersets) {
				os.write(s.gid + "\tsuperset for");
				for (String g: s.subsetIDs) {
					os.write("\t" + g);
				}
				os.write("\n");
			}
						
			os.write("\nRemoved (too few samples):\n");
			for (OverlapSet o: removed) {
				os.write(o.gid + "\tsamples before and after\t" + o.nSamples + "\t" + String.valueOf(o.nSamples - o.gsmsToRemove.size()) + "\n");
			}
						
			os.write("\nSamples to remove:\n");
			for (String gid: removedSamples.keySet()) {
				os.write(gid + "\tremove (" + removedSamples.get(gid).size() + ")");
				for (String gsm: removedSamples.get(gid)) {
					os.write("\t" + gsm);
				}
				os.write("\n");
			}
			os.close();
		} catch (IOException e) {
			logger.fatal("Could not write to output file: " + e.toString());
			throw new StageException("Could not write to output file: " + e.toString());
		}
		
		/*
		 * Write log files
		 */
		String logfile = null;
		try {
			// Input PCL files
			logfile = OsPath.join(stageLogDir, "pcls");
			FSUtils.writeTextFile(logfile, pclInputFiles);
			logFiles.add(logfile);
			
			// Duplicates
			HashSet<String> gids = parser.getDuplicateIDs();
			ArrayList<String> gids2 = new ArrayList<String>();
			gids2.addAll(gids);
			logfile = OsPath.join(stageLogDir, "duplicates");
			FSUtils.writeTextFile(logfile, gids2);
			logFiles.add(logfile);
			
			// Supersets
			gids = parser.getSupersetIDs();
			gids2.clear();
			gids2.addAll(gids);
			logfile = OsPath.join(stageLogDir, "supersets");
			FSUtils.writeTextFile(logfile, gids2);
			logFiles.add(logfile);
			
			// Removed files
			logfile = OsPath.join(stageLogDir, "removed");
			gids2.clear();
			gids2.addAll(removedGids);
			FSUtils.writeTextFile(logfile, gids2);
			logFiles.add(logfile);
			
			// Removed samples
			logfile = OsPath.join(stageLogDir, "sampels");
			BufferedWriter log = new BufferedWriter(new FileWriter(new File(logfile)));
			for (String gid: removedSamples.keySet()) {
				log.write(gid);
				for (String gsm: removedSamples.get(gid)) {
					log.write("\t" + gsm);
				}
				log.write("\n");
			}
			log.close();
			logFiles.add(logfile);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + logfile);
			logger.warn(e1.toString());
		}
		
		/*
		 * Create list of output files
		 */
		ArrayList<String> outputFiles = new ArrayList<String>();
		for (String pcl: pclInputFiles) {
			String id = FilenameUtils.getDsetID(pcl);
			if (removedGids.contains(id) == false) {
				outputFiles.add(pcl);
			}
		}
		
		return outputFiles;
	}
	
	/**
	 * Function to parse arguments file created in retrieve()
	 */
}
