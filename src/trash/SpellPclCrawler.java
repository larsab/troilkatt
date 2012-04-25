package trash;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.TroilkattFile;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.SinkTable;
import edu.princeton.function.troilkatt.tables.Table;
import edu.princeton.function.troilkatt.utils.OsPath;

/**
 * Detect updates in a sink. 
 */
public class SpellPclCrawler extends Source {
	protected String pclSinkName;
	protected String infoSinkName;

	SinkTable sinkTable;	

	FileSystem hdfs;

	/**
	 * @param arguments: pcl sink name, info sink name
	 * 
	 * Refer to the Crawl for descriptions of the rest of the arguments.
	 */
	public SpellPclCrawler(String name, String arguments, String outputDir,
			String logDir, String tmpDir, DataTable datasetTable,
			PipelineTable pipelineTable, TroilkattProperties troilkattProperties) {
		super(name, arguments, outputDir, logDir, tmpDir, datasetTable,
				pipelineTable, troilkattProperties);

		/*
		 * Initialize global data structures
		 *
		 * Note! that input and output directory is the same for this crawler        
		 */
		String[] argsParts = args.split(" ");
		pclSinkName = argsParts[0];
		infoSinkName = argsParts[1];
		sinkTable = new SinkTable(pipelineTable.hdfsConfig, pipelineTable.hbConfig, troilkattProperties);


		try {
			hdfs = FileSystem.get(pipelineTable.hdfsConfig);
		} catch (IOException e1) {
			logger.fatal("Could not get handle for HDFS");
			logger.fatal(e1.toString());
			throw new RuntimeException("Could not get handle for HDFS");
		}		
	}

	/**
	 * Check for new updates and generate outputfiles. This function is periodically called from the
	 * main loop (via the Crawl.crawl2() function)
	 *
	 * @param retrievedFiles: list of absolute filenames for new files retrieved
	 * @param deletedFiles: list of deleted files 
	 * @param logs: list of log files to save
	 * @param objects: hashmap where objects to be stored in Bigtable are saved (id -> object)   	
	 * @param timestamp: string with unique identifier for this crawl
	 *
	 * @return retrievedFiles, updatedFiles, deletedFiles, logs: are updated
	 */
	@Override
	public void crawl(ArrayList<TroilkattFile> retrievedFiles, ArrayList<TroilkattFile> deletedFiles, ArrayList<String> logs, HashMap<String, Object> objects, long timestamp) {
		logger.info("Crawl at: " + timestamp);
		
		// Download the .info files from the infoSink
		ArrayList<TroilkattFile> infoFiles = sinkTable.getAllFiles(infoSinkName, -1);
		boolean spellInfoFound = false;
		for (TroilkattFile tf: infoFiles) {	
			String filename = tf.getFilename();
			if (filename.equals("spell.info")) {
				tf.setDir(outputDir);
				tf.downloadFile(hdfs, logger);
				spellInfoFound = true;
				break;
			}
		}
		
		if (! spellInfoFound) {
			logger.fatal("spell.info not found in sink");
			throw new RuntimeException("spell.info not found in sink");
		}
		retrievedFiles.add(createTroilkattFile(outputDir, "spell.info"));
		
		HashMap<String, String> file2pubmed = getPubmedMapping("spell.info");
		
		createFiles("all", sinkTable.getAllFiles(pclSinkName, -1), file2pubmed, retrievedFiles);
		createFiles("new", sinkTable.getNewFiles(pclSinkName, -1), file2pubmed, retrievedFiles);
		createFiles("updated", sinkTable.getUpdatedFiles(pclSinkName, -1), file2pubmed, retrievedFiles);		
		
		// There are no log files
		// There are no objects
	}

	/**
	 * Create a pubmed ID file
	 * 
	 * @param infoFilename: .info filename
	 * @param outputFilename: pubmed filename 	 
	 */
	public HashMap<String, String> getPubmedMapping(String infoFilename) {
		/* Info file columns
		 *  0. File
		 *  1. DatasetID
		 *  2. Organism
		 *  3. Platform
		 *  4. ValueType
		 *  5. *channels
		 *  6. Title
		 *  7. Description
		 *  8. PubMedID
		 *  9. *features
		 * 10. *samples
		 * 11. date
		 * 12. Min
		 * 13. Max
		 * 14. Mean
		 * 15. *Neg
		 * 16. *Pos
		 * 17. *Zero
		 * 18. *MV
		 * 19. *Total
		 * 20. *Channels
		 * 21. logged
		 * 22. zerosAreMVs
		 * 23. MVcutoff
		 */

		HashMap<String, String> file2pubmed = new HashMap<String, String>();
		
		try {
			String[] lines = Table.readTextFile(OsPath.join(outputDir, infoFilename));	    							

			// first line is a comment
			for (int i = 1; i < lines.length; i++) { 
				String l = lines[i];

				String[] cols = l.split("\t");
				if (cols.length != 24) {
					logger.warn("Invalid columns count for line: " + l);
					continue;
				}
				else {
					String id = cols[0].substring(0, cols[0].indexOf('.'));
					file2pubmed.put(id, cols[8]);
				}
			}						
		} catch (IOException e) {
			logger.fatal("Could not parse info file: " + infoFilename);
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}	
		
		return file2pubmed;
	}
	
	/**
	 * Create a filelist files
	 * 
	 * @param infoFilename: .info filename
	 * @param outputFilename: pubmed filename
	 * @param pclFiles: all files in the sink 	 
	 */
	public void createFiles(String prefix, 
			ArrayList<TroilkattFile> pclFiles, 
			HashMap<String, String> file2pubmed,
			ArrayList<TroilkattFile> retrievedFiles) {
		/* Info file columns
		 *  0. File
		 *  1. DatasetID
		 *  2. Organism
		 *  3. Platform
		 *  4. ValueType
		 *  5. *channels
		 *  6. Title
		 *  7. Description
		 *  8. PubMedID
		 *  9. *features
		 * 10. *samples
		 * 11. date
		 * 12. Min
		 * 13. Max
		 * 14. Mean
		 * 15. *Neg
		 * 16. *Pos
		 * 17. *Zero
		 * 18. *MV
		 * 19. *Total
		 * 20. *Channels
		 * 21. logged
		 * 22. zerosAreMVs
		 * 23. MVcutoff
		 */

		File pubmedFile = new File(OsPath.join(outputDir, prefix + ".pubmed"));
		File filelistFile = new File(OsPath.join(outputDir, prefix + ".files"));		
		
		try {			   
			
			BufferedWriter pubmed = new BufferedWriter(new FileWriter(pubmedFile));
			BufferedWriter filelist = new BufferedWriter(new FileWriter(filelistFile));
			
			for (TroilkattFile p: pclFiles) {
				String filename = p.getFilename();
				String pclId = filename.substring(0, filename.indexOf('.'));
				
				if (file2pubmed.containsKey(pclId)) {
					pubmed.write(filename + "\t" + file2pubmed.get(pclId) + "\n");					
				}
				else {
					logger.fatal("PCL file not found in spell.info: " + pclId);
				}
				
				filelist.write(p.getHDFSFilename() + "\n");					
			}
			pubmed.close();
			filelist.close();
		} catch (IOException e) {			
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}	
		
		retrievedFiles.add(createTroilkattFile(outputDir, prefix + ".pubmed"));
		retrievedFiles.add(createTroilkattFile(outputDir, prefix + ".files"));
	}
}
