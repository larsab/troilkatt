package edu.princeton.function.troilkatt.pipeline;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.Table;
import edu.princeton.function.troilkatt.utils.OsPath;

/**
 * Merge .info files produced by the GSD SOFT->PCL file conversion
 */
public class MergeInfoFiles extends Stage {
	private String infoFilename = null;
	
	// Key: GSD filename, Entry: info_file line for that filename
	private HashMap<String, String> infoMap = null;
	
	/**
	 * This stage does not have any arguments.
	 *
	 * @param args: none
	 * @param: see description for super-class
	 */
	public MergeInfoFiles(String name, String args, String stageDir,
			String logDir, String tmpDir, Stage prevStage, DataTable datasetTable,
			PipelineTable pipelineTable,
			TroilkattProperties troilkattProperties, boolean processAll,
			boolean processNew, boolean processUpdated, boolean processDeleted) {
		super(name, args, stageDir, logDir, tmpDir, prevStage, datasetTable,
				pipelineTable, troilkattProperties, processAll, processNew,
				processUpdated, processDeleted);				
	}

	
	/**
	 * Function called to process data.
	 * 
	 * @param files: list of filenames to process
	 * @param fileset: fileset being processed ('all', 'new', 'updated' or 'deleted')
	 *
	 * @return: null
	 */    	
	@SuppressWarnings("unchecked")
	public void process(String files[], String fileset) {        
        if ((files == null) || (files.length == 0)) {
        	File file = new File(OsPath.join(outputDir, fileset + ".info"));		 
    		try {
    			BufferedWriter os = new BufferedWriter(new FileWriter(file));    			    			
    			os.close();
    		} catch (IOException e) {
    			logger.fatal("Could not write to info file: " + infoFilename);
    			logger.fatal(e.toString());
    			throw new RuntimeException(e);
    		}	
    		return;
        }
        
        Object o = pipelineTable.getObject(name, "infoMap", -1);
        if (o == null) {
        	infoMap = new HashMap<String, String>();
        }
        else {
        	infoMap = (HashMap<String, String>)o;
        }
        HashMap<String, String> currentMap = new HashMap<String, String>();        
        
        String headerLine = null;
        for (String f: files) {
        	try {
				String[] lines = Table.readTextFile(f);				
				if (lines.length < 2) {
					logger.warn("Info file has <2 lines: " + f);
					continue;
				}
				
				if (headerLine == null) {
					headerLine = lines[0];
				}
				
				String dataLine = lines[1];
				int firstSplit = dataLine.indexOf(" ");
				String id = OsPath.basename(dataLine.substring(0, firstSplit));
				String rest = dataLine.substring(firstSplit, dataLine.length());				
				infoMap.put(id, id + rest);		
				currentMap.put(id, id + rest);
			} catch (IOException e) {
				logger.fatal("Could not read from file: " + f);
				logger.fatal(e.toString());
				throw new RuntimeException(e);
			}
        	
        }      
        
        File file = new File(OsPath.join(outputDir,"spell.info"));		 
		try {
			BufferedWriter os = new BufferedWriter(new FileWriter(file));
			os.write(headerLine);
			for (String k: infoMap.keySet()) {		
				os.write(infoMap.get(k) + "\n");
			}
			os.close();
		} catch (IOException e) {
			logger.fatal("Could not write to info_file: " + infoFilename);
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}	
		
		file = new File(OsPath.join(outputDir, fileset + ".info"));		 
		try {
			BufferedWriter os = new BufferedWriter(new FileWriter(file));
			os.write(headerLine + "\n");
			for (String k: currentMap.keySet()) {		
				os.write(currentMap.get(k) + "\n");
			}
			os.close();
		} catch (IOException e) {
			logger.fatal("Could not write to info_file: " + infoFilename);
			logger.fatal(e.toString());
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Helper function for filling in a HashMap with objects to save in Hbase. The deault
	 * implementation is empty. Subclasses that need to save objects should override this
	 * function.
	 * 
	 * @param map: HashMap for objects (and their keys) to be saved in Hbase	
	 */
	public void getObjects(HashMap<String, Object> map) {
		map.put("infoMap", infoMap);		
	}
}
