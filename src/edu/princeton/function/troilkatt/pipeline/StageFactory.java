package edu.princeton.function.troilkatt.pipeline;

import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Factory for creating stage objects.
 */
public class StageFactory {
	/*
	 * List of valid stage names.
	 */
	public static final String[] stageNames = {
		"filter", 
		"execute_per_dir",	"execute_per_file", "execute_per_file_mr", "execute_per_file_sge",
		"script_per_file", "script_per_file_mr", "script_per_file_sge", "script_per_dir", 
		"mapreduce", "mapreduce_stage", "sge_stage",
		"null_stage",
		"find_gsm_overlap", "find_gsm_overlap_mongodb",
		"save_filelist", "save_filelist_mongodb"};
	
	/**
	 * This function is called to create a new pipeline stage instance. 
	 *
	 * @param type the stage type
	 * @param stageNum stage number in pipeline.
	 * @param name name of the stage.
	 * @param args stage specific arguments.
	 * @param outputDirectory output directory in HDFS. The directory name is either relative to
	 * the troilkatt root data directory, or absolute (starts with either "/" or "hdfs:/")
	 * @param compressionFormat compression to use for output files
	 * @param storageTime persistent storage time for output files in days. If -1 files
	 * are stored forever. If zero files are deleted immediately after pipeline execution is done.
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param hdfsStageMetaDir meta file directory for this stage in HDFS.
	 * @param hdfsStageTmpDir tmp directory for this stage in HDFS  (can be null).
	 * @param pipeline reference to the pipeline this stage belongs to. 
	 * @param logger: callers logger instance
	 *
	 * @return a new stage instance.
	 * 
	 * @throws TroilkattPropertiesException 
	 * @throws StageInitException 
	 */
	public static Stage newStage(String type, 
			int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline,		
			Logger logger) throws TroilkattPropertiesException, StageInitException {
	    
	    if (type.equals("filter")) {
	        return new Filter(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("execute_per_dir")) {	        
	        return new ExecuteDir(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime,
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("execute_per_file")) {	        
	        return new ExecutePerFile(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("execute_per_file_mr")) {	        
	        return new ExecutePerFileMR(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("execute_per_file_sge")) {	        
	        return new ExecutePerFileSGE(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("script_per_file")) {	        
	        return new ScriptPerFile(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("script_per_file_mr")) {	        
	        return new ScriptPerFileMR(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    else if (type.equals("script_per_file_sge")) {	        
	        return new ScriptPerFileSGE(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);				
	    }
	    //else if (type.equals("script_per_file_sge")) {	        
	    //    return new ScriptPerFileSGE(stageNum, name, args, 
		//			outputDirectory, compressionFormat, storageTime, 
		//			localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
		//			pipeline);				
	    //}
	    else if (type.equals("script_per_dir")) {	     
	        return new ScriptPerDir(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);			
	    }
	    //else if (type.equals("script_per_file")) {	     
	    //   return new ScriptPerFile(stageNum, name, args, 
		//			outputDirectory, compressionFormat, storageTime, 
		//			pipeline);
		//			
	    //}	      
	    else if (type.equals("mapreduce")) {	 	    	
	        return new MapReduce(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);					
	    }
	    else if (type.equals("mapreduce_stage")) {	 
	    	return new MapReduceStage(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);					
	    }
	    else if (type.equals("sge_stage")) {	 
	    	return new SGEStage(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);					
	    }
	    else if (type.equals("null_stage")) {	 
	    	return new NullStage(stageNum, name, args, 
					outputDirectory, compressionFormat, storageTime, 
					localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
					pipeline);					
	    }
	    else if (type.equals("find_gsm_overlap")) {	     
		     return new FindGSMOverlap(stageNum, name, args, 
						outputDirectory, compressionFormat, storageTime, 
						localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
						pipeline);
	    }
	    else if (type.equals("find_gsm_overlap_mongodb")) {	     
		     return new FindGSMOverlapMongoDB(stageNum, name, args, 
						outputDirectory, compressionFormat, storageTime, 
						localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
						pipeline);
	    }
	    else if (type.equals("save_filelist")) {	     
	    	return new SaveFilelist(stageNum, name, args, 
	    			outputDirectory, compressionFormat, storageTime, 
	    			localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
	    			pipeline);
	    }
	    else if (type.equals("save_filelist_mongodb")) {	     
	    	return new SaveFilelistMongoDB(stageNum, name, args, 
	    			outputDirectory, compressionFormat, storageTime, 
	    			localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
	    			pipeline);
	    }
	    /*  Add more stages here
	     * else if (type.equals("name")) {	     
	     *    return new Class(stageNum, name, args, 
		 *			outputDirectory, compressionFormat, storageTime, 
		 *			localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
		 *			pipeline);
	     *	
	     *}
	     */
	    else {
	        logger.fatal("Unknown pipeline step: " + type);
	        throw new StageInitException("Unknown pipeline step: " +  type);
	    }
	}	
	
	/**
	 * This function is called to create a new pipeline stage instance. 
	 *
	 * @param type the stage type
	 * @param stageNum stage number in pipeline.
	 * @param name name of the stage.
	 * @param args stage specific arguments.
	 * @param outputDirectory output directory in HDFS. The directory name is either relative to
	 * the troilkatt root data directory, or absolute (starts with either "/" or "hdfs:/")
	 * @param compressionFormat compression to use for output files
	 * @param storageTime persistent storage time for output files in days. If -1 files
	 * are stored forever. If zero files are deleted immediately after pipeline execution is done.
	 * @param pipeline reference to the pipeline this stage belongs to. 
	 * @param logger: callers logger instance
	 *
	 * @return a new stage instance.
	 * 
	 * @throws TroilkattPropertiesException 
	 * @throws StageInitException 
	 */
	public static Stage newStage(String type, 
			int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,			
			Pipeline pipeline,		
			Logger logger) throws TroilkattPropertiesException, StageInitException {
	    
		TroilkattProperties troilkattProperties = pipeline.troilkattProperties;
		String localRootDir = troilkattProperties.get("troilkatt.localfs.dir");
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", stageNum, name));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		return newStage(type, stageNum, name, args,	outputDirectory, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline, logger);				
	}	
	
	/**
	 * Test if a stage name is valid.
	 * 
	 * @param stageName name to test
	 * @return true if valid. 
	 */
	public static boolean isValidStageName(String stageName) {
		for (String s: stageNames) {
			if (s.equals(stageName)) {
				return true;
			}
		}
		return false;
	}
}
