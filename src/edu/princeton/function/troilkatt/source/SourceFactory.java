package edu.princeton.function.troilkatt.source;

import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class SourceFactory {
	/**
	 * This function is called to create a new crawler instance. 
	 *
	 *When writing a new crawler it needs to be added here.
	 *
	 * @param type the crawler type
	 * @param sourceName unique identifier for this source
	 * @param args crawler specific arguments
	 * @param outputDir directory where the crawler stores donwloaded and intermediate files
	 * @param logDir directory where all logfiles are stored
	 * @param tmpDir directory for temporary files
	 * @param datasetTable a dataset table object
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param hdfsStageMetaDir meta file directory for this stage in HDFS.
	 * @param hdfsStageTmpDir tmp directory for this stage in HDFS  (can be null).
	 * @param pipelineTable tables.pipeline.Pipeline object
	 * @param troilkattProperties troilkatt properties object
	 * @param logger callers logger object
	 *	    
	 * @return a new crawler object
	 * @throws StageInitException 
	 * @throws TroilkattPropertiesException 
	 */
	public static Source newSource(String type, String sourceName, String args, 
			String outputDir, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline,
			Logger logger) throws TroilkattPropertiesException, StageInitException {
		
	    if (type.equals("array_express")) {
	        return new ArrayExpressMirror(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("execute_source")) {
	        return new ExecuteSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("file_source")) {
	        return new FileSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("geo_gds_mirror")) {	        
	        return new GeoGDSMirror(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("geo_gse_mirror")) {	        
	        return new GeoGSEMirror(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("geo_raw_mirror")) {	        
	        return new GeoRawMirror(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("geo_raw_org")) {	        
	        return new GeoRawOrg(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("list_dir")) {	        
	        return new ListDir(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("list_dir_diff")) {	        
	        return new ListDirDiff(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("list_dir_new")) {
	    	return new ListDirNew(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }	    
	    else if (type.equals("null_source")) {
	    	return new NullSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("script_source")) {
	    	return new ScriptSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
	    else if (type.equals("hbase_source")) {
		    return new HbaseSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		}
	    else if (type.equals("mongodb_source")) {
		    return new MongoDBSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		}
	    else if (type.equals("href_source")) {
		    return new HREFSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		}
	    else if (type.equals("os_cmds_source")) {
		    return new OsCmdsSource(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		}
	    // Add more crawlers here
	    //else if (type.equals("source")) {
	    //    return new Source(sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    //}
	    //
	    else {
	        logger.fatal("Unknown source: "  + type);
	        throw new StageInitException("Unknown source: "  + type);
	    }
	}
	
	/**
	 * This function is called to create a new crawler instance. 
	 *
	 *When writing a new crawler it needs to be added here.
	 *
	 * @param type the crawler type
	 * @param sourceName unique identifier for this source
	 * @param args crawler specific arguments
	 * @param outputDir directory where the crawler stores donwloaded and intermediate files
	 * @param logDir directory where all logfiles are stored
	 * @param tmpDir directory for temporary files
	 * @param datasetTable a dataset table object
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param hdfsStageMetaDir meta file directory for this stage in HDFS.
	 * @param hdfsStageTmpDir tmp directory for this stage in HDFS  (can be null).
	 * @param pipelineTable tables.pipeline.Pipeline object
	 * @param troilkattProperties troilkatt properties object
	 * @param logger callers logger object
	 *	    
	 * @return a new crawler object
	 * @throws StageInitException 
	 * @throws TroilkattPropertiesException 
	 */
	public static Source newSource(String type, String sourceName, String args, 
			String outputDir, String compressionFormat, int storageTime,
			Pipeline pipeline,
			Logger logger) throws TroilkattPropertiesException, StageInitException {
		
		TroilkattProperties troilkattProperties = pipeline.troilkattProperties;
		String localRootDir = troilkattProperties.get("troilkatt.localfs.dir");
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, sourceName));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		return newSource(type, sourceName, args, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline, logger);
	}
}
