package edu.princeton.function.troilkatt.sink;

import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class SinkFactory {

	public static Sink newSink(String type, int stageNum, String sinkName,
			String args, 
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline, Logger logger) throws TroilkattPropertiesException, StageInitException {
		if (type.equals("copy_to_local")) {
	        return new CopyToLocalFS(stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
		else if (type.equals("copy_to_remote")) {
	        return new CopyToRemote(stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    } 
		else if (type.equals("null_sink")) {
	        return new NullSink(stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
		else if (type.equals("global_meta_sink")) {
	        return new GlobalMeta(stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	    }
		//else if (type.equals("sink")) {
       // return new Sink(stageNum, sinkName, args, pipeline);
		//} 
		else {
			logger.fatal("Unknown sink: "  + type);
	        throw new StageInitException("Unknown sink: "  + type);
		}
	}
	
	public static Sink newSink(String type, int stageNum, String sinkName,
			String args, 
			Pipeline pipeline, Logger logger) throws TroilkattPropertiesException, StageInitException {
		
		TroilkattProperties troilkattProperties = pipeline.troilkattProperties;
		String localRootDir = troilkattProperties.get("troilkatt.localfs.dir");
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", stageNum, sinkName));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		return newSink(type, stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline, logger);	    
	}
}
