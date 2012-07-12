package edu.princeton.function.troilkatt.clients;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.Level;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class ExecuteMapReduceStage extends TroilkattClient {
	
	// The MapReduce jar file to execute
	protected String jarFile;
	// Main class in jar file
	protected String mainClass;
	// The stage arguments
	protected String stageArgs; 
	// File with input arguments to MapReduce program
	public String argsFilename;
	// Command used to run MapReduce job
	protected String mapReduceCmd;

	public ExecuteMapReduceStage() {
		argsFilename = OsPath.join(stageTmpDir, "input.args");
		String libJars = troilkattProperties.get("troilkatt.libjars");
		mapReduceCmd = "hadoop jar " + jarFile + " " + mainClass + " -libjars " + libJars + " " + argsFilename;
	}
	
	/**
	 * Print usage information.
	 */
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] [args]\n\n" +
	
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-p PIPELINE   PIPELINE name to use.\n" +
				"\t-s NAME       Stage NAME (e.g. 003-soft2pcl) to use.\n" +
				
				"\t              (default: %s).\n" +
				"\t-h            Display command line options.", progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logging")));
	}

	/**
	 * Helper function to create an arguments file. The filename is given by the argsFilename global
	 * variable.
	 * 
	 * edu.princeton.function.troilkatt.mapreduce.TroilkattMapreduce.readMapReduceArgsfile is the corresponding
	 * to read the arguments file
	 * 
	 * @param inputFiles of input files to process
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timstamp for this iteration
	 * @return none
	 * @throws StageException if file could not be created
	 */
	public void writeMapReduceArgsFile(ArrayList<String> inputFiles, 
			String hdfsTmpOutputDir, long timestamp) throws StageException {
		// Note! write order of lines must match read order in TroilkattMapReduce.readMapReduceArgsFile
		try {
			PrintWriter out = new PrintWriter(new FileWriter(argsFilename));
			out.println("configuration.file = " + troilkattProperties.getConfigFile());
			out.println("pipeline.name = " + pipelineName);			
			out.println("stage.name = " + stageName);
			out.println("stage.args = " + stageArgs);
			out.println("hdfs.output.dir = " + hdfsTmpOutputDir);
			out.println("compression.format = " + compressionFormat);
			out.println("storage.time = " + storageTime);
			//out.println("hdfs.log.dir = " + hdfsLogDir);			
			out.println("hdfs.meta.dir = " + hdfsMetaDir);
			String mapreduceDir;
			try {
				mapreduceDir = troilkattProperties.get("troilkatt.localfs.mapreduce.dir");
			} catch (TroilkattPropertiesException e) {
				logger.fatal("Invalid properies file: " + e.getMessage());
				throw new StageException("Could not create input arguments file");
			}
			out.println("mapred.input.dir = " + OsPath.join(mapreduceDir, "input"));
			out.println("mapred.output.dir = " + OsPath.join(mapreduceDir, "output"));
			out.println("mapred.meta.dir = " + OsPath.join(mapreduceDir, "meta"));
			out.println("mapred.global-meta.dir = " + OsPath.join(mapreduceDir, "global-meta"));
			out.println("mapred.tmp.dir = " + OsPath.join(mapreduceDir, "tmp"));
			
			out.println("jobclient.input.dir = " + stageInputDir);
			out.println("jobclient.output.dir = " + stageOutputDir);
			out.println("jobclient.meta.dir = " + stageMetaDir);
			out.println("jobclient.global-meta.dir = " + globalMetaDir);
			out.println("jobclient.log.dir = " + stageLogDir);
			out.println("jobclient.tmp.dir = " + stageTmpDir);
			
			//Logger lp = logger;
			Level level = logger.getLevel(); //null;			
			//while (lp != null) {
			//	level = lp.getLevel();
			//	if (level != null) {
			//		break;
			//	}
			//	else {
			//		lp = lp.getParent();
			//	}
			//}			
			String logLevelName;
			if (level == null) { // could not find level: set to info as fallback
				logger.warn("Could not find logging level");
				logLevelName = "info";
			}
			else {
				logLevelName = level.toString();
			}
			
			out.println("logging.level = " + logLevelName);			
			out.println("timestamp = " + timestamp);
			out.println("input.files.start");
			for (String f: inputFiles) {
				out.println(f);
			}
			out.println("input.files.end");
			out.close();
		} catch (IOException e1) {
			logger.fatal("Could not create input arguments file: " + e1.getMessage());
			throw new StageException("Could not create input arguments file");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
