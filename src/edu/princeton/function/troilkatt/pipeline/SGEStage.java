package edu.princeton.function.troilkatt.pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.Level;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;

/** 
 * Execute a stage as an SGE job. There is one SGE task for each input file. Each task
 * comprises a stripped down troilkatt that is used to run the stage. The stage to run
 * and its arguments are all specified as stage arguments. 
 * 
 * The stripped down troilkatt is started by executing troilkatt.sge.ExecuteStage
 */
public class SGEStage extends Stage {	
	// Script run by SGE for each file
	public String scriptFilename;
	// File with input arguments to SGE executed program
	public String argsFilename;
	
	// Root directory for SGE files. This is used both for temporary output and log files
	protected String sgeDir;	
	// classpath used to start troilkatt.sge.ExecuteStage 
	protected String classPath;
	
	// args with TROILKATT symbols
	protected String stageArgs;
	
	// container bin arguments (given as stage arguments)
	protected int maxProcs;
	protected long maxVMSize; // in MB
	
	// SGE queue arguments (in properties file)
	protected int slotsPerNode; 
	// Number of slots to request for each SGE task (slotsPerNode / maxProcs)
	protected int sgePESlots;
	
	/**
	 * Constructor.
	 * 
	 * @param args stage arguments
	 *  0: stage to execute
	 *  1: maximum number of processes to run per cluster node
	 *  2: maximum per process virtual memory size (in MB)
	 *  3...: stage specific arguments 
	 * @param other see superclass description
	 */
	public SGEStage(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String nfsStageMetaDir, String nfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args,
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, nfsStageMetaDir, nfsStageTmpDir,
				pipeline);		
		
		sgeDir = troilkattProperties.get("troilkatt.globalfs.sge.dir");		
		classPath = troilkattProperties.get("troilkatt.classpath");
		try {
			slotsPerNode = Integer.valueOf(troilkattProperties.get("troilkatt.sge.slots.per.node"));
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid value for troilkatt.sge.slots.per.node in configuration file: ", e);
			throw new StageInitException("Invalid value for troilkatt.sge.slots.per.node");
		}
		scriptFilename = OsPath.join(stageTmpDir, "sge.sh");		
		argsFilename = OsPath.join(sgeDir, "sge.args");					
	
		/*
		 * Do custom parsing of arguments
		 */
		String[] argsParts = args.split(" ");
        if (argsParts.length < 3) {
        	logger.fatal("Invalid arguments: " + args);
        	logger.fatal("Usage: stageType maxProcs maxVirtualMem");
        	throw new StageInitException("Too few arguments: " + args);
        }		
        
		try {
			maxProcs = Integer.valueOf(argsParts[1]);
			maxVMSize = Long.valueOf(argsParts[2]);			
		} catch (NumberFormatException e) {
			logger.fatal("Invalid max memory size argument: ", e);
			throw new StageInitException("Invalid number for maximum troilkatt or task vmem: " + argsParts[1] + " or " + argsParts[2]);
		}
		
		sgePESlots = slotsPerNode / maxProcs;
		if (slotsPerNode % maxProcs != 0) {
			sgePESlots++;
		}
		
		stageArgs = argsParts[0];
        for (int i = 3; i < argsParts.length; i++) {
        	String p = argsParts[i];
        	if (stageArgs == null) {
        		stageArgs = p;
        	}
        	else {
        		stageArgs = stageArgs + " " + p;
        	}
        }        
	}
	
	/**
	 * The downloadInputFiles is overridden such that the input files are not downloaded.
	 * Instead the SGE tasks will do this.
	 */
	@Override
	public ArrayList<String> downloadInputFiles(ArrayList<String> nfsFiles) throws StageException {
		return nfsFiles;
	}
	
	/**
	 * Function called to save the output files created by this stage.
	 * 
	 * @param localFiles list of files on local FS to save
	 * @return list of filenames in NFS
	 * @throws StageException if one or more files could not be saved
	 */
	@Override
	 public ArrayList<String> saveOutputFiles(ArrayList<String> localFiles, long timestamp) throws StageException {
		// Move files from tmp to permanent storage
		ArrayList<String> nfsFiles = new ArrayList<String>();
		
		String nfsTmpOutputDir = OsPath.join(sgeDir, getStageID() + "-" + timestamp + "/output");
		
		for (String f: localFiles) {			
			String relName = OsPath.absolute2relative(f, nfsTmpOutputDir);
			String newName = OsPath.join(tfsOutputDir, relName);
			String dirName = OsPath.dirname(newName);
			
			try {
				if (! tfs.isdir(dirName)) {
					tfs.mkdir(dirName);
				}

				if (tfs.renameFile(f, newName) == false) {
					logger.warn("Could not move output file to: " + newName);					
					throw new StageException("Could not move output file to: " + newName);
				}
			} catch (IOException e) {
				logger.error("Could not move tmp output files to persistent output directory");
				throw new StageException("Could not move output file to: " + newName);
			}
			
			nfsFiles.add(newName);			
		}	
		
		return nfsFiles;
	 }
	
	/**
	 * Helper function to write an SGE script
	 * @throws StageException 
	 */
	public void writeSGEScript(String nfsTmpLogDir) throws StageException {
		/*
		 * Script file
		 */
		try {
			PrintWriter out = new PrintWriter(new FileWriter(scriptFilename));
			
			out.write("#!/bin/bash\n\n");
			
			// SGE name
			out.write("#$ -N troilkatt.ExecuteStage\n");
			out.write("#$ -S /bin/bash\n\n");
			
			/*
			 * Command to execute
			 */
			// java command									
			out.write("java -Xmx2048m -classpath " + classPath + " edu.princeton.function.troilkatt.sge.ExecuteStage ");
			// 1st argument: arguments file location
			out.write(argsFilename);
			// 2nd argument: task ID
			out.write(" ${SGE_TASK_ID}");
			// 3rd argument: SGE job ID
			out.write(" sge_job");
			// Save log files in the tmp NFS log dir
			out.write(" > ");
			out.write(OsPath.join(nfsTmpLogDir, "sge_${SGE_TASK_ID}.out"));			
			// Cannot redirect both stdout and stderr in SGE???
			out.write(" 2> ");
			out.write(OsPath.join(nfsTmpLogDir, "sge_${SGE_TASK_ID}.err"));			
			out.write("\n\n");
			
			//File must be executable otherwise sge job won't run it
			File file = new File(scriptFilename);
			file.setExecutable(true);

			
			out.close();
		} catch (IOException e1) {
			logger.fatal("Could not create input SGE scripts file: " + e1.getMessage());
			throw new StageException("Could not create SGE scripts file");
		}
	}
	
	/**
	 * Helper function to create an arguments file. The filename is given by the argsFilename global
	 * variable.
	 * 
	 * edu.princeton.function.troilkatt.sge.ExecuteStage.readSHEArgsfile is the corresponding method
	 * to read the arguments file
	 * 
	 * @param inputFiles of input files to process
	 * @param nfsTmpOutputDir output directory for MapReduce job
	 * @param timstamp for this iteration
	 * @return none
	 * @throws StageException if file could not be created
	 */
	public void writeSGEArgsFile(String nfsTmpOutputDir, String nfsTmpLogDir, long timestamp, ArrayList<String> inputFilenames) throws StageException {
		// Note! write order of lines must match read order in TroilkattMapReduce.readMapReduceArgsFile
		try {
			PrintWriter out = new PrintWriter(new FileWriter(argsFilename));
			out.println("configuration.file = " + troilkattProperties.getConfigFile());
			out.println("pipeline.name = " + pipelineName);			
			out.println("stage.name = " + stageName);			
			out.println("stage.args = " + stageArgs);
			out.println("nfs.output.dir = " + nfsTmpOutputDir); // tmp storage for output files
			out.println("compression.format = " + compressionFormat);
			out.println("storage.time = " + storageTime);
			out.println("nfs.log.dir = " + nfsTmpLogDir); // tmp storage for log files		
			out.println("nfs.meta.dir = " + tfsMetaDir); // shared among all tasks
			String localSgeDir;
			try {
				localSgeDir = troilkattProperties.get("troilkatt.localfs.sge.dir");
			} catch (TroilkattPropertiesException e) {
				logger.fatal("Invalid properies file: " + e.getMessage());
				throw new StageException("Could not create input arguments file");
			}
			out.println("sge.pipeline.dir = " + OsPath.join(localSgeDir, "pipeline"));
			out.println("sge.tmp.dir = " + OsPath.join(localSgeDir, "tmp"));
		
			
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
			
			out.println("max.num.procs = " + maxProcs);
			out.println("max.vm.size = " + maxVMSize);
			
			out.println("input.files.start");
			for (String f: inputFilenames) {
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
	 * Move SGE job log files to a local directory. This function is called befor compressing
	 * the data.
	 * 
	 * @param nfsTmpOutputDir output directory for MapReduce job
	 * @param timestamp timestamp to add to output files
	 * @return none
	 * @throws StageException 
	 */
	protected void moveSGELogFiles(String nfsTmpLogDir, ArrayList<String> logFiles) throws StageException {	
		ArrayList<String> tmpFiles;
		try {
			tmpFiles = tfs.listdirR(nfsTmpLogDir);			
		} catch (IOException e) {
			logger.fatal("Could not read list of outputfiles in NFS: " + e.toString());
			throw new StageException("Could not read list of outputfiles in MFS");
		}
		
		if (tmpFiles == null) {
			return;
		}
		
		for (String f: tmpFiles) {			
			String relName = OsPath.absolute2relative(f, nfsTmpLogDir);
			String newName = OsPath.join(stageLogDir, relName);
			String dirName = OsPath.dirname(newName);
			
			try {
				if (! tfs.isdir(dirName)) {
					tfs.mkdir(dirName);
				}

				if (tfs.renameFile(f, newName) == false) {
					logger.warn("Could not move log file to: " + newName);
					logger.warn("Skipping log file: " + f);
					continue;
				}
			} catch (IOException e) {
				logger.error("Could not move local log files to shared logfile directory");
			}
			
			logFiles.add(newName);			
		}		
	}

	/**
	 * Run the MapReduce program and update file lists.
	 * 
	 * @param inputFiles list of NFS input files to process
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files
	 * @throws StageException if stage cannot be executed
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {						
		
		// Temporary SGE output directory on NFS
		String nfsTmpOutputDir = OsPath.join(sgeDir, getStageID() + "-" + timestamp + "/output");
		OsPath.mkdir(nfsTmpOutputDir);
		// Root for task log files
		String nfsTmpLogDir = OsPath.join(sgeDir, getStageID() + "-" + timestamp + "/log");
		OsPath.mkdir(nfsTmpLogDir);
		
		// Create arguments file for MapReduce Job-task
		writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, timestamp, inputFiles);
		
		// Create SGE scripts and filenames file
		writeSGEScript(nfsTmpLogDir);
		
		// Redirect output and execute SGE job
		String outputLogfile = OsPath.join(stageLogDir, "sge.output");
		String errorLogfile = OsPath.join(stageLogDir, "sge.error");
		
		// execute sge job
				
		// Submit and wait for completion
		String sgeCmd = getCmd(inputFiles.size(), outputLogfile, errorLogfile, nfsTmpLogDir);
		int rv = Stage.executeCmd(sgeCmd , logger);

		// Always update log files even if job crashes
		updateLogFiles(logFiles);
		// Also, move all log files to a single directory on local fs
		moveSGELogFiles(nfsTmpLogDir, logFiles);
				
		if (rv != 0) {
			logger.warn("SGE job failed with error code: " + rv);
			logger.warn("Suppressing job failure: job to be continued.");
			//throw new StageException("SGE job failed");
		}
		
		// These are not executed in case the job fails
		
		// Update list of meta and log files 
		updateMetaFiles(metaFiles);		
		
		// Output filenames will be saved as normal
		try {
			return tfs.listdir(nfsTmpOutputDir);
		} catch (IOException e) {
			logger.error("Could not list output dir" + e);
			throw new StageException("Could not list output dir");
		}
	}
	
	/**
	 * Return SGE command to execute
	 * 
	 * 
	 * @param memSize: memory size in megabytes
	 */
	protected String getCmd(int nInputFiles, String outputLogfile, String errorLogfile, String tmpLogdir) {
		// Note SGE task ID indexes starts from one (and not zero), and range includes last index
		return String.format("qsub -sync y -l h_vmem=%dM -pe mpi %d -wd %s -t %d-%d %s > %s 2> %s", maxVMSize, sgePESlots, tmpLogdir, 1, nInputFiles, scriptFilename, outputLogfile, errorLogfile);
	}
	
}
        
        
