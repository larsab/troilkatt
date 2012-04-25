import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.TroilkattFile2;

public class Stage {
	/**
	 * Remove output directory from all filenames in a list
	 * 
	 * @param files: list of files
	 *
	 * @return: lsit of files with ouput directory removed from filenames.
	 */
	public String[] stripOutputDir(String files []) {
		if (files == null) { 
			return null;
		}

		String relativeList[] = new String[files.length];

		for (int i = 0; i < files.length; i++) {
			String f = files[i];
			logger.debug("Replace " + outputDir + " with <space> in" + f);
			System.out.println("Class is: " + this.getClass());
			relativeList[i] = f.replace(outputDir, "");
		}

		return relativeList;
	}

	/**
	 * Move all new files into a new directory and return the directory name. Use 
	 * getFileList(dirname, true)to get the list of all the files. 
				throw new StageException("Could not save log files")
			}

	 *
	 * * @return: directory where new files are stored (null if no new files)
	 */
	public String createDirCopyFiles() {
		if (newFiles.size() == 0) {
			return null;
		}               

		String newDir = OsPath.join(outputDir, "new"); 
		if (! OsPath.mkdir(newDir)) {
			logger.fatal("Could not create new directory: " + newDir);
			throw new RuntimeException("mkdir failed");
		}

		for (String f: getNewFiles(true)) {
			String basename = OsPath.basename(f);
			if (deleteList.containsKey(basename)) {
				logger.debug("File already processed: " + basename);
				continue;
			}
			
			String newName = OsPath.join(newDir, basename);			
			try {
				logger.debug("Copy " + f + " to " + newName);
				OsPath.copy(f, newName);
			} catch (IOException e) {
				logger.fatal("Could not copy file to new directory: " + f);
				logger.fatal(e.toString());
				throw new RuntimeException("copy failed");
			}			

			// Also maintain an undo-list such that the files can be deleted when
			// the processing is done
			deleteList.put(basename, newName);
		}

		return newDir; 
	}
	
	/**
	 * Helper function to verify that files to process are in the input directory. 
	 * Note that subclasses that handle file download by themselves should override 
	 * this function.
	 * 
	 * @param inputFiles file list to check	
	 */
	protected void verifyInputFiles(ArrayList<TroilkattFile2> inputFiles) {				
		for (TroilkattFile2 tf: inputFiles) {
			if (! OsPath.isfile(tf.localName)) {
				logger.fatal("Input file not in input directory:" + tf.localName);
				throw new RuntimeException("Input file not in input directory:" + tf.localName);
			}							
		}
	}
	
	/**
	 * @return HDFS output directory name
	 */
	public String getHDFSOutputDir() {
		if (hdfsOutputDir == null) {
			logger.fatal("getOutputDir called, but outputDir is not set");
		}
		return hdfsOutputDir;
	}

	/**
	 * Return all files in a directory. This function is typically used to get all files 
	 * in the output directory
	 *
	 * @param dir directory
	 * @param absolute true if absolute filenames should be returned (default).
	 *  
	 * @return list with filenames (absolute or relative), or an empty list if the 
	 *  directory is empty;
	 */
	protected String[] getFileList(String dir, boolean absolute) {
		return OsPath.listdir(dir, false, absolute, this.logger);
	}
	
	/**
	 * Return all files in a directory. This function is typically used to get all files 
	 * in the output directory
	 *
	 * @param dir directory
	 * @param absolute true if absolute filenames should be returned (default).
	 *  
	 * @return list with filesnames (absolute or relative), or an empty list if the 
	 *  directory is empty;
	 */
	protected String[] getRecursiveFileList(String dir, boolean absolute) {
		return OsPath.listdir(dir, true, absolute, this.logger);
	}
	
	/**
	 * Execute a command (*nix only)
	 * 
	 * @param cmd command to execute
	 * @param logger logger instance
	 * @return cmd return value, or -1 if the command could not be executed or the command
	 * was interrupted.
	 */
	@Deprecated
	public static int executeCmd2(String cmd, Logger logger) {		
		/* 
		 * It is necessary to execute the command using a shell in order to (esily) redirect
		 * stdout and stderr to a user specified file.
		 */
		String[] cmdParts = cmd.split(" ");
		ArrayList<String> cmdList = new ArrayList<String>();
		
		boolean redirectOutput = false; // set to true when > is encountered in cmd
		boolean redirectError = false; // set to true when 2> is encountered in cmd
		String outputFilename = null;
		String errorFilename = null;
		for (String p: cmdParts) {
			if (redirectOutput) {
				outputFilename = p;
				redirectOutput = false;
			}
			else if (redirectError) {
				errorFilename = p;
				redirectError = false;
			}
			else if (p.equals(">")) {
				redirectOutput = true;
			}
			else if (p.equals("2>")) {
				redirectError = true;
			}
			else {
				cmdList.add(p);
			}
		}

		ProcessBuilder pb = new ProcessBuilder(cmdList);
		logger.debug("Execute: " + cmd);

		Process child;
		try {
			child = pb.start();
			int exitValue = 0;

			/* Wait until the cmd completes */
			logger.debug("Waiting for command to complete");			
			child.waitFor();
			exitValue = child.exitValue();
			if (exitValue != 0) {
				logger.warn("Child exit value was: " + exitValue);					
			}
			
			if (outputFilename != null) {
				InputStream stdout = child.getInputStream();
				OutputStream os = new FileOutputStream(outputFilename);
				IOUtils.copy(stdout, os);
			}
			if (errorFilename != null) {	
				InputStream stderr = child.getErrorStream();
				OutputStream os = new FileOutputStream(errorFilename);
				IOUtils.copy(stderr, os);
			}			

			return exitValue;
		} catch (IOException e) {
			logger.warn(String.format("Failed to execute: %s", cmd));
			logger.warn(e.toString());
		} catch (InterruptedException e) {
			logger.warn("Wait for child to complete was interrupted");
			logger.warn(e.toString());
		}
		
		return -1;
	}
}
