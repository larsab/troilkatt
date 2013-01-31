package edu.princeton.function.troilkatt.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;


/**
 * Troilkatt file system wrapper superclass.
 * 
 */
public class TroilkattFS {
	/* List of compression algorithms supported by Troilkatt */
	public static final String[] compressionExtensions = {
		"none", /* no compression */
		"gz",   /* gnu zip compression */
		"bz2",  /* bzip compression */
	};
		
	protected Logger logger;
	
	/**
	 * Constructor.
	 * 
	 */
	public TroilkattFS() {		
		logger = Logger.getLogger("troilkatt.tfs");
	}
	
	/**
	 * Do a (non-recursive) directory listing.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdir(String hdfsDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Do a recursive listing of files.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException 
	 */
	public ArrayList<String> listdirR(String hdfsDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Get a list of the files with the newest timestamps in a directory. The listing
	 * is recursive.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdirN(String hdfsDir) throws IOException {
		ArrayList<String> allFiles = listdirR(hdfsDir);
		if (allFiles == null) {
			logger.warn("No files in directory: " + hdfsDir);
			return null;
		}
					
		HashMap<String, Long> name2timestamp = new HashMap<String, Long>();
		HashMap<String, String> name2fullname = new HashMap<String, String>();
		for (String f: allFiles) {
			String name = getFilenameName(f);
			long timestamp = getFilenameTimestamp(f);
			
			if ((name == null) || (timestamp == -1)) {
				logger.warn("File without a valid timestamp: " + OsPath.basename(f));
				continue;
			}
			
			if (name2timestamp.containsKey(name)) {
				long currentTimestamp = name2timestamp.get(name);
				if (timestamp > currentTimestamp) {
					name2timestamp.put(name, timestamp);
					name2fullname.put(name, f);
				}
			}
			else {
				name2timestamp.put(name, timestamp);
				name2fullname.put(name, f);
			}
		}
		
		ArrayList<String> newestFiles = new ArrayList<String>(); 
		for (String n: name2fullname.keySet()) {
			newestFiles.add(name2fullname.get(n));
		}
		
		return newestFiles;
	}
	
	/**
	 * Get a list of the files with a specified timestamps in a directory. The listing
	 * is recursive.
	 * 
	 * @param hdfsDir directory to list
	 * @param timestamp timestamp files are matched against
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdirT(String hdfsDir, long timestamp) throws IOException {
		ArrayList<String> allFiles = listdirR(hdfsDir);
		if (allFiles == null) {
			logger.warn("No files in directory: " + hdfsDir);
			return null;
		}
					
		ArrayList<String> matchedFiles = new ArrayList<String>();
		for (String f: allFiles) {
			long ft = getFilenameTimestamp(f);
			if (ft == timestamp) {
				matchedFiles.add(f);
			}
		}
		
		return matchedFiles;
	}
	
	/**
	 * Get the subdirectory directory with the newest timestamp.
	 * 
	 * Note! the input directory is assumed to have sub-directories of the type:
	 * "timestamp.compression", and further it is assumed that only the timestamp differ
	 * among the subdirectories.
	 * 
	 * @param hdfsDir directory with sub-directories to check
	 * @return sub-directory (basename) with highest timestamp, or null if no timestamped directory
	 * is found, or null if multiple timestamped directories with different names are found.
	 * @throws IOException 
	 */
	public String getNewestDir(String hdfsDir) throws IOException {
		ArrayList<String> subdirs = listdir(hdfsDir);
		
		if (subdirs == null) {
			logger.warn("Invalid directory: " + hdfsDir);
			return null;
		}
		else if (subdirs.size() == 0) {
			logger.warn("No timestamped sub-directories in: " + hdfsDir);
			return null;
		}
				
		long maxTimestamp = -1;
		String newestSubdir = null;
		for (String s: subdirs) {
			long timestamp = getDirTimestamp(s);
			if (timestamp == -1) {
				logger.warn("Invalid subdirectory name: " + s);
				continue;
			}
			
			if (timestamp > maxTimestamp) {
				maxTimestamp = timestamp;
				newestSubdir = OsPath.basename(s);
			}
		}
		
		return newestSubdir;
	}
	
	/**
	 * Copy file from HDFS to the local FS directory, uncompress the file,
	 * and remove the timestamp and compression extension. 
	 * 
	 * @param hdfsName filename in HDFS. The file must have a timestamp and compression extension.
	 * @param localDir directory on local FS where file is copied and uncompressed to.
	 * @param tmpDir directory on local FS used to hold file before compression
	 * @param logDir directory where logfiles are written to.
	 * @return local FS filename (joined with localDir), or null if the file could not be 
	 * copied/uncompressed
	 * @throws IOException 
	 */
	public String getFile(String hdfsName, String localDir, String tmpDir, String logDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Copy all files in a HDFS directory to a local FS directory, uncompress the file,
	 * and remove the timestamps and compression extension.
	 * 
	 * @param hdfsName HDFS directory with files to download
	 * @param localDir directory where files are written to
	 * @param logDir directory for logfiles
	 * @param tmpDir directory where temporal files are stored during uncompression
	 * @return list of filenames on local FS, or null if the directory name is invalid or one or more files could
	 * not be downloaded
	 * @throws IOException 
	 */
	public ArrayList<String> getDirFiles(String hdfsName, String localDir, String logDir, String tmpDir) throws IOException {		
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Compress a file, add a timestamp, and move the file from local FS to HDFS.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to HDFS and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.
	 * @param hdfsOutputDir HDFS output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return HDFS filename on success, null otherwise
	 */
	public String putLocalFile(String localFilename, String hdfsOutputDir, String tmpDir, String logDir, 
			String compression, long timestamp) {
		
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Compress a file and move the file from local FS to HDFS.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to HDFS and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.
	 * @param hdfsOutputDir HDFS output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return HDFS filename on success, null otherwise
	 */
	public String putLocalFile(String localFilename, String hdfsOutputDir, String tmpDir, String logDir, String compression) {
		throw new RuntimeException("Method not implemented");
	}
	

	/**
	 * Create a directory using the timestamp as name, compress  the directory files, and copy the 
	 * compressed directory from local FS to HDFS 
	 * 
	 * @param hdfsDir HDFS directory where new sub-directory is created
	 * @param timestamp name for new sub-directory
	 * @param metaFiles files to copy to HDFS
	 * @param localFiles list of absolute filenames in directory to copy to HDFS
	 * @param compression method to use for directory 
	 * @param logDir directory where logfiles are stored
	 * @param tmpDir directory on local FS where compressed directory is stored
	 * @return true if all files were copied to HDFS, false otherwise
	 */
	public boolean putLocalDirFiles(String hdfsDir, long timestamp, ArrayList<String> localFiles, 
			String compression, String logDir, String tmpDir) {		
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Move a file from a temporary directory to an output directory. A timestamp
	 * will be added to the filename and the file will be compressed if necessary.
	 * 
	 * @param srcFilename source filename
	 * @param dstDir destination directory
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timsetamp to add to file
	 * @return destination filename, or null if the file could not be moved
	 * @throws IOException 
	 */
	public String putHDFSFile(String srcFilename, String dstDir,
			String tmpDir, String logDir, 
			String compression, long timestamp) throws IOException {
		
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Move a file from a temporary directory to an output directory. The source
	 * filename is assumed to have a valid timestamp and compression format.
	 * 
	 * @param srcFilename source filename
	 * @param dstDir destination directory
	 * @return destination filename, or null if the file could not be moved
	 * @throws IOException 
	 */
	public String putHDFSFile(String srcFilename, String dstDir) throws IOException { 
			
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Compress a file using an external program.
	 * 
	 * @param localFilename file to compress.
	 * @param outpitDir directory where compressed file is written
	 * @param logDir directory where log files are written.
	 * @param compression format to use.
	 * @return compressed filename, or null if compression failed.
	 */
	public String compressFile(String localFilename, String outputDir, String logDir, String compression) {
		String basename = OsPath.basename(localFilename);
		String cmd = null;
		String compressedFilename = null;
		if (compression.equals("gz") || compression.equals("gzip")) {
			compressedFilename = OsPath.join(outputDir, basename + ".gz");
			cmd = String.format("gzip -c %s > %s 2> %s",
					localFilename,				
					compressedFilename,					
					OsPath.join(logDir, "gzip." + basename + ".error"));				
		}
		else if (compression.equals("bz2") || compression.equals("bzip")) {
			compressedFilename = OsPath.join(outputDir, basename + ".bz2");
			cmd = String.format("bzip2 -c %s > %s 2> %s",
					localFilename,
					compressedFilename,
					OsPath.join(logDir, "bzip." + basename + ".error"));               
		}
		//else if (compression.equals("zip")) {
		//	compressedFilename = OsPath.join(outputDir, basename + ".zip");
		//	cmd = String.format("zip %s %s > %s 2> %s",										
		//			compressedFilename,
		//			localFilename,
		//			OsPath.join(logDir, "zip." + basename + ".output"),
		//			OsPath.join(logDir, "zip." + basename + ".error"));
		//}  
		else {
			logger.fatal("Unknown compression format: " + compression);
			return null;
		}
		
		int rv = Stage.executeCmd(cmd, logger);
		if (rv != 0) {
			return null;
		}
		
		return compressedFilename;
	}
	
	/**
	 * Uncompress a file by executing an external program.
	 * 
	 * @param compressedName file to uncompress
	 * @param outputFilename filename of uncompressed file
	 * @param logDir directory where logfiles are written
	 * @return true if file was successfully compressed, false otherwise.
	 */
	public boolean uncompressFile(String compressedName, String uncompressedName, String logDir) {
		String basename = OsPath.basename(compressedName);
		String cmd = null;
		if (basename.endsWith(".gz") || basename.endsWith(".Z")) {
			cmd = String.format("gunzip -cf %s > %s 2> %s",
					compressedName,
					uncompressedName,        	    		
					OsPath.join(logDir, "gunzip." + basename + ".error"));        	    
		}		
		//else if (basename.endsWith(".zip")) {
		//	cmd = String.format("unzip -d %s %s %s > %s 2> %s",
		//			OsPath.dirname(uncompressedName),
		//			compressedName,
		//			OsPath.basename(uncompressedName),
		//			OsPath.join(logDir, "unzip." + basename + ".output"),
		//			OsPath.join(logDir, "unzip." + basename + ".error"));
		//}  
		else if (basename.endsWith(".bz2") || basename.endsWith(".bz")) {
			cmd = String.format("bunzip2 -c %s > %s 2> %s",
					compressedName,
					uncompressedName,
					OsPath.join(logDir, "bunzip." + basename + ".error"));
		}
		else {
			logger.warn("Unknown extension for file: " + basename);
			return false;
		}
		
		int rv = Stage.executeCmd(cmd, logger);
		if (rv != 0) {
			return false;
		}
		
		return true;			
	}
	
	/**
	 * Compress a directory using a specified compression method.
	 * 
	 * @param localDir directory to compress.
	 * @param outDir output directory for compressed file
	 * @param logDir directory where log files are written.
	 * @param compression format to use.
	 * @return compressed filename, or null if compression failed.
	 */
	public String compressDirectory(String localDir, String outDir, String logDir, String compression) {
		if (OsPath.isdir(localDir) == false) {
			logger.warn("Not a directory on the local file system: " + localDir);
			return null;
		}
		
		
		String basename = OsPath.basename(localDir);
		String cmd = null;
		String compressedDir = null;		
		
		if (compression.equals("tar.gz")) {
			compressedDir = OsPath.join(outDir, basename + ".tar.gz");
			cmd = String.format("cd %s; tar cvzf %s . > %s 2> %s",
					localDir,
					compressedDir,					
					OsPath.join(logDir, "tar." + basename + ".output"),
					OsPath.join(logDir, "tar." + basename + ".error"));				
		}
		else if (compression.equals("tar.bz2")) {
			compressedDir = OsPath.join(outDir, basename + ".tar.bz2");
			cmd = String.format("cd %s; tar cvjf %s . > %s 2> %s",
					localDir,
					compressedDir,					
					OsPath.join(logDir, "tar." + basename + ".output"),
					OsPath.join(logDir, "tar." + basename + ".error"));               
		}  
		else if (compression.equals("tar")) {
			compressedDir = OsPath.join(outDir, basename + ".tar");
			cmd = String.format("cd %s; tar cvf %s . > %s 2> %s",
					localDir,
					compressedDir,
					OsPath.join(logDir, "tar." + basename + ".output"),
					OsPath.join(logDir, "tar." + basename + ".error"));               
		}  
		else {
			logger.fatal("Unknown compression format: " + compression);
			return null;
		}
		
		int rv = Stage.executeCmd(cmd, logger);
		if (rv != 0) {
			return null;
		}
		
		return compressedDir;
	}

	/**
	 * Uncompress a directory using an external program.
	 * 
	 * @param compressedDir compressed directory
	 * @param dstDir destination directory. Note that the files are uncompressed to this directory, and not to a sub-directory
	 * within it.
	 * @param logDir directory where logfiles are saved 
	 * @return list of uncompressed directory content, or null if the directory could not be decompressed.
	 */
	public ArrayList<String> uncompressDirectory(String compressedDir, String dstDir, String logDir) {
		String compression = getDirCompression(compressedDir);
		String basename = OsPath.basename(compressedDir);
		
		String cmd = null;
		if (compression.equals("tar.gz")) {
			cmd = String.format("tar xvzf %s -C %s > %s 2> %s",
					compressedDir,
					dstDir,
					OsPath.join(logDir, "untar." + basename + ".output"),
					OsPath.join(logDir, "untar." + basename + ".error"));
		}
		else if (compression.equals("tar.bz2")) {
			cmd = String.format("tar xvjf %s -C %s > %s 2> %s",
					compressedDir,
					dstDir,
					OsPath.join(logDir, "untar." + basename + ".output"),
					OsPath.join(logDir, "untar." + basename + ".error"));
		}
		else if (compression.equals("tar")) {
			cmd = String.format("tar xvf %s -C %s > %s 2> %s",
					compressedDir,
					dstDir,
					OsPath.join(logDir, "untar." + basename + ".output"),
					OsPath.join(logDir, "untar." + basename + ".error"));
		}		
		else {
			logger.warn("Unknown extension for compressed directory: " + basename);
			return null;
		}
		
		int rv = Stage.executeCmd(cmd, logger);
		if (rv != 0) {
			return null;
		}
				
		String[] files = OsPath.listdirR(dstDir, logger);
		
		ArrayList<String> dirContent = new ArrayList<String>();
		for (String f: files) {
			dirContent.add(f);
		}
		return dirContent;	
	}

	/**
	 * Check if file exists in HDFS
	 *
	 * @param filename filename to check
	 * @return true if directory exist
	 * @throws IOException
	 */
	public boolean isfile(String filename) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Check if directory exists in HDFS
	 *
	 * @param dirName directory to check
	 * @return true if directory exist
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	public boolean isdir(String dirName) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Mkdir that first checks if a directory exists. Otherwise it creates the directory.
	 *
	 * @param hdfs HDFS handle
	 * @param dirName directory to create
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	public void mkdir(String dirName) throws IOException {
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Rename a file. 
	 * 
	 * Note! this function overwrites the destination file if it already exists
	 * 
	 * @param srcName source name
	 * @param dstName destinaion name
	 * @return true if file was renamed. False otherwise.
	 * already exists
	 * @throws IOException 
	 */
	public boolean renameFile(String srcName, String dstName) throws IOException {
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Delete file in HDFS
	 * 
	 * @param filename file to delete
	 * @return true if file was deleted, false otherwise
	 * @throws IOException 
	 */
	public boolean deleteFile(String filename) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Delete a directory recursively in HDFS
	 * 
	 * @param directory to delete
	 * @return true if directory was deleted, false otherwise
	 * @throws IOException 
	 */
	public boolean deleteDir(String dirname) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Clenup a troilkatt directory by deleting files that are older than storageTime.
	 * The cleanup is recursive such that all subdirectories will also be cleaned.
	 * 
	 * @param hdfsOutputDir HDFS directory to clean
	 * @param timestamp current timestamp
	 * @param storageTime time to keep files in the directory. The time is given in days.
	 * If storageTime is -1, no files are deleted. If storageTime is 0 all files are 
	 * deleted.
	 * @throws IOException 
	 */
	public void cleanupDir(String hdfsOutputDir, long timestamp, int storageTime) throws IOException {
		if (storageTime <= -1) {
			logger.info("All files in directory are kept forever: " + hdfsOutputDir);
			return;
		}
		else if (storageTime == 0) {
			storageTime = Integer.MIN_VALUE;; // to ensure all files are deleted
		}
		
		ArrayList<String> files = listdirR(hdfsOutputDir);
		if (files == null) {
			logger.warn("Invalid directory: " + hdfsOutputDir);
			return;
		}
		
		int deleted = 0;
		for (String f: files) {
			long fileTimestamp = getFilenameTimestamp(f);
			if (fileTimestamp == -1) {
				logger.warn("Could not get timestamp for file: " + f);
				continue;
			}
			
			long fileAge = (timestamp - fileTimestamp) / (1000 * 60 * 60 * 24); // in days
			if (fileAge < 0) {
				logger.warn("Invalid file age: " + fileAge);
				continue;
			}
			
			if (fileAge > storageTime) {
				if (deleteFile(f) == false) {
					logger.warn("Could not delete file: " + f );
				}
				else {
					deleted++;
				}
			}
		}
		if (deleted > 0) {
			logger.info("Deleted " + deleted + " of " + files.size() + " files");
		}
	}
	
	/**
	 * Clenup a troilkatt meta directory by deleting files that are older than storageTime.
	 * The newest file will not be deleted even if it is older than storageTime.
	 * 
	 * @param hdfsMetaDir HDFS meta directory to clean
	 * @param timestamp current timestamp
	 * @param storageTime time to keep files in the directory. The time is given in days.
	 * If storageTime is -1, no files are deleted. If storageTime is 0 all files are 
	 * deleted.
	 * @throws IOException 
	 */
	public void cleanupMetaDir(String hdfsMetaDir, long timestamp, int storageTime) throws IOException {
		if (storageTime <= -1) {
			logger.info("All files in directory are kept forever: " + hdfsMetaDir);
			return;
		}
		else if (storageTime == 0) {
			storageTime = Integer.MIN_VALUE; // to ensure all files are deleted
		}
		
		String newestDir = getNewestDir(hdfsMetaDir);
		if (newestDir == null) {
			logger.warn("No directory archive found: " + hdfsMetaDir);
			return;
		}
		
		ArrayList<String> files = listdir(hdfsMetaDir);
		if (files == null) {
			logger.warn("Invalid directory: " + hdfsMetaDir);
			return;
		}
		int deleted = 0;
		for (String f: files) {
			String basename = OsPath.basename(f);
			if (basename.equals(newestDir)) {
				// Do not delete newest dir
				continue;
			}
			
			long fileTimestamp = getDirTimestamp(f);
			if (fileTimestamp == -1) {
				logger.warn("Could not get timestamp for file: " + f);
				continue;
			}
			
			long fileAge = (timestamp - fileTimestamp) / (1000 * 60 * 60 * 24); // in days
			if (fileAge < 0) {
				logger.warn("Invalid file age: " + fileAge);
				continue;
			}
			
			if (fileAge > storageTime) {
				if (deleteFile(f) == false) {
					logger.warn("Could not delete file: " + f );
				}
				else {
					deleted++;
				}
			}
		}
		if (deleted > 0) {
			logger.info("Deleted " + deleted + " of " + files.size() + " meta dirs");
		}
	}

	/**
	 * Remove timestamp and compression format.
	 * 
	 * @param hdfsName filename with timestamp and compression format
	 * @return basename with timestamp and compression format removed, or null if the
	 * filename is invalid.
	 */
	public String getFilenameName(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
		
		if (parts.length > 2) {
			String name = parts[0];
			for (int i = 1; i < parts.length - 2; i++) {
				name = name + "." + parts[i];
			}
			
			return name;
		}
		else {
			logger.fatal("Invalid tfs filename: " + hdfsName);
			return null;
		}		
	}
	
	/**
	 * Get directory from path.
	 * 
	 * @param hdfsName filename with timestamp and compression format
	 * @return directory part of the filename (parent).
	 */
	public String getFilenameDir(String hdfsName) {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Get timestamp from filename
	 * 
	 * @param hdfsName filename with timestamp and compression format.
	 * @return timestamp, or -1 if the filename is invalid.
	 */
	public long getFilenameTimestamp(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
		if (parts.length < 3) {
			logger.warn("Invalid tfs filename: " + hdfsName);
			return -1;
		}
		
		try {
			return Long.valueOf(parts[parts.length - 2]);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid tfs filename: " + hdfsName);
			return -1;
		}
	}
	
	/**
	 * Get compression format from filename
	 * 
	 * @param hdfsName filename with timestamp and compression format.
	 * @return compression format (gz, bz2, etc), or null if the filename is invalid.
	 */
	public String getFilenameCompression(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
	
		if (parts.length >= 3) {
			return parts[parts.length - 1];
		}
		else {
			logger.fatal("Invalid tfs filename: " + hdfsName);
			return null;
		}
	}
	
	/**
	 * Get timestamp from directory name
	 * 
	 * @param hdfsName filename with timestamp and compression format (timestamp.compression or timestamp.tar.compression).
	 * @return timestamp, or -1 if the filename is invalid.
	 */
	public long getDirTimestamp(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
		
		if (parts.length < 2) {
			logger.fatal("Invalid tfs directory: " + hdfsName);
			return -1;		
		}
		
		String timestampString = parts[parts.length - 2];
		if (timestampString.equals("tar")) {
			if (parts.length >= 3) {
				timestampString = parts[ parts.length - 3 ];
			}
			else {
				logger.fatal("Invalid compression format for tfs directory: " + hdfsName);
				return -1;
			}
		}		
		
		try {
			return Long.valueOf(timestampString);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid tfs directory: " + hdfsName);
			return -1;
		}
	}
	
	/**
	 * Get compression format from directory
	 * 
	 * @param hdfsName filename with timestamp and compression format.
	 * @return compression format, or null if the filename is invalid.
	 */
	public String getDirCompression(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");		
		
		if (parts.length >= 2) {
			if (parts[parts.length - 2].equals("tar")) {
				return "tar." + parts[parts.length - 1];
			}
			else {
				return parts[parts.length - 1];
			}
		}
		else {
			logger.fatal("Invalid hdfs filename: " + hdfsName);
			return null;
		}
	}
	
	/**
	 * Check if compression format is supported by Troilkatt
	 * 
	 * @param compression format to check.
	 * @return true if supported.
	 */
	public static boolean isValidCompression(String compression) {
		if (compression == null) {
			return false;
		}
		
		for (String s: compressionExtensions) {
			if (compression.equals(s)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * @return list of supported compression formats.
	 */
	public static String[] getValidCompression() {
		return compressionExtensions;
	}
	
	/**
	 * Get filesize
	 * 
	 * @param filename
	 * @return size in bytes, or -1 if an invalid filename
	 * @throws IOException 
	 */
	public long fileSize(String filename) throws IOException {
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Copy a status file from HDFS to local FS
	 * 
	 * @param hdfsFilename Source HDFS filename
	 * @param localFilename Destination local FS filename
	 * @throws IOException
	 * @throws TroilkattPropertiesException if invalid hdfsFilename
	 */	
	public void getStatusFile(String hdfsFilename, String localFilename) throws IOException, TroilkattPropertiesException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Copy a status file from local FS to HDFS
	 * 
	 * @param localFilename Source local FS filename
	 * @param hdfsFilename Destination HDFS filename
	 * 
	 * @throws IOException if file could not be copeid to HDFS
	 * @throws TroilkattPropertiesException 
	 */
	public void saveStatusFile(String localFilename, String hdfsFilename) throws IOException, TroilkattPropertiesException {		
		throw new RuntimeException("Method not implemented");
	}
}
