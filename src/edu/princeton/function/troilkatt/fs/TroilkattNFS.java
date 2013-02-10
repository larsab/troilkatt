package edu.princeton.function.troilkatt.fs;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.utils.Utils;

/**
 * Troilkatt wrapper for HDFS
 * 
 * Note the code is written for version 0.20.X. Some minor changes are required for 0.21.X.
 */
public class TroilkattNFS extends TroilkattFS {
	
	/**
	 * Constructor.
	 * 
	 */
	public TroilkattNFS() {
		logger = Logger.getLogger("troilkatt.nfs");
	}
	
	/**
	 * Do a (non-recursive) directory listing.
	 * 
	 * @param nfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	@Override
	public ArrayList<String> listdir(String nfsDir) throws IOException {
		if (! OsPath.isdir(nfsDir)) {
			logger.warn("Not a directory: " + nfsDir);
			return null;
		}
		
		String[] files = OsPath.listdir(nfsDir, logger);
		return Utils.array2list(files);	
	}
	
	/**
	 * Do a recursive listing of files.
	 * 
	 * @param nfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException 
	 */
	@Override
	public ArrayList<String> listdirR(String nfsDir) throws IOException {
		if (! OsPath.isdir(nfsDir)) {
			logger.warn("Not a directory: " + nfsDir);
			return null;
		}
		
		String[] files = OsPath.listdirR(nfsDir, logger);
		return Utils.array2list(files);	
	}
	
	/**
	 * Copy file from NFS to the local FS directory, uncompress the file,
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
	@Override
	public String getFile(String nfsName, String localDir, String tmpDir, String logDir) throws IOException {		
		if (! isfile(nfsName)) {
			logger.error("Not a file: " + nfsName);
			return null;
		}
		
		String compression = getFilenameCompression(nfsName);
		if (compression == null) {
			logger.fatal("Invalid compression extension: " + nfsName);
			return null;
		}
		
		long timestamp = getFilenameTimestamp(nfsName);
		if (timestamp == -1) {
			logger.fatal("Invalid timestamp: " + nfsName);
			return null;
		}		
		
		String basename = getFilenameName(nfsName);		
		if (basename == null) {
			logger.fatal("Invalid filename: " + nfsName);
			return null;
		}
		
		if (! OsPath.isdir(localDir)) {
			logger.fatal("Invalid directory: " + localDir);
			return null;
		}
		
		String finalName = OsPath.join(localDir, basename);
		
		if (uncompressFile(nfsName, finalName, logDir)) { // success
			return finalName;
		} 
		else {
			return null;
		}
	}

	/**
	 * Copy all files in a NFS directory to a local FS directory. The directory content may
	 * be packed and compressed in which case the file is uncompressed and unpacked. Also,
	 * remove the timestamps and compression extension from all files.
	 * 
	 * @param nfsName HDFS directory with files to download
	 * @param localDir directory where files are written to
	 * @param logDir directory for logfiles
	 * @param tmpDir directory where temporal files are stored during uncompression
	 * @return list of filenames on local FS, or null if the directory name is invalid or one or more files could
	 * not be downloaded
	 * @throws IOException 
	 */
	@Override
	public ArrayList<String> getDirFiles(String nfsName, String localDir, String logDir, String tmpDir) throws IOException {				
		ArrayList<String> localFiles = null;
		String compression = getDirCompression(nfsName);
		if (compression == null) {
			logger.fatal("Invalid compression format: " + nfsName);
			return null;
		}
		
		if (! OsPath.isdir(localDir)) {
			logger.debug("Creating directory on local FS: " + localDir);
			if (OsPath.mkdir(localDir) == false) {
				logger.fatal("Could not create directory on local FS: " + localDir);
				throw new IOException("Could not create directory on local FS: " + localDir);
			}
		}
		
		localFiles = new ArrayList<String>();		
		if (compression.equals("none")) { 
			// Directory is not compressed or packed, so just copy all files
			ArrayList<String> nfsFiles = listdirR(nfsName);
			if (nfsFiles == null) {
				logger.warn("Empty directory, or directory could not be listed: " + nfsName);
				return null;
			}
						
			for (String f: nfsFiles) {
				String localName = OsPath.join(localDir, OsPath.basename(f));
				if (OsPath.copy(f, localName) == false) {
					logger.warn("Could not copy file: " + f);
					return null;
				}
				
				localFiles.add(localName);				
			}
		}
		else {	// Directory is compressed or packed
			localFiles = uncompressDirectory(nfsName, localDir, logDir);					
		}

		return localFiles;
	}

	/**
	 * Compress a file, add a timestamp, and move the file from local FS to HDFS.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to NFS and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.
	 * @param nfsOutputDir NFS output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return NFS filename on success, null otherwise
	 */
	@Override
	public String putLocalFile(String localFilename, String nfsOutputDir, String tmpDir, String logDir, 
			String compression, long timestamp) {
		
		logger.debug("Put local file " + localFilename + " to HDFS dir " + nfsOutputDir + " with timestamp " + timestamp + " using compression " + compression);						
		
		/*
		 * Check input file and output directory
		 */
		try {
			if (! isfile(localFilename)) {
				logger.error("Not a file: " + localFilename);
				return null;
			}
			if (fileSize(localFilename) == 0) {
				logger.warn("Empty file ignored: " + localFilename);
				return null;
			}
			
			if (isfile(nfsOutputDir)) {
				logger.fatal("Destiantion is a file and not a directory: " + nfsOutputDir);
			}
			else if (! isdir(nfsOutputDir)) {
				logger.warn("Creating destination directory: " + nfsOutputDir);
				mkdir(nfsOutputDir);
			}
			if (! isValidCompression(compression)) {
				logger.fatal("Invalid compression format: " + compression);
				return null;
			}
		} catch (IOException e1) {
			logger.fatal("Could not check if argument is a file: " + e1);
			return null; 
		}
		
		/*
		 * Check arguments
		 */
		if (timestamp < 0) {
			logger.fatal("Invalid timestamp: " + timestamp);
			return null;
		}
		String timestampFilename = OsPath.join(tmpDir, OsPath.basename(localFilename) + "." + timestamp);
		
		if (OsPath.rename(localFilename, timestampFilename) == false) {
			logger.fatal("Could not rename file: " + localFilename + " to " + timestampFilename);
			return null;
		} 
		
		// This function does the actual compression
		return compressFile(timestampFilename, nfsOutputDir, logDir, compression);
	}
	
	/**
	 * Compress a file and move the file from local FS to NFS. Note that this function does not 
	 * add a timestamp to the filename.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to HDFS and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS. The filename should include a timestamp.
	 * @param nfsOutputDir NFS output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return HDFS filename on success, null otherwise
	 */
	@Override
	public String putLocalFile(String localFilename, String nfsOutputDir, String tmpDir, String logDir, String compression) {
		/*
		 * Check input file and output directory
		 */
		try {
			if (! isfile(localFilename)) {
				logger.error("Not a file: " + localFilename);
				return null;
			}
			if (fileSize(localFilename) == 0) {
				logger.warn("Empty file ignored: " + localFilename);
				return null;
			}
			
			if (isfile(nfsOutputDir)) {
				logger.fatal("Destiantion is a file and not a directory: " + nfsOutputDir);
			}
			else if (! isdir(nfsOutputDir)) {
				logger.warn("Creating destination directory: " + nfsOutputDir);
				mkdir(nfsOutputDir);
			}			
		} catch (IOException e1) {
			logger.fatal("Could not check if argument is a file: " + e1);
			return null; 
		}
		if (! isValidCompression(compression)) {
			logger.fatal("Invalid compression format: " + compression);
			return null;
		}		
		
		return compressFile(localFilename, nfsOutputDir, logDir, compression);
	}
	

	/**
	 * Create a directory using the timestamp as name, compress  the directory files, and copy the 
	 * compressed directory from local FS to HDFS 
	 * 
	 * @param nfsDir HDFS directory where new sub-directory is created
	 * @param timestamp name for new sub-directory
	 * @param metaFiles files to copy to HDFS
	 * @param localFiles list of absolute filenames in directory to copy to HDFS
	 * @param compression method to use for directory 
	 * @param logDir directory where logfiles are stored
	 * @param tmpDir directory on local FS where compressed directory is stored
	 * @return true if all files were copied to HDFS, false otherwise
	 */
	@Override
	public boolean putLocalDirFiles(String nfsDir, long timestamp, ArrayList<String> localFiles, 
			String compression, String logDir, String tmpDir) {				
		if (localFiles.isEmpty()) {
			logger.warn("No files to put");
			return false;
		}
		
		try {
			if (! isdir(nfsDir)) {
				logger.fatal("Invalid NFS output directory: " + nfsDir);
				return false;
			}
		} catch (IOException e) {
			logger.fatal("Could not check NFS directory: " + nfsDir);
			return false;
		}
		if (timestamp < 0) {
			logger.fatal("Invalid timestamp: " + timestamp);
			return false;
		}
		
		if (compression.equals("none")) {
			String subdir = OsPath.join(nfsDir, timestamp + "." + compression);
			for (String f: localFiles) {
				String name = OsPath.join(subdir, OsPath.basename(f));
				String dir = OsPath.dirname(name);
				
				if (! OsPath.isdir(dir)) {
					if (! OsPath.mkdir(dir)) {
						return false;
					}
				}
				if (! OsPath.copy(f, name)) {
					return false;
				}				
			}	
		}
		else {
			// First make a temporary directory
			String tmpSubdir = OsPath.join(tmpDir, String.valueOf(timestamp));
			if (! OsPath.mkdir(tmpSubdir)) {
				logger.fatal("Could not make tmp directory: " + tmpDir);
			}
			
			System.out.println("copy files to " + tmpSubdir);
			for (String f: localFiles) {
				String tmpFilename = OsPath.join(tmpSubdir, OsPath.basename(f));				
				if (! OsPath.copy(f, tmpFilename)) {
					logger.fatal("Could not copy file to tmp directory: " + f);
					return false;
				}	
			}			
			
			// Compress the tmp directory
			String compressedDir = compressDirectory(tmpSubdir, 
					OsPath.join(nfsDir, String.valueOf(timestamp)), 
					logDir, compression); 
			if (compressedDir == null) {
				logger.fatal("Could not compress directory: " + tmpSubdir);						
				return false;
			}			
		}
		return true;
	}

	/**
	 * Move a file from a temporary directory to an output directory. A timestamp
	 * will be added to the filename and the file will be compressed if necessary.
	 * The source file will be deleted.
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
	@Override
	public String putHDFSFile(String srcFilename, String dstDir,
			String tmpDir, String logDir, 
			String compression, long timestamp) throws IOException {
		
		logger.info(String.format("Move file %s to %s using compression %s and timestamp %d\n", srcFilename, dstDir, compression, timestamp));
		
		// There is no difference in local and NFS files
		String dstFilename = putLocalFile(srcFilename, dstDir, tmpDir, logDir, compression, timestamp);
		if (dstFilename == null) {
			if (putLocalFile(srcFilename, dstDir, tmpDir, logDir, compression, timestamp) == null) {
				logger.error("Could not put NFS file");
				return null;
			}
		}
		
		// Delete source file
		if(! deleteFile(srcFilename)) {
			logger.warn("Could not delete source file: " + srcFilename);
		}			
		return dstFilename;		
	}
	
	/**
	 * Move a file from a temporary directory to an output directory. The source
	 * filename is assumed to have a valid timestamp and compression format. The
	 * source file is deleted.
	 * 
	 * @param srcFilename source filename
	 * @param dstDir destination directory
	 * @return destination filename, or null if the file could not be moved
	 * @throws IOException 
	 */
	@Override
	public String putHDFSFile(String srcFilename, String dstDir) throws IOException { 
			
		if (! isfile(srcFilename)) {
			logger.fatal("Source is not a file: " + srcFilename);
			return null;
		}
		
		if (getFilenameTimestamp(srcFilename) < 0) {
			logger.fatal("Invalid timestamp in source file: " + srcFilename);
			return null;
		}
		
		String compression = getFilenameCompression(srcFilename);
		if ((compression == null ) || (! isValidCompression(compression))) {
			logger.fatal("Invalid compression format in source file: " + srcFilename);
			return null;
		}
		
		if (isfile(dstDir)) {
			logger.fatal("Destiantion is a file and not a directory: " + dstDir);
		}
		else if (! isdir(dstDir)) {
			logger.warn("Creating destination directory: " + dstDir);
			mkdir(dstDir);
		}
				
		String dstFilename = OsPath.join(dstDir, OsPath.basename(srcFilename));					
		if (OsPath.rename(srcFilename, dstFilename) == false) {
			logger.fatal("Could not move " + srcFilename + " to " + dstFilename);
			return null;
		}
		
		return dstFilename;					
	}

	
	/**
	 * Check if file exists in NFS
	 *
	 * @param filename filename to check
	 * @return true if file exist
	 * @throws IOException
	 */
	@Override
	public boolean isfile(String filename) throws IOException {
		return OsPath.isfile(filename);
	}
	
	/**
	 * Check if directory exists in NFS
	 *
	 * @param dirName directory to check
	 * @return true if directory exist
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	@Override
	public boolean isdir(String dirName) throws IOException {
		return OsPath.isdir(dirName);
	}
	
	/**
	 * Mkdir that first checks if a directory exists. Otherwise it creates the directory.
	 *
	 * @param hdfs HDFS handle
	 * @param dirName directory to create
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	@Override
	public void mkdir(String dirName) throws IOException {
		if (OsPath.isdir(dirName)) { // directory is already created
			return;
		}
		
		if (! OsPath.mkdir(dirName)) {
			throw new IOException("NFS mkdir " + dirName + " failed");
		}
	}

	/**
	 * Rename a file. 
	 * 
	 * Note! this function overwrites the destination file if it already exists
	 * 
	 * @param srcName source name
	 * @param dstName destinaion name
	 * @return true if file was renamed. False otherwise.
	 * @throws IOException 
	 */
	@Override
	public boolean renameFile(String srcName, String dstName) throws IOException {
		return OsPath.rename(srcName, dstName);
	}

	/**
	 * Delete file in NFS
	 * 
	 * @param filename file to delete
	 * @return true if file was deleted, false otherwise
	 * @throws IOException 
	 */
	@Override
	public boolean deleteFile(String filename) throws IOException {
		return OsPath.delete(filename);
	}
	
	/**
	 * Delete a directory recursively in HDFS
	 * 
	 * @param directory to delete
	 * @return true if directory was deleted, false otherwise
	 * @throws IOException 
	 */
	@Override
	public boolean deleteDir(String dirname) throws IOException {
		if (isdir(dirname)) {
			return OsPath.deleteAll(dirname);
		}
		else {
			// Not a directory
			return false;
		}
	}
	
	/**
	 * Get directory from path.
	 * 
	 * @param nfsName filename with timestamp and compression format
	 * @return directory part of the filename (parent).
	 */
	@Override
	public String getFilenameDir(String nfsName) {
		return OsPath.dirname(nfsName);			
	}
	
	/**
	 * Get filesize
	 * 
	 * @param filename
	 * @return size in bytes, or -1 if an invalid filename
	 * @throws IOException 
	 */
	@Override
	public long fileSize(String filename) throws IOException {
		if (! isfile(filename)) {
			return -1;
		}
		
		return OsPath.fileSize(filename);
	}		
}
