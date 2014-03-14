package edu.princeton.function.troilkatt.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.security.AccessControlException;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.utils.Utils;

import org.diffdb.fswrapper;

/**
 * Troilkatt wrapper for HDFS
 * 
 * Note the code is written for version 0.20.X. Some minor changes are required for 0.21.X.
 */
public class TroilkattGS extends TroilkattFS {
	public FileSystem hdfs;
	protected Configuration conf;
        public fswrapper runner;
	public Logger logger;
	
	/**
	 * Constructor.
	 * 
	 * @throws IOException 
	 */
	public TroilkattGS(Configuration cf) throws IOException {
		logger = Logger.getLogger("troilkatt.gs");
		hdfs = FileSystem.get(new Configuration());
                runner = new fswrapper("troilkatt");
	}
	
	/**
	 * Alternative constructor to be called from MapReduce jobs.
	 * 
	 * @param hdfs HDFS handle
	 * @throws IOException 
	 */
	public TroilkattGS(FileSystem hdfs) throws IOException {
		logger = Logger.getLogger("troilkatt.gs");
                runner = new fswrapper("troilkatt");
		this.hdfs = hdfs;
	}
	
	/**
	 * Do a (non-recursive) directory listing.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	@Override
	public ArrayList<String> listdir(String hdfsDir) throws IOException {
		ArrayList<String> returns = runner.ls(hdfsDir);
		logger.info("listDir: " + hdfsDir + " Length: " + Integer.toString(returns.size()));
		return returns;
	}
	
	/**
	 * Do a recursive listing of files.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException 
	 */
	@Override
	public ArrayList<String> listdirR(String hdfsDir) throws IOException {
		logger.info("listDirR dir:" + hdfsDir);
		return runner.lsRec(hdfsDir);
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
	@Override
	public String getFile(String hdfsName, String localDir, String tmpDir, String logDir) throws IOException {
		logger.info("getfile hdfsName:" + hdfsName);
		// Do GeStore Get
		ArrayList<String> files = runner.getFile(hdfsName);
                for(String file : files) {
                    //Copy to local dir
                    FileSystem fs = runner.getFS();
                    fs.copyToLocalFile(false, new Path(file), new Path(localDir));
                }
		return localDir;
	}

	/**
	 * Copy all files in a HDFS directory archive to a local FS directory, uncompress the file,
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
	@Override
	public ArrayList<String> getDirFiles(String hdfsName, String localDir, String logDir, String tmpDir) throws IOException {	
		logger.info("getdirfiles hdfsName: " + hdfsName);
		//Do multiple gestore gets
		// Does not work!
		return runner.getFile(hdfsName);
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
	@Override
	public String putLocalFile(String localFilename, String hdfsOutputDir, String tmpDir, String logDir, 
			String compression, long timestamp) {
		logger.info("putlocalfile " + localFilename + " hdfsDir: " + hdfsOutputDir);
		// Do GeStore put
		runner.putFile(localFilename, hdfsOutputDir + localFilename + "." + Long.toString(timestamp) + ".none");
		return hdfsOutputDir + localFilename;
	}
	
	/**
	 * Compress a file and move the file from local FS to HDFS. (Note no timestamp is added to the 
	 * filename).
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to HDFS and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.  The filename should include a timestamp.
	 * @param hdfsOutputDir HDFS output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return HDFS filename on success, null otherwise
	 */
	@Override
	public String putLocalFile(String localFilename, String hdfsOutputDir, String tmpDir, String logDir, String compression) {
		logger.info("putlocalfile2 " + localFilename);
		runner.putFile(localFilename, hdfsOutputDir + localFilename + ".none");
		// Do GeStore Put (no timestamp)
		return "";
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
	@Override
	public boolean putLocalDirFiles(String hdfsDir, long timestamp, ArrayList<String> localFiles, 
			String compression, String logDir, String tmpDir) {		
		logger.info("putlocaldirfiles hdfsDir: " + hdfsDir);
		// Do GeStore put (multiple files)
		for(String file : localFiles) {
			String [] fileParts = file.split("/");
			String addendum = "." + Long.toString(timestamp) + ".none";
			File oldFile = new File(file);
			File newFile = new File(file + addendum);
			if(newFile.exists()) {
				logger.warn("File exists, using old file!");
			}
			if(!oldFile.renameTo(newFile)) {
				logger.warn("Unable to rename file!");
				return false;
			}
			runner.putFile(file + addendum, hdfsDir + "/" + fileParts[fileParts.length-1] + addendum);
			logger.info("File to put into GeStore: " + file);
			logger.info("GeStore target: " + hdfsDir + fileParts[fileParts.length-1] + addendum);
                }
		return true;
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
	@Override
	public String putTFSFile(String srcFilename, String dstDir,
			String tmpDir, String logDir, 
			String compression, long timestamp) throws IOException {
		logger.info(String.format("Move file %s to %s using compression %s and timestamp %d\n", srcFilename, dstDir, compression, timestamp));
		// Not sure what's happening here...
		return "";
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
	@Override
	public String putTFSFile(String srcFilename, String dstDir) throws IOException { 
		logger.info("putTFSfile");
		// Again, not sure
		return "";					
	}



	/**
	 * Check if file exists in HDFS
	 *
	 * @param filename filename to check
	 * @return true if file exist
	 * @throws IOException
	 */
	@Override
	public boolean isfile(String filename) throws IOException {
		logger.info("isfile");
		//Check if something is in GeStore
		return true;
	}
	
	/**
	 * Check if directory exists in HDFS
	 *
	 * @param dirName directory to check
	 * @return true if directory exist
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	@Override
	public boolean isdir(String dirName) throws IOException {
		logger.info("isdir");
                // Always return true? How to handle dirs...
		return true;
	}
	
	/**
	 * Mkdir that first checks if a directory exists. If the directory does not exist it is created.
	 * Otherwise the method returns. 
	 *
	 * @param hdfs HDFS handle
	 * @param dirName directory to create
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
	@Override
	public void mkdir(String dirName) throws IOException {
		logger.info("mkdir");
		//Always succeed?
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
		logger.info("renamefile");
		// Create a new reference to the file I guess?
		// New GS functionality needed
		return true;
	}

	/**
	 * Delete file in HDFS
	 * 
	 * @param filename file to delete
	 * @return true if file was deleted, false otherwise
	 * @throws IOException 
	 */
	@Override
	public boolean deleteFile(String filename) throws IOException {
		logger.info("deletefile");
		// Delete a reference, new GS functionality needed
		return true;
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
		logger.info("deleteDir");
		// Delete dir, new functionality needed
		return true;
	}
	
	/**
	 * Get directory from path.
	 * 
	 * @param hdfsName filename with timestamp and compression format
	 * @return directory part of the filename (parent).
	 */
	@Override
	public String getFilenameDir(String hdfsName) {
		logger.info("getfilenamedir");
		// I have no idea what to do here... Strip the filename extension maybe?
		return "";
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
		logger.info("filesize");
		// Get the filename in HDFS, check with HDFS
		return 1;
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
		logger.info("getstatusfile");
		Path hdfsPath = new Path(hdfsFilename);
		Path localPath = new Path(localFilename);
		
		/*
		 * Verify, download, or create status file
		 */
		if (! OsPath.isfile(localFilename)) {
			System.out.println("Status file not found: " + localFilename);
			boolean hdfsFileDownloaded = false;
			
			try {
				if (hdfs.isFile(hdfsPath)) {
					if (Utils.getYesOrNo("Download from HDFS?", true)) {
						logger.debug(String.format("Copy HDFS file %s to local file %s\n", hdfsFilename, localFilename));
						hdfs.copyToLocalFile(hdfsPath, localPath);
						hdfsFileDownloaded = true;
					}
				}
				else {
					logger.info("Status file not in local FS nor HDFS");
				}
			} catch (IOException e1) {
				logger.fatal("Could not copy status file from HDFS to local FS", e1);
				throw e1;
			}									
			
			if (! hdfsFileDownloaded) {
				System.out.println("Creating new status file");
				logger.info("Create new status file");
				File nf = new File(localFilename);
				try {
					if (nf.createNewFile() == false) {
						throw new RuntimeException("Status file already exists");						
					}
				} catch (IOException e) {
					logger.fatal("Could not create new status file: ", e);
					throw e;
				}	
				// Attempt to save new status file to verify that the HDFS path is valid
				try {
					saveStatusFile(localFilename, hdfsFilename);
				} catch (IOException e) {
					logger.fatal("Could not save status file", e);
					throw new TroilkattPropertiesException("Invalid HDFS status filename: " + hdfsFilename);
				}
			}
		}
		else { // file in local FS
			//TODO: verify that the files on local and HDFS match
			
			// TODO: verify that the status file is not corrupt
		}
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
		logger.info("savestatusfile");
		Path hdfsPath = new Path(hdfsFilename);
		Path localPath = new Path(localFilename);
		
		logger.info(String.format("Copy status file %s to HDFS file %s\n", localFilename, hdfsFilename));
		if (hdfs.isFile(hdfsPath)) {
			logger.debug("Deleting older version of status file");
			hdfs.delete(hdfsPath, false);
		}
	
		/*
		 * Fix bug in hadoop 0.20.X that causes the copyFromLocalFile to fail if there exists a .crc file
		 * that was created before the status file was modified.
		 */
		String checksumFilename = OsPath.join(OsPath.dirname(localFilename), "." + OsPath.basename(localFilename) + ".crc");
		if (OsPath.isfile(checksumFilename)) {
			logger.warn("Deleting stale checksum file: " + checksumFilename);
			OsPath.delete(checksumFilename);
		}
		
		hdfs.copyFromLocalFile(false, true, localPath, hdfsPath); // keep local & overwrite
	}

	/**
	 * Do a recursive listing of files.
	 * 
	 * @param hdfsDir directory to list
	 * @param filePaths list where file-paths are stored.
	 * @return none, but filePaths is updated with directory content
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private void getFilelistRecursive(String hdfsDir, ArrayList<Path> filePaths) throws FileNotFoundException, IOException {
		logger.info("getFilelistRecursive");
		FileStatus[] files = null;
					
		files = hdfs.listStatus(new Path(hdfsDir));			
		if (files == null) {
			logger.fatal("No files in directory");
			return;
		}
	
		
		for (FileStatus fs: files) {
			Path p = fs.getPath();
			if (fs.isDirectory()) {			
				getFilelistRecursive(path2filename(p), filePaths);
			}
			else if (p.getName().startsWith(".")) {
				logger.info("Ignoring hidden file: " + path2filename(p));
			}
			else {				
				filePaths.add(p);
			}
		}
	}

	/**
	 * Copy a file from local FS to HDFS and take care of exception and return value
	 * checking.
	 * 
	 * @param localName file to copy
	 * @param hdfsName destination name
	 * @return true on success, false otherwise
	 */
	private boolean copyFileToHdfs(String localName, String hdfsName) {
		logger.info("copyfiletohdfs");
		Path localPath = new Path(localName);
		
		// Make sure there is no old .crc file in the folder (HDFS bug/feature)
		OsPath.delete(localName.replace(OsPath.basename(localName), "." + OsPath.basename(localName) + ".crc"));
		
		Path hdfsPath = new Path(hdfsName);	
		try {
			logger.debug(String.format("Copy local file %s to HDFS file %s\n", localName, hdfsName));
			if (hdfs.isFile(hdfsPath)) {
				logger.info("File already exist: deleting older version");
				hdfs.delete(hdfsPath, false);
			}
			hdfs.copyFromLocalFile(false, true, localPath, hdfsPath); // keep local & overwrite
			
			if (! hdfs.exists(hdfsPath)) {
				logger.fatal("Could not upload file: " + localName);
				throw new IOException("HDFS copy failed");
			}	
		} catch (IOException e1) {
			logger.fatal("Could not copy from local FS to HDFS: ", e1);
			logger.fatal("Local filename: " + localName);
			logger.fatal("HDFS filename: " + hdfsName);
			return false;				
		}
		
		return true;
	}
	
	/**
	 * Helper function to copy a file from HDFS to local FS and take care of exception and 
	 * return value checking.
	 * 
	 * @param hdfsName source filename (on HDFS)
	 * @param localName destination filename (on local FS)
	 * @return true on success, false otherwise
	 */
	private boolean copyFileFromHdfs(String hdfsName, String localName) {
		logger.info("copyfilefromhdfs");
		Path localPath = new Path(localName);
		
		// Make sure there is no old .crc file in the folder (HDFS bug/feature)
		OsPath.delete(localName.replace(OsPath.basename(localName), "." + OsPath.basename(localName) + ".crc"));
		
		Path hdfsPath = new Path(hdfsName);	
		try {
			logger.debug(String.format("Copy HDFS file %s to local file %s\n", hdfsName, localName));			
			hdfs.copyToLocalFile(hdfsPath, localPath);			
		} catch (IOException e1) {
			logger.fatal("Could not copy from HDFS to local FS: ", e1);
			logger.fatal("Local filename: " + localName);
			logger.fatal("HDFS filename: " + hdfsName);
			return false;			
		}
		
		return true;
	}
	
	/**
	 * Helper function to move a HDFS file.
	 * 
	 * @param srcFilename source filename
	 * @param dstFilename destination filename
	 * @return true if the file was succefully moved
	 * @throws IOException
	 */
	private boolean moveHDFSFile(String srcFilename, String dstFilename) throws IOException {
		logger.info("movehdfsfile");
		if (isfile(dstFilename)) {
			logger.warn("Deleting existing destination file: " + dstFilename);
			if (deleteFile(dstFilename) == false) {
				logger.fatal("Could not delete existing destination file: " + dstFilename);
				return false;
			}
		}
		
		Path srcPath = new Path(srcFilename);
		Path dstPath = new Path(dstFilename);
		return hdfs.rename(srcPath, dstPath);		
	}

	/**
	 * Helper function to convert a hadoop Path object to a String absolute filename
	 */
	private String path2filename(Path p) {
		logger.info("path3filename");
		URI uri = p.toUri();		
		return uri.getPath();		
	}
}
