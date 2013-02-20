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

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.utils.Utils;

/**
 * Troilkatt wrapper for HDFS
 * 
 * Note the code is written for version 0.20.X. Some minor changes are required for 0.21.X.
 */
public class TroilkattHDFS extends TroilkattFS {
	public FileSystem hdfs;
	protected Configuration conf;
	
	/**
	 * Constructor.
	 * 
	 * @throws IOException 
	 */
	public TroilkattHDFS() throws IOException {
		conf = new Configuration();		
		hdfs = FileSystem.get(conf);		
		logger = Logger.getLogger("troilkatt.hdfs");
	}
	
	/**
	 * Alternative constructor to be called from MapReduce jobs.
	 * 
	 * @param hdfs HDFS handle
	 * @throws IOException 
	 */
	public TroilkattHDFS(FileSystem hdfs) throws IOException {
		conf = hdfs.getConf();
		this.hdfs = hdfs;
		logger = Logger.getLogger("troilkatt.hdfs");
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
		if (! isdir(hdfsDir)) {
			logger.warn("Not a directory: " + hdfsDir);
			return null;
		}
		
		ArrayList<String> filenames = new ArrayList<String>();
		
		FileStatus[] files = null;					
		files = hdfs.listStatus(new Path(hdfsDir));			
		if (files == null) {
			logger.warn("No files in directory");		
		}
		else {		
			for (FileStatus fs: files) {
				filenames.add(path2filename(fs.getPath()));			
			}
		}
		
		return filenames;
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
		if (! isdir(hdfsDir)) {
			logger.warn("Not a directory: " + hdfsDir);
			return null;
		}
		
		ArrayList<String> filenames = new ArrayList<String>();
		ArrayList<Path> filePaths = new ArrayList<Path>();
		
		getFilelistRecursive(hdfsDir, filePaths);
		for (Path p: filePaths) {			
			filenames.add(path2filename(p));
		}
		
		return filenames;
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
		
		String compression = getFilenameCompression(hdfsName);
		if (compression == null) {
			logger.fatal("Invalid compression extension: " + hdfsName);
			return null;
		}
		
		long timestamp = getFilenameTimestamp(hdfsName);
		if (timestamp == -1) {
			logger.fatal("Invalid timestamp: " + hdfsName);
			return null;
		}		
		
		String basename = getFilenameName(hdfsName);		
		if (basename == null) {
			logger.fatal("Invalid filename: " + hdfsName);
			return null;
		}
		String finalName = OsPath.join(localDir, basename);
		
		// Check if hadoop supports the compression codec of the input file
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		Path inputPath = new Path(hdfsName);
		CompressionCodec codec = factory.getCodec(inputPath);
		
		if (codec == null) { // Codec is not supported
			/*
			 * Copy file to local FS, then uncompress
			 */
			String tmpName = OsPath.join(tmpDir, OsPath.basename(hdfsName));
			if (copyFileFromHdfs(hdfsName, tmpName) == false) {
				logger.fatal("Could not copy file from HDFS: " + hdfsName);
				return null;
			}			

			if (compression.equals("none")) {				
				if (OsPath.rename(tmpName, finalName) == false) {
					logger.fatal("Could not rename " + tmpName + " to: " + finalName);
					return null;
				}			
			}
			else {
				if (! uncompressFile(tmpName, finalName, logDir)) {
					logger.fatal("Could not uncompress file: " + tmpName);
					return null;
				}
				if (! OsPath.delete(tmpName)) {
					logger.warn("Could not delete compressed file: " + tmpName);
					// File may have been deleted by uncompress script, so we just continue
				}
			}
		}
		else { // codec is supported
			CompressionInputStream in = null;
			FileOutputStream out = null;
			try {
				in = codec.createInputStream(hdfs.open(inputPath));
				out = new FileOutputStream(finalName);
			} catch (FileNotFoundException e) {
				logger.warn("File not found: " + e.getMessage());
				return null;
			}
			
			IOUtils.copyBytes(in, out, conf); // also closes streams at the end
		}
		if (! OsPath.isfile(finalName)) {
			logger.fatal("Uncompressed file does not exist");
			return null;
		}
		
		return finalName;
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
		ArrayList<String> localFiles = null;
		String compression = getDirCompression(hdfsName);
		if (compression == null) {
			logger.fatal("Invalid compression format: " + hdfsName);
			return null;
		}
		
		if (! OsPath.isdir(localDir)) {
			logger.debug("Creating directory on local FS: " + localDir);
			if (OsPath.mkdir(localDir) == false) {
				logger.fatal("Could not create directory on local FS: " + localDir);
				throw new IOException("Could not create directory on local FS: " + localDir);
			}
		}
		
		if (compression.equals("none")) { 
			// Directory is not compressed or packed, just download all files
			ArrayList<String> hdfsFiles = listdirR(hdfsName);
			if (hdfsFiles == null) {
				logger.warn("Invalid HDFS directory name: " + hdfsName);
				return null;
			}
			
			localFiles = new ArrayList<String>();
			for (String f: hdfsFiles) {
				String localName = OsPath.join(localDir, OsPath.basename(f));
				if (copyFileFromHdfs(f, localName) == false) {
					logger.warn("Could not copy file: " + f);
					return null;
				}
				
				localFiles.add(localName);				
			}
		}
		else {	// Directory is compressed or packed
			String tmpName = OsPath.join(tmpDir, OsPath.basename(hdfsName));
			// Download compressed/packed file
			if (copyFileFromHdfs(hdfsName, tmpName) == false) {
				logger.warn("Could not download compressed directory: " + hdfsName);
				return null;
			}
			
			localFiles = uncompressDirectory(tmpName, localDir, logDir);
			
			OsPath.delete(tmpName);
			
			if (localFiles == null) {
				logger.warn("Could not uncompressed directory: " + hdfsName);
				return null;
			}
		}
				
		
		return localFiles;
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
		
		logger.debug("Put local file " + localFilename + " to HDFS dir " + hdfsOutputDir + " with timestamp " + timestamp + " using compression " + compression);
		
		if (timestamp < 0) {
			logger.fatal("Invalid timestamp: " + timestamp);
			return null;
		}
		
		String timestampFilename = OsPath.join(tmpDir, OsPath.basename(localFilename) + "." + timestamp);	
		String compressedName = timestampFilename + "." + compression;
		String hdfsFilename = OsPath.join(hdfsOutputDir, OsPath.basename(compressedName));
		
		// Check if the compression format for the output file is supported by hadoop
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		Path outputPath = new Path(hdfsFilename);
		CompressionCodec codec = factory.getCodec(outputPath);
		
		if (codec == null) { // codec not supported
			/*
			 * Execute script to compress file to local FS, then copy compressed file to HDFS
			 */
			if (compression.equals("none")) {
				if (OsPath.rename(localFilename, compressedName) == false) {
					logger.fatal("Could not rename file: " + localFilename + " to " + compressedName);
					return null;
				} 
			}
			else {
				if (OsPath.rename(localFilename, timestampFilename) == false) {
					logger.fatal("Could not rename file: " + localFilename + " to " + timestampFilename);
					return null;
				}

				compressedName = compressFile(timestampFilename, tmpDir, logDir, compression); 			
			}

			if (compressedName == null) {
				logger.fatal("Could not compress file: " + timestampFilename + " with " + compression);
				return null;
			}

			if (copyFileToHdfs(compressedName, hdfsFilename) == false) {
				logger.fatal("Could not copy compressed file: " + compressedName + " to HDFS file: " + hdfsFilename);
				return null;
			}
			//logger.debug("File added to HDFS (via local FS): " + outputPath.toString());
		}
		else { // codec supported
			// Directly write file to HDFS
			try {						
				FileInputStream in = new FileInputStream(localFilename);
				CompressionOutputStream out = codec.createOutputStream(hdfs.create(outputPath));
				IOUtils.copyBytes(in, out, conf); // also closes streams at the end
								
				/*byte[] buf = new byte[512];
				while (true) {
					int nRead = in.read(buf);
					if (nRead > 0) {
						out.write(buf, 0, nRead);
					}
					else {
						break;
					}
				}
				
				logger.debug("Flush compression output stream");
				out.flush();
				logger.debug("Finnish compression output stream");
				out.finish();
				
				in.close();
				out.close();
				
				if (! isfile(hdfsFilename)) {
					logger.fatal("File not in HDFS: " + hdfsFilename);
					return null;
				}
				
				logger.debug("File added to HDFS (using codec): " + outputPath.toString());*/
			} catch (FileNotFoundException e) {
				logger.fatal("Could not open input file: " + localFilename + ": " + e.getMessage());
				return null;
			} catch (IOException e) {
				logger.fatal("Could not write file to HDFS: IOException: " + e.getMessage());
				return null;
			} 
		}
		
		return hdfsFilename;
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
		if (OsPath.fileSize(localFilename) == 0) {
			return null;
		}
		
		String compressedName = localFilename + "." + compression;
		String hdfsFilename = OsPath.join(hdfsOutputDir, OsPath.basename(compressedName));
		
		// Check if the compression format for the output file is supported by hadoop
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		Path outputPath = new Path(hdfsFilename);
		CompressionCodec codec = factory.getCodec(outputPath);
		
		if (codec == null) { // codec not supported
			/*
			 * Execute script to compress file to local FS, then copy compressed file to HDFS
			 */
			if (compression.equals("none")) {
				if (OsPath.rename(localFilename, compressedName) == false) {
					logger.fatal("Could not rename file: " + localFilename + " to " + compressedName);
					return null;
				} 
			}
			else {
				compressedName = compressFile(localFilename, tmpDir, logDir, compression); 		
				if (compressedName == null) {
					logger.fatal("Could not compress file: " + localFilename + " with " + compression);
					return null;
				}
			}
			
			if (copyFileToHdfs(compressedName, hdfsFilename) == false) {
				logger.fatal("Could not copy compressed file: " + compressedName + " to HDFS file: " + hdfsFilename);
				return null;
			}
		}
		else { // codec supported
			// Directly write file to HDFS
			try {
				FileInputStream in = new FileInputStream(localFilename);
				CompressionOutputStream out = codec.createOutputStream(hdfs.create(outputPath));
				IOUtils.copyBytes(in, out, conf); // also closes streams at the end
			} catch (FileNotFoundException e) {
				logger.fatal("Could not open input file: " + localFilename + ": " + e.getMessage());
				return null;
			} catch (IOException e) {
				logger.fatal("Could not write file to HDFS: IOException: " + e.getMessage());
				return null;
			} 
		}
		
		return hdfsFilename;
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
		if (timestamp < 0) {
			logger.fatal("Invalid timestamp: " + timestamp);
			return false;
		}
		if (localFiles.isEmpty()) {
			logger.warn("No local files to add to directory");
			return true;
		}
		
		if (compression.equals("none")) {
			String subdir = OsPath.join(hdfsDir, timestamp + "." + compression);
			for (String f: localFiles) {
				String hdfsName = OsPath.join(subdir, OsPath.basename(f));
				if (! copyFileToHdfs(f, hdfsName)) {
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
			
			System.out.println("Copy files to " + tmpSubdir);
			for (String f: localFiles) {
				String tmpFilename = OsPath.join(tmpSubdir, OsPath.basename(f));				
				if (! OsPath.copy(f, tmpFilename, logger)) {
					logger.fatal("Could not copy file to tmp directory: " + f);
					return false;
				}	
			}			
			
			// Compress the tmp directory
			String compressedDir = compressDirectory(tmpSubdir, tmpSubdir, logDir, compression); 
			if (compressedDir == null) {
				logger.fatal("Could not compress directory: " + tmpSubdir);						
				return false;
			}
			
			// Copy directory to HDFS
			String subdir = OsPath.join(hdfsDir, OsPath.basename(compressedDir));
			if (! copyFileToHdfs(compressedDir, subdir)) {
				return false;
			}
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
	public String putHDFSFile(String srcFilename, String dstDir,
			String tmpDir, String logDir, 
			String compression, long timestamp) throws IOException {
		
		logger.info(String.format("Move file %s to %s using compression %s and timestamp %d\n", srcFilename, dstDir, compression, timestamp));
		
		if (! isfile(srcFilename)) {
			logger.fatal("Source is not a file: " + srcFilename);
			return null;
		}
		else if (timestamp < 0) {
			logger.fatal("Invalid timestamp: " + timestamp);
			return null;
		}
		else if (! isValidCompression(compression)) {
			logger.fatal("Invalid compression format: " + compression);
			return null;
		}
		else if (fileSize(srcFilename) == 0) {
			logger.warn("Empty file ignored: " + srcFilename);
			return null;
		}
		
		if (isfile(dstDir)) {
			logger.fatal("Destiantion is a file and not a directory: " + dstDir);
		}
		else if (! isdir(dstDir)) {
			logger.warn("Creating destination directory: " + dstDir);
			mkdir(dstDir);
		}
		
		String srcCompression = OsPath.getLastExtension(srcFilename);				
		Path srcPath = new Path(srcFilename);
				
		/*
		 * Case 1: Source file is already compressed and can just be renamed
		 */
		if (srcCompression.equals(compression)) {
			// Replace compression extension with timestamp + compression extension
			String dstFilename = OsPath.join(dstDir, OsPath.basename(srcFilename.substring(0, srcFilename.length() - compression.length()) + timestamp + "." + compression));					
			if (moveHDFSFile(srcFilename, dstFilename) == false) {
				logger.fatal("Could not move " + srcFilename + " to " + dstFilename);
				return null;
			}
			return dstFilename;			
		}
		/*
		 * Case 2: The output file should not be compressed so the source file can just be 
		 * moved
		 */
		else if (compression.equals("none")) {
			String dstFilename = OsPath.join(dstDir, OsPath.basename(srcFilename)) + "." + timestamp + "." + compression;					
			if (moveHDFSFile(srcFilename, dstFilename) == false) {
				logger.fatal("Could not move " + srcFilename + " to " + dstFilename);
				return null;
			}
			return dstFilename;
		}
		/*
		 * Case 3: Compression is necessary so the files cannot simply be moved
		 */
		else {
			String dstFilename = OsPath.join(dstDir, OsPath.basename(srcFilename)) + "." + timestamp + "." + compression;
			Path dstPath = new Path(dstFilename);
			
			// Check if the compression formats of the files are supported by hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			//CompressionCodec inputCodec = factory.getCodec(srcPath);
			CompressionCodec outputCodec = factory.getCodec(dstPath);
			
			if (outputCodec != null) { // output codec is supported by hadoop
				// Read compressed file directly and write compressed file directly
				InputStream in = hdfs.open(srcPath); 
				CompressionOutputStream out = outputCodec.createOutputStream(hdfs.create(dstPath));
				IOUtils.copyBytes(in, out, conf); // also closes streams at the end
			}
			else {
				// Copy input file to local FS, compress and rewrite back to hadoop
				// The TroilkattFS methods will use a compression codec if possible
				String localSrcFilename = OsPath.join(tmpDir, OsPath.basename(srcFilename));
				if (copyFileFromHdfs(srcFilename, localSrcFilename) == false) {
					logger.fatal("Could not copy srcFile to local FS");
					return null;
				}				
				dstFilename = putLocalFile(localSrcFilename, dstDir, tmpDir, logDir, compression, timestamp); 
				if (dstFilename == null) {
					logger.fatal("Could not write file to: " + dstDir);
					return null;
				}
			}
			
			// Delete source file
			if(! deleteFile(srcFilename)) {
				logger.warn("Could not delete source file: " + srcFilename);
			}
			
			return dstFilename;
		}
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
		if (moveHDFSFile(srcFilename, dstFilename) == false) {
			logger.fatal("Could not move " + srcFilename + " to " + dstFilename);
			return null;
		}
		
		return dstFilename;					
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
		Path path = new Path(filename);
		try {			
			if (hdfs.getFileStatus(path).isDir()) {
				return false;
			}
			else {
				return true;
			}						
		} catch (FileNotFoundException e) {
			return false;
		} catch (IllegalArgumentException e) { 
			// invalid filename format
			return false;
		}
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
		Path dirPath = new Path(dirName);
		try {
			FileStatus dirStatus = hdfs.getFileStatus(dirPath);
			return dirStatus.isDir();
		} catch (FileNotFoundException e) {
			return false;
		}
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
		Path dirPath = new Path(dirName);
		FileStatus dirStatus = null;
		try {
			dirStatus = hdfs.getFileStatus(dirPath);
		} catch (FileNotFoundException e) {
			// do nothing
		}
		if ((dirStatus == null) || (dirStatus.isDir() == false)) {			
			if (hdfs.mkdirs(dirPath) == false) {
				throw new IOException("HDFS mkdir " + dirName + " failed");
			}
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
		if (! isfile(srcName)) {
			// Source file does not exist
			return false;
		}
		
		if (isdir(dstName)) {
			// Destination file is a directory
			return false;
		}
		
		if (isfile(dstName)) {
			// dst file already exists, so it is deleted
			if (! deleteFile(dstName)) {
				// Could not delete dst file
				return false;
			}
		}
		
		return hdfs.rename(new Path(srcName), new Path(dstName));
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
		return hdfs.delete(new Path(filename), false); // non-recursive
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
			return hdfs.delete(new Path(dirname), true);
		}
		else {
			// Not a directory
			return false;
		}
	}
	
	/**
	 * Get directory from path.
	 * 
	 * @param hdfsName filename with timestamp and compression format
	 * @return directory part of the filename (parent).
	 */
	@Override
	public String getFilenameDir(String hdfsName) {
		// There does not seem to be a hdfs command for removing the hostname from
		// a filename so we do it manually		
		if (hdfsName.startsWith("hdfs://")) {
			// remove protocol
			hdfsName = hdfsName.replace("hdfs://", "");
			// remove host
			int firstSlash = hdfsName.indexOf("/");
			hdfsName = hdfsName.substring(firstSlash);
		}
		return OsPath.dirname(hdfsName);			
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
		
		Path p = new Path(filename);
		FileStatus s = hdfs.getFileStatus(p);
		return s.getLen();
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
				logger.fatal("Could not copy status file from HDFS to local FS" + e1.toString());
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
					logger.fatal("Could not create new status file: " + e);
					throw e;
				}	
				// Attempt to save new status file to verify that the HDFS path is valid
				try {
					saveStatusFile(localFilename, hdfsFilename);
				} catch (IOException e) {
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
		FileStatus[] files = null;
					
		files = hdfs.listStatus(new Path(hdfsDir));			
		if (files == null) {
			logger.fatal("No files in directory");
			return;
		}
	
		
		for (FileStatus fs: files) {
			Path p = fs.getPath();
			if (fs.isDir()) {			
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
			logger.fatal("Could not copy from local FS to HDFS: " + e1.toString());
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
		Path localPath = new Path(localName);
		
		// Make sure there is no old .crc file in the folder (HDFS bug/feature)
		OsPath.delete(localName.replace(OsPath.basename(localName), "." + OsPath.basename(localName) + ".crc"));
		
		Path hdfsPath = new Path(hdfsName);	
		try {
			logger.debug(String.format("Copy HDFS file %s to local file %s\n", hdfsName, localName));			
			hdfs.copyToLocalFile(hdfsPath, localPath);			
		} catch (IOException e1) {
			logger.fatal("Could not copy from HDFS to local FS: " + e1.toString());
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
		URI uri = p.toUri();		
		return uri.getPath();		
	}
}
