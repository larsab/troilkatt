package edu.princeton.function.troilkatt.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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

import edu.princeton.function.troilkatt.pipeline.Stage;

/**
 * Troilkatt wrapper for HDFS
 * 
 * Note the code is written for version 0.20.X. Some minor changes are required for 0.21.X.
 */
public class TroilkattFS {
	/* List of compression algorithms supported by Troilkatt */
	static final String[] compressionExtensions = {
		"none", /* no compression */
		"gz",   /* gnu zip compression */
		//"gzip",   /* gnu zip compression */
		"bz2",  /* bzip compression */
		//"bzip"  /* bzip compression */
	};
	
	public FileSystem hdfs;
	protected Configuration conf;
	protected Logger logger;
	
	/**
	 * Constructor.
	 * 
	 * @param hdfs HDFS handle
	 */
	public TroilkattFS(FileSystem hdfs) {
		this.hdfs = hdfs;
		conf = hdfs.getConf();
		logger = Logger.getLogger("troilkatt.tfs");
	}
	
	/**
	 * Do a (non-recurisive) directory listing.
	 * 
	 * @param hdfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
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
					logger.warn("Could not directory file: " + f);
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
	public boolean putLocalDirFiles(String hdfsDir, long timestamp, ArrayList<String> localFiles, 
			String compression, String logDir, String tmpDir) {		
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
			String compressedDir = compressDirectory(tmpSubdir, tmpDir, logDir, compression); 
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
	 * Mkdir that first checks if a directory exists. Otherwise it creates the directory.
	 *
	 * @param hdfs HDFS handle
	 * @param dirName directory to create
	 * @throws IOException if an IOException occurs or if the mkdir failed.
	 */
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
	 * already exists
	 * @throws IOException 
	 */
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
			logger.fatal("Invalid hdfs filename: " + hdfsName);
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
	 * Get timestamp from filename
	 * 
	 * @param hdfsName filename with timestamp and compression format.
	 * @return timestamp, or -1 if the filename is invalid.
	 */
	public long getFilenameTimestamp(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
		if (parts.length < 3) {
			logger.warn("Invalid hdfs filename: " + hdfsName);
			return -1;
		}
		
		try {
			return Long.valueOf(parts[parts.length - 2]);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid hdfs filename: " + hdfsName);
			return -1;
		}
	}
	
	/**
	 * Get compression format from filename
	 * 
	 * @param hdfsName filename with timestamp and compression format.
	 * @return compression format, or null if the filename is invalid.
	 */
	public String getFilenameCompression(String hdfsName) {
		String basename = OsPath.basename(hdfsName);
		String[] parts = basename.split("\\.");
	
		if (parts.length >= 3) {
			return parts[parts.length - 1];
		}
		else {
			logger.fatal("Invalid hdfs filename: " + hdfsName);
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
			logger.fatal("Invalid hdfs directory: " + hdfsName);
			return -1;		
		}
		
		String timestampString = parts[parts.length - 2];
		if (timestampString.equals("tar")) {
			if (parts.length >= 3) {
				timestampString = parts[ parts.length - 3 ];
			}
			else {
				logger.fatal("Invalid compression format for hdfs directory: " + hdfsName);
				return -1;
			}
		}		
		
		try {
			return Long.valueOf(timestampString);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid hdfs directory: " + hdfsName);
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
		if (! isfile(filename)) {
			return -1;
		}
		
		Path p = new Path(filename);
		FileStatus s = hdfs.getFileStatus(p);
		return s.getLen();
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
