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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
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
	 * @param hdfs HDFS handle
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
		String finalName = OsPath.join(localDir, basename);
		
		// Check if commons-compress supports the file format
		InputStream is = new FileInputStream(nfsName);
		CompressorInputStream cin = null;
		try {
			cin = new CompressorStreamFactory().createCompressorInputStream(compression, is);
		} catch (CompressorException e) { // This is expected, for example for the "none" format
			logger.warn("Unknwon compression: " + compression);			
		}
		
		if (cin == null) { // Codec is not supported
			if (compression.equals("none")) {
				// Copy file to local storage
				if (OsPath.copy(nfsName, finalName) == false) {
					logger.fatal("Could not copy " + nfsName + " to: " + finalName);
					return null;
				}			
			}
			else { // attempt other compressors
				if (! uncompressFile(nfsName, finalName, logDir)) {
					logger.fatal("Could not uncompress file: " + nfsName);
					return null;
				}				
			}
			is.close();
		}
		else { // codec is supported
			FileOutputStream out = null;
			try {
				out = new FileOutputStream(finalName);
			} catch (FileNotFoundException e) {
				logger.warn("File not found: " + e);
				return null;
			}
			
			/*
			 * Copy file data
			 */
			final byte[] buffer = new byte[4096]; // use a 4KB buffer
			int n = 0;
			while (true) {
				n = cin.read(buffer);
				if (n == -1) { // EOF
					break;
				}
				out.write(buffer, 0, n);
			}
			
			out.close();
			cin.close();
		}
		if (! OsPath.isfile(finalName)) {
			logger.fatal("Uncompressed file does not exist");
			return null;
		}
		
		return finalName;
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
		if (! isdir(nfsName)) {
			logger.error("Not a directory: " + nfsName);
			return null;
		}
				
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
		
		if (compression.equals("none")) { 
			// Directory is not compressed or packed, so just copy all files
			ArrayList<String> nfsFiles = listdirR(nfsName);
			if (nfsFiles == null) {
				logger.warn("Empty directory, or directory could not be listed: " + nfsName);
				return null;
			}
			
			localFiles = new ArrayList<String>();
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
			String tmpName = OsPath.join(tmpDir, OsPath.basename(nfsName));
			
			/*
			 *  Attempt to open using commons-compress libraries
			 */
			// Check if commons-compress supports the file format
			InputStream is = new FileInputStream(nfsName);
			CompressorInputStream cin = null;
			ArchiveInputStream ain = null;
			// the local varuable compression contains both the archive and compression format (e.g. tar.gz) 
			String compressionExtension = OsPath.getLastExtension(nfsName); // just compression format
			String archiveExtension = OsPath.getLastExtension(OsPath.removeLastExtension(nfsName)); // just archive format
			try {
				cin = new CompressorStreamFactory().createCompressorInputStream(compressionExtension, is);
			} catch (CompressorException e) { // This is expected, for example for the "none" format
				logger.warn("Unknwon compression: " + compressionExtension);			
			}
			try {
				ain = new ArchiveStreamFactory().createArchiveInputStream(archiveExtension, cin);
			} catch (ArchiveException e) { // This is expected, for example for the "none" format
				logger.warn("Unknwon archive: " + archiveExtension);			
			}
			
			if ((cin == null) || (ain == null)) { // Attempt to use fallback compression methods
				OsPath.copy(nfsName, tmpName);
				localFiles = uncompressDirectory(tmpName, localDir, logDir);
				
				OsPath.delete(tmpName);
				
				if (localFiles == null) {
					logger.warn("Could not uncompressed directory: " + nfsName);
					return null;
				}
			}
			else {
				while (true) { // for all files in archive
					ArchiveEntry ae = ain.getNextEntry();
					String outputFilename = OsPath.join(ae.getName())
					FileOutputStream fos = new FileOutputStream();
					long fileSize = ae.getSize();
					long bytesRead = 0;
					while (bytesRead < fileSize) {
						ain.read(b, off, len)
					}
					fos.write(b, off, len)
				}
				
				/*
				 * Copy file data
				 */
				final byte[] buffer = new byte[4096]; // use a 4KB buffer
				int n = 0;
				while (true) {
					n = cin.read(buffer);
					if (n == -1) { // EOF
						break;
					}
					out.write(buffer, 0, n);
				}
			}
			
			// Download compressed/packed file
			if (copyFileFromHdfs(hdfsName, tmpName) == false) {
				logger.warn("Could not download compressed directory: " + hdfsName);
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
	
	/**
	 * Copy file data in 4KB chunks
	 * 
	 * @param fin initialized file input stream
	 * @param fos initialized file output stream
	 * @return none
	 * @throws IOException 
	 */
	private void copyFileData(FileInputStream fin, FileOutputStream fos) throws IOException {
		/*
		 * Copy file data
		 */
		final byte[] buffer = new byte[4096]; // use a 4KB buffer
		int n = 0;
		while (true) {
			n = fin.read(buffer);
			if (n == -1) { // EOF
				break;
			}
			fos.write(buffer, 0, n);
		}
	}
}
