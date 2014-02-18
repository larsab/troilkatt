package edu.princeton.function.troilkatt.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.ar.ArArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;

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
	 * @param tfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdir(String tfsDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Do a recursive listing of files.
	 * 
	 * @param tfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException 
	 */
	public ArrayList<String> listdirR(String tfsDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Get a list of the files with the newest timestamps in a directory. The listing
	 * is recursive.
	 * 
	 * @param tfsDir directory to list
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdirN(String tfsDir) throws IOException {
		ArrayList<String> allFiles = listdirR(tfsDir);
		if (allFiles == null) {
			logger.warn("No files in directory: " + tfsDir);
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
	 * @param tfsDir directory to list
	 * @param timestamp timestamp files are matched against
	 * @return ArrayList of String with absolute filenames, or null if the directory
	 * name is not valid.
	 * @throws IOException
	 */
	public ArrayList<String> listdirT(String tfsDir, long timestamp) throws IOException {
		ArrayList<String> allFiles = listdirR(tfsDir);
		if (allFiles == null) {
			logger.warn("No files in directory: " + tfsDir);
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
	 * @param tfsDir directory with sub-directories to check
	 * @return sub-directory (basename) with highest timestamp, or null if no timestamped directory
	 * is found, or null if multiple timestamped directories with different names are found.
	 * @throws IOException 
	 */
	public String getNewestDir(String tfsDir) throws IOException {
		ArrayList<String> subdirs = listdir(tfsDir);
		
		if (subdirs == null) {
			logger.warn("Invalid directory: " + tfsDir);
			return null;
		}
		else if (subdirs.size() == 0) {
			logger.warn("No timestamped sub-directories in: " + tfsDir);
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
	 * Copy file from TFS to the local FS directory, uncompress the file,
	 * and remove the timestamp and compression extension. 
	 * 
	 * @param tfsName filename in tfs. The file must have a timestamp and compression extension.
	 * @param localDir directory on local FS where file is copied and uncompressed to.
	 * @param tmpDir directory on local FS used to hold file before compression
	 * @param logDir directory where logfiles are written to.
	 * @return local FS filename (joined with localDir), or null if the file could not be 
	 * copied/uncompressed
	 * @throws IOException 
	 */
	public String getFile(String tfsName, String localDir, String tmpDir, String logDir) throws IOException {
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Copy all files in a tfs directory to a local FS directory, uncompress the file,
	 * and remove the timestamps and compression extension.
	 * 
	 * @param tfsName tfs directory with files to download
	 * @param localDir directory where files are written to
	 * @param logDir directory for logfiles
	 * @param tmpDir directory where temporal files are stored during uncompression
	 * @return list of filenames on local FS, or null if the directory name is invalid or one or more files could
	 * not be downloaded
	 * @throws IOException 
	 */
	public ArrayList<String> getDirFiles(String tfsName, String localDir, String logDir, String tmpDir) throws IOException {		
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Compress a file, add a timestamp, and move the file from local FS to tfs.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to tfs and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.
	 * @param tfsOutputDir tfs output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return tfs filename on success, null otherwise
	 */
	public String putLocalFile(String localFilename, String tfsOutputDir, String tmpDir, String logDir, 
			String compression, long timestamp) {
		
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Compress a file and move the file from local FS to tfs.
	 * 
	 * NOTE! This function will move the source file, since we assume that there may be some large files
	 * that are put to tfs and that creating multiple copies of these is both space and time consuming.
	 *  
	 * @param localFilename absolute filename on local FS.
	 * @param tfsOutputDir tfs output directory where file is copied.
	 * @param tmpDir directory on local FS where temporary files can be stored
	 * @param logDir logfile directory on local FS
	 * @param compression compression method to use for file 
	 * @param timestamp timestamp to add to file
	 * @return tfs filename on success, null otherwise
	 */
	public String putLocalFile(String localFilename, String tfsOutputDir, String tmpDir, String logDir, String compression) {
		throw new RuntimeException("Method not implemented");
	}
	

	/**
	 * Create a directory using the timestamp as name, compress  the directory files, and copy the 
	 * compressed directory from local FS to tfs 
	 * 
	 * @param tfsDir tfs directory where new sub-directory is created
	 * @param timestamp name for new sub-directory
	 * @param metaFiles files to copy to tfs
	 * @param localFiles list of absolute filenames in directory to copy to tfs
	 * @param compression method to use for directory 
	 * @param logDir directory where logfiles are stored
	 * @param tmpDir directory on local FS where compressed directory is stored
	 * @return true if all files were copied to tfs, false otherwise
	 */
	public boolean putLocalDirFiles(String tfsDir, long timestamp, ArrayList<String> localFiles, 
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
	public String putTFSFile(String srcFilename, String dstDir,
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
	public String putTFSFile(String srcFilename, String dstDir) throws IOException { 
			
		throw new RuntimeException("Method not implemented");
	}

	/**
	 * Compress a file using an external program.
	 * 
	 * @param secFilename file to compress.
	 * @param outputDir directory where compressed file is written
	 * @param logDir directory where log files are written.
	 * @param compression format to use.
	 * @return compressed filename, or null if compression failed.
	 */
	public String compressFile(String uncompressedFilename, String outputDir, String logDir, String compression) {
		String basename = OsPath.basename(uncompressedFilename);
		String compressedFilename = OsPath.join(outputDir, basename + "." + compression);
		
		// Check if commons-compress has codec for the file format (gz, bzip, xz, pack200)
		OutputStream os = null;
		try {
			os = new FileOutputStream(compressedFilename);
		} catch (FileNotFoundException e1) {
			logger.fatal("Could not open output stream: ", e1);
			return null;
		}
		CompressorOutputStream cos = null;
		try {
			if (compression.equals("bz2")) {
				cos = new CompressorStreamFactory().createCompressorOutputStream("bzip2", os);
			}
			else {
				cos = new CompressorStreamFactory().createCompressorOutputStream(compression, os);
			}
		} catch (CompressorException e) { 
			// This is expected, for example for the "none" format
			logger.warn("Unknown compression format: " + compression);			
		}

		if (cos == null) { // codec not supported
			// output stream is not used if the codec is not supported
			try {
				os.close();
				OsPath.delete(compressedFilename);
			} catch (IOException e) {
				logger.warn("Could not close output stream");
				// Attempt to continue
			} 

			/*
			 * Execute script to compress file to local FS, then copy compressed file to tfs
			 */
			if (compression.equals("none")) { // No compression to use
				if (OsPath.rename(uncompressedFilename, compressedFilename) == false) {
					logger.error("Could not rename file: " + uncompressedFilename + " to " + compressedFilename);
					return null;
				} 
			}
			else { // Attempt fallback compression
				logger.fatal("Unknown compression format: " + compression);
				return null;				
			}
		}
		else { // codec supported
			// Use commons compress libraries
			InputStream is = null;
			try {
				is = new FileInputStream(uncompressedFilename);
			} catch (FileNotFoundException e) {
				logger.error("Could not open local file: " + uncompressedFilename + ": ", e);
				return null;
			}

			/*
			 * Copy file data
			 */
			try {
				final byte[] buffer = new byte[4096]; // use a 4KB buffer
				int n = 0;
				while (true) {
					n = is.read(buffer);
					if (n == -1) { // EOF
						break;
					}
					cos.write(buffer, 0, n);
				}
				cos.close();
				is.close();
			} catch (IOException e) {
				logger.error("IOException during file copy: ", e);
				try {
					cos.close();
					if (OsPath.delete(compressedFilename)) {
						logger.error("Could not delete output file: " + compressedFilename);
					}
				} catch (IOException e1) {
					logger.error("Could not close output stream: ", e);					
				}
				return null;
			}
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
		String compression = OsPath.getLastExtension(compressedName);
		
		// Check if commons-compress supports the file format
		InputStream is = null;
		try {
			is = new FileInputStream(compressedName);
		} catch (FileNotFoundException e1) {
			logger.error("Could not open compressed file: " + compressedName, e1);
			return false;
		}
		CompressorInputStream cin = null;
		try {
			if (compression.equals("bz2")) {
				cin = new CompressorStreamFactory().createCompressorInputStream("bzip2", is);
			}
			else {
				cin = new CompressorStreamFactory().createCompressorInputStream(compression, is);
			}
		} catch (CompressorException e) { // This is expected, for example for the "none" format
			logger.warn("Unknown compression: " + compression);			
		}

		if (cin == null) { // Codec is not supported
			// Input stream is not used if codec is not supported
			try {
				is.close();
			} catch (IOException e) {
				logger.warn("Could not close input stream: ",  e);
				// Attempt to continue
			}

			if (compression.equals("none")) {
				// Copy file to local storage
				if (OsPath.copy(compressedName, uncompressedName) == false) {
					logger.fatal("Could not copy " + compressedName + " to: " + uncompressedName);
					return false;
				}			
			}
			else { // attempt other compressors
				logger.warn("Unknown extension for file: " + basename);
				return false;
			}

		}
		else { // codec is supported
			FileOutputStream out = null;
			try {
				out = new FileOutputStream(uncompressedName);
			} catch (FileNotFoundException e) {
				logger.warn("Could not open uncompressed file stream: ", e);
				return false;
			}

			/*
			 * Copy file data
			 */
			try {
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
			} catch (IOException e) {
				logger.warn("IOException when uncompress file: ", e);
				OsPath.delete(uncompressedName);
			}
		}	
		
		return true;
	}
	
	/**
	 * Compress and archive a directory using a specified compression method.
	 * 
	 * @param inputDir directory with files to archive/ compress.
	 * @param outputDirname name of archive without compression extension 
	 * @param logDir directory where log files are written.
	 * @param compression format to use.
	 * @return compressed filename, or null if compression failed.
	 */
	public String compressDirectory(String inputDir, String outputDirname, String logDir, String compression) {
		if (OsPath.isdir(inputDir) == false) {
			logger.warn("Not a directory on the local file system: " + inputDir);
			return null;
		}		
		
		final byte[] buffer = new byte[4096]; // use a 4KB buffer
		
		String[] inputFiles = OsPath.listdirR(inputDir, logger);
				
		//String compressedName = OsPath.join(outDir, basename + "." + compression);
		String compressedName = outputDirname + "." + compression;
		String compressionFormat = OsPath.getLastExtension(compression);
		String archiverFormat = OsPath.removeLastExtension(compression);
		
		// Attempt to open compression stream
		OutputStream os = null;		
		CompressorOutputStream cos = null;
		try {
			os = new FileOutputStream(compressedName);
			if (compressionFormat.equals("bz2")) {
				cos = new CompressorStreamFactory().createCompressorOutputStream("bzip2", os);
			}
			else {
				cos = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat, os);
			}
		} catch (CompressorException e) { 
			// This is expected, for example if compression should not be used
			logger.warn("Unknown compression format: " + compressionFormat);			
		} catch (FileNotFoundException e) {
			logger.error("Could not open output stream to file: " + compressedName, e);
			return null;
		}

		ArchiveOutputStream aos = null;
		if (cos == null) { // no compression		 
			try {
				aos = new ArchiveStreamFactory().createArchiveOutputStream(compression, os);
				archiverFormat = compression;
			} catch (ArchiveException e) { 
				// This is (somewhat) expected, for example if an alternative 
				logger.warn("Unknown archiver format: " + compression);			
			}
		} 
		else { // with compression
			try {
				aos = new ArchiveStreamFactory().createArchiveOutputStream(archiverFormat, cos);
			} catch (ArchiveException e) { 
				// This is (somewhat) expected, for example if an alternative 
				logger.warn("Unknown archiver format: " + archiverFormat);
			}			
		}		
		
		if (aos != null) {
			// Make sure it is one of the below supported archive formats
			if  (! archiverFormat.equals("ar") &&  
					! archiverFormat.equals("zip") &&
					! archiverFormat.equals("tar") &&
					! archiverFormat.equals("jar") &&
					! archiverFormat.equals("cpio")) {				
				logger.error("Unexpected archive format");
				try {
					aos.close();
				} catch (IOException e) {
					logger.warn("Could not close archvie in exception clause: ", e);
				}
				if (OsPath.delete(compressedName) == false) {
					logger.warn("Could not delete archive file in exception clause: " + compressedName);
				}
				return null;
			}
			
			try { // one big try that catches all IOexceptions when adding files to an archive
				for (String f: inputFiles) {				
					InputStream is = new FileInputStream(f);								
					String arName = OsPath.absolute2relative(f, inputDir);
								
					ArchiveEntry ar = null;
					if  (archiverFormat.equals("ar")) {
						ar = new ArArchiveEntry(new File(f), arName);
					}
					else if (archiverFormat.equals("zip")) {
						ar = new ZipArchiveEntry(new File(f), arName);
					}
					else if (archiverFormat.equals("tar")) {
						ar = new TarArchiveEntry(new File(f), arName);				
						((TarArchiveOutputStream)aos).setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
					}				
					else if (archiverFormat.equals("cpio")) {
						ar = new CpioArchiveEntry(new File(f), arName);
					}				
					
					/*
					 * Add entry and copy file data
					 */
					aos.putArchiveEntry(ar);					 					
					
					// TODO: use IOUtils
					int n = 0;
					while (true) {
						n = is.read(buffer);
						if (n == -1) { // EOF
							break;
						}
						aos.write(buffer, 0, n);
					}				
					aos.closeArchiveEntry();
					is.close();				
				} // for input files
			
				aos.close();			
			} catch (IOException e1) {
				logger.error("Could not add file to archive: ", e1);
				try {
					aos.close();
				} catch (IOException e2) {
					logger.warn("Could not close archvie in exception clause: ", e2);
				}
				if (OsPath.delete(compressedName) == false) {
					logger.warn("Could not delete archive file in exception clause: " + compressedName);
				}
				return null;
			}
			return compressedName;
		}
		else { // archiver not supported
			logger.fatal("Unknown archive format: " + compression);
			return null;
		}		
	}

	/**
	 * Uncompress a directory using an external program.
	 * 
	 * Note. If the uncompress/unpack fails before all files are unpacked, the files that were 
	 * unpacked before the fail are not deleted. 
	 * 
	 * @param compressedDir compressed directory
	 * @param dstDir destination directory. Note that the files are uncompressed to this directory, and not to a sub-directory
	 * within it.
	 * @param logDir directory where logfiles are saved 
	 * @return list of uncompressed directory content, or null if the directory could not be decompressed.
	 */
	public ArrayList<String> uncompressDirectory(String compressedDir, String dstDir, String logDir) {
		String compression = getDirCompression(compressedDir);
		// the local variable compression my contain both the archive and compression format (e.g. tar.gz) 
		String compressionExtension = OsPath.getLastExtension(compression); // just compression format
		String archiveExtension = OsPath.getLastExtension(OsPath.removeLastExtension(compression)); // just archive format		
		logger.debug("Compressed dir:" + compressedDir +", destination dir: " + dstDir);
		ArrayList<String> dirContent = new ArrayList<String>();
		
		/*
		 *  Attempt to open using commons-compress libraries
		 */
		// Check if commons-compress supports the file format
		InputStream is;
		try {
			is = new FileInputStream(compressedDir);
		} catch (FileNotFoundException e1) {
			logger.warn("Could not open: " + compressedDir + ": ", e1);
			return null;
		}
		CompressorInputStream cin = null;
		ArchiveInputStream ain = null;
		
		try {
			if (compressionExtension.equals("bz2")) {
				cin = new CompressorStreamFactory().createCompressorInputStream("bzip2", is);
			}
			else {
				cin = new CompressorStreamFactory().createCompressorInputStream(compressionExtension, is);
			}
		} catch (CompressorException e) { // This is expected, for example for the "none" format
			logger.warn("Unknown compression: " + compressionExtension);			
		}
		
		if (cin != null) { // valid compression extension
			try {
				ain = new ArchiveStreamFactory().createArchiveInputStream(archiveExtension, cin);
			} catch (ArchiveException e) { // This is expected, for example for the "none" format
				logger.warn("Unknown archive: " + archiveExtension);			
			}
		} 
		else { // not a valid compression extension, so we assume it is not a compressed archive
			try {
				ain = new ArchiveStreamFactory().createArchiveInputStream(compression, is);
			} catch (ArchiveException e) { // This is expected, for example for the "none" format
				logger.warn("Unknown archive: " + compressionExtension);		
			}
		}
		
		if ((cin == null) && (ain == null)) { // Attempt to use fallback compression methods
			logger.fatal("Unknown archive format: " + compression);
			return null;
		}
		
		//final byte[] buffer = new byte[4096]; // use a 4KB buffer	
		try {
			while (true) { // for all files in archive
				ArchiveEntry ae = ain.getNextEntry();					
				if (ae == null) { // no more entries
					break;
				}				
								
				if (ae.isDirectory()) {
					OsPath.mkdir(OsPath.join(dstDir, ae.getName()));
					continue;
				}
				
				// entry is for a file
				String outputFilename = OsPath.join(dstDir, ae.getName());
				FileOutputStream fos = new FileOutputStream(outputFilename);
				
				IOUtils.copy(ain, fos);
				//long fileSize = ae.getSize();
				//long bytesRead = 0;
				//while (bytesRead < fileSize) {
				//	int n = ain.read(buffer);
				//	if (n == -1) { // EOF
				//		break;
				//	}
				//	fos.write(buffer, 0, n);
				//	bytesRead += n;
				//}
				fos.close();
				dirContent.add(outputFilename);
			}
			ain.close();
		} catch (IOException e1) {
			logger.error("Could not unpack archive entry: ", e1);
			try {
				ain.close();
			} catch (IOException e2) {
				logger.warn("Could not close archive in exception clause: ", e2);
			}
			return null;
		}
		
		if (cin != null) {
			try {
				cin.close();
			} catch (IOException e) {
				logger.warn("Could not close compression stream: ", e);
			}
		}
		
		return dirContent;
	}

	/**
	 * Check if file exists in tfs
	 *
	 * @param filename filename to check
	 * @return true if directory exist
	 * @throws IOException
	 */
	public boolean isfile(String filename) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Check if directory exists in tfs
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
	 * @param tfs tfs handle
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
	 * Delete file in tfs
	 * 
	 * @param filename file to delete
	 * @return true if file was deleted, false otherwise
	 * @throws IOException 
	 */
	public boolean deleteFile(String filename) throws IOException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Delete a directory recursively in tfs
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
	 * @param tfsOutputDir tfs directory to clean
	 * @param timestamp current timestamp
	 * @param storageTime time to keep files in the directory. The time is given in days.
	 * If storageTime is -1, no files are deleted. If storageTime is 0 all files are 
	 * deleted.
	 * @throws IOException 
	 */
	public void cleanupDir(String tfsOutputDir, long timestamp, int storageTime) throws IOException {
		if (storageTime <= -1) {
			logger.info("All files in directory are kept forever: " + tfsOutputDir);
			return;
		}
		else if (storageTime == 0) {
			storageTime = Integer.MIN_VALUE;; // to ensure all files are deleted
		}
		
		ArrayList<String> files = listdirR(tfsOutputDir);
		if (files == null) {
			logger.warn("Invalid directory: " + tfsOutputDir);
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
	 * @param tfsMetaDir tfs meta directory to clean
	 * @param timestamp current timestamp
	 * @param storageTime time to keep files in the directory. The time is given in days.
	 * If storageTime is -1, no files are deleted. If storageTime is 0 all files are 
	 * deleted.
	 * @throws IOException 
	 */
	public void cleanupMetaDir(String tfsMetaDir, long timestamp, int storageTime) throws IOException {
		if (storageTime <= -1) {
			logger.info("All files in directory are kept forever: " + tfsMetaDir);
			return;
		}
		else if (storageTime == 0) {
			storageTime = Integer.MIN_VALUE; // to ensure all files are deleted
		}
		
		String newestDir = getNewestDir(tfsMetaDir);
		if (newestDir == null) {
			logger.warn("No directory archive found: " + tfsMetaDir);
			return;
		}
		
		ArrayList<String> files = listdir(tfsMetaDir);
		if (files == null) {
			logger.warn("Invalid directory: " + tfsMetaDir);
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
	 * @param tfsName filename with timestamp and compression format
	 * @return basename with timestamp and compression format removed, or null if the
	 * filename is invalid.
	 */
	public String getFilenameName(String tfsName) {
		String basename = OsPath.basename(tfsName);
		String[] parts = basename.split("\\.");
		
		if (parts.length > 2) {
			String name = parts[0];
			for (int i = 1; i < parts.length - 2; i++) {
				name = name + "." + parts[i];
			}
			
			return name;
		}
		else {
			logger.fatal("Invalid tfs filename: " + tfsName);
			return null;
		}		
	}
	
	/**
	 * Get directory from path.
	 * 
	 * @param tfsName filename with timestamp and compression format
	 * @return directory part of the filename (parent).
	 */
	public String getFilenameDir(String tfsName) {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Get timestamp from filename
	 * 
	 * @param tfsName filename with timestamp and compression format.
	 * @return timestamp, or -1 if the filename is invalid.
	 */
	public long getFilenameTimestamp(String tfsName) {
		String basename = OsPath.basename(tfsName);
		String[] parts = basename.split("\\.");
		if (parts.length < 3) {
			logger.warn("Invalid tfs filename: " + tfsName);
			return -1;
		}
		
		try {
			return Long.valueOf(parts[parts.length - 2]);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid tfs filename: timestamp is not a number: " + tfsName, e);
			return -1;
		}
	}
	
	/**
	 * Get compression format from filename
	 * 
	 * @param tfsName filename with timestamp and compression format.
	 * @return compression format (gz, bz2, etc), or null if the filename is invalid.
	 */
	public String getFilenameCompression(String tfsName) {
		String basename = OsPath.basename(tfsName);
		String[] parts = basename.split("\\.");
	
		if (parts.length >= 3) {
			return parts[parts.length - 1];
		}
		else {
			logger.fatal("Invalid tfs filename: " + tfsName);
			return null;
		}
	}
	
	/**
	 * Get timestamp from directory name
	 * 
	 * @param tfsName filename with timestamp and compression format (timestamp.compression or timestamp.tar.compression).
	 * @return timestamp, or -1 if the filename is invalid.
	 */
	public long getDirTimestamp(String tfsName) {
		String basename = OsPath.basename(tfsName);
		String[] parts = basename.split("\\.");
		
		if (parts.length < 2) {
			logger.fatal("Invalid tfs directory: " + tfsName);
			return -1;		
		}
		
		String timestampString = parts[parts.length - 2];
		if (timestampString.equals("tar")) {
			if (parts.length >= 3) {
				timestampString = parts[ parts.length - 3 ];
			}
			else {
				logger.fatal("Invalid compression format for tfs directory: " + tfsName);
				return -1;
			}
		}		
		
		try {
			return Long.valueOf(timestampString);
		}
		catch (NumberFormatException e) {
			logger.fatal("Invalid tfs directory: timestmap is not a number" + tfsName, e);
			return -1;
		}
	}
	
	/**
	 * Get compression format from directory
	 * 
	 * @param tfsName filename with timestamp and compression format.
	 * @return compression format, or null if the filename is invalid.
	 */
	public String getDirCompression(String tfsName) {
		String basename = OsPath.basename(tfsName);
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
			logger.fatal("Invalid tfs filename: " + tfsName);
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
	 * Copy a status file from tfs to local FS
	 * 
	 * @param tfsFilename Source tfs filename
	 * @param localFilename Destination local FS filename
	 * @throws IOException
	 * @throws TroilkattPropertiesException if invalid tfsFilename
	 */	
	public void getStatusFile(String tfsFilename, String localFilename) throws IOException, TroilkattPropertiesException {
		throw new RuntimeException("Method not implemented");
	}
	
	/**
	 * Copy a status file from local FS to tfs
	 * 
	 * @param localFilename Source local FS filename
	 * @param tfsFilename Destination tfs filename
	 * 
	 * @throws IOException if file could not be copeid to tfs
	 * @throws TroilkattPropertiesException 
	 */
	public void saveStatusFile(String localFilename, String tfsFilename) throws IOException, TroilkattPropertiesException {		
		throw new RuntimeException("Method not implemented");
	}
}
