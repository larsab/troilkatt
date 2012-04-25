package edu.princeton.function.troilkatt.fs;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



/**
 * Use TroilkattFS instead.
 */
@Deprecated
public class HDFSUtils {
	/**
	 * Return list of files in a HDFS directory
	 * 
	 * @param hdfs: HDFS FileSystem handle
	 * @param dir: HDFS directory
	 * @param logger: caller's logger instance
	 * 
	 * @return FileStatus list
	 * @throws IOException 
	 */
	public static FileStatus[] listDir(FileSystem hdfs, String dir, Logger logger) throws IOException {
		logger.info("Read contents of HDFS directory: " + dir);				
		FileStatus[] files = null;
		try {			
			files = hdfs.listStatus(new Path(dir));
			if (files == null) {
				logger.fatal("HDFS directory read failed");
				System.err.println("Could not read content of directory: " + dir);
				System.exit(-1);
			}
		} catch (IOException e1) {
			logger.fatal("Could not read filelist from HDFS directory: " + dir);
			logger.fatal(e1.toString());
			throw e1;
		}
		
		return files;
	}
	
	/**
	 * Move a file in HDFS
	 * 
	 * @param src: source filename
	 * @param dst: destination filename
	 * 
	 * @return: none
	 * @throws IOException 
	 */
	public static void move(FileSystem hdfs, String src, String dst, Logger logger) throws IOException {
		if (src.equals(dst)) {
			return;
		}
		
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);
		Path dstDir = new Path(OsPath.dirname(dst));
		
		if (! hdfs.exists(dstDir)) {
			logger.debug("Creating new destination directory: " + dstDir);
			if (! hdfs.mkdirs(dstDir)) {
				logger.fatal("Failed to create new destination directory: " + dstDir);
				throw new RuntimeException("Failed to create new destination directory: " + dstDir);
			}
		}
		
		if (hdfs.exists(dstPath)) {
			logger.debug("Deleting existing file: " + dstPath);
			if (! hdfs.delete(dstPath, false)) {
				logger.fatal("Failed to delete existing destination file: " + dstPath);
				throw new RuntimeException("Failed to delete existing destination file: " + dstPath);
			}
		}
		
		String basename = OsPath.basename(src);
		logger.debug("Move file from map-specific output directory to job output directory: " + basename);
		if (! hdfs.rename(srcPath, dstPath)) {
			logger.fatal("Could not move outputfile");
			logger.fatal("Source: " + srcPath);
			logger.fatal("Destination: " + dstPath);
			throw new RuntimeException("Could not move file: " + basename);
		}				
		
		if (! hdfs.exists(dstPath)) {
			throw new IOException("Could not move file: " + basename);
		}
	}
	
	/**
	 * Helper function to return an absolute path, without the hostname:port part
	 */
	public static String getAbsoluteName(String qualifiedName) {			
		if (! qualifiedName.startsWith("hdfs://")) {
			throw new RuntimeException("Unknown qualified name: " + qualifiedName);				
		}
		
		String absoluteName = qualifiedName.substring(7); // remove hdfs://
		
		return absoluteName.substring( absoluteName.indexOf('/') );
	}
	
	
	
	
	

}
