package edu.princeton.function.troilkatt.fs;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Vector;
import org.apache.log4j.Logger;

/**
 * Functions similar to those found in the Python os.path module.
 *
 */
public class OsPath {
	/**
	 * Join two paths
	 * 
	 * @reutrn joined path
	 */
	public static String join(String a, String b) {
		if (a == null) {
			throw new RuntimeException("dir = null, file = " + b);
		}
		if (b == null) {
			throw new RuntimeException("file = null, dir = " + a);
		}
		
		a = a.replace("\\", "/");
		b = b.replace("\\", "/");
		
		if (a.endsWith(File.separator)) {
			return a + b;
		}
		else {
			return a + File.separator + b;
		}

	}

	/**
	 * Get basename of a filename.
	 * 
	 * @param filename
	 * @return basename
	 */
	public static String basename(String filename) {
		File f = new File(filename);
		return f.getName();
	}
	
	/**
	 * Get directory of a filename.
	 * 
	 * @param filename
	 * @return basename
	 */
	public static String dirname(String filename) {
		File f = new File(filename);
		return f.getParent();
	}
	
	/**
	 * Get last extension in a filename
	 * 
	 * @param filename
	 * @return last extension (filename content after the last "."), or null
	 * if filename could not be split.
	 */
	public static String getLastExtension(String filename) {
		if (filename == null) {
			return null;
		}
		if (filename.isEmpty()) {
			return null;
		}
		
		String[] parts = filename.split("\\.");
		if (parts.length == 0) {
			return null;
		}
		else {
			return parts[parts.length - 1];
		}
	}
	
	/**
	 * Remove the last extension in a filename
	 * 
	 * @param filename
	 * @return filename with last extension removed, or null if the 
	 * filename is invalid
	 */
	public static String removeLastExtension(String filename) {
		if (filename == null) {
			return null;
		}
		
		if (filename.isEmpty()) {
			return null;
		}
		
		String[] parts = filename.split("\\.");
		if (parts.length < 2) {
			// no extension in filename
			return null;
		}
		
				
		String newFilename = parts[0];
		for (int i = 1; i < parts.length - 1; i++) { // do not inclue last extension
			newFilename = newFilename + "." + parts[i];
		}
		return newFilename;			
	}
	
	/**
	 * Replace last extension in a filename
	 * 
	 * @param filename
	 * @param newExt new extension (not including '.' (dot))
	 * @return filename with last extension replaced, or null if the 
	 * filename is invalid
	 */
	public static String replaceLastExtension(String filename, String newExt) {
		if ((filename == null) || (newExt == null)) {
			return null;
		}
		
		if (filename.isEmpty()) {
			return null;
		}
		
		String[] parts = filename.split("\\.");
		
		String newFilename = parts[0];
		for (int i = 1; i < parts.length - 1; i++) {
			newFilename = newFilename + "." + parts[i];
		}
		return newFilename + "." + newExt;			
	}

	/**
	 * Tests whether the given file is a normal file.
	 *  
	 * @param filename: filename .
	 * @return true if a normal file, false otherwise.
	 */
	public static boolean isfile(String filename) {
		return (new File(filename)).isFile();
	}
	
	/**
	 * Tests whether the given file is a directory.
	 *  
	 * @param dirname: directory name.
	 * @return true if a directory, false otherwise.
	 */
	public static boolean isdir(String dirname) {
		return (new File(dirname)).isDirectory();
	}

	/**
	 * Get filesize
	 * 
	 * @param filename
	 * @retun file size in bytes
	 */
	public static long fileSize(String filename) {
		File f = new File(filename);
		return f.length();
	}
	
	/**
	 * Rename a file.
	 * 
	 * @param oldName: old filename
	 * @param newName new filename
	 * 
	 * @return: true on success, false otherwise.
	 */
	public static boolean rename(String oldName, String newName) {
		File oldFile = new File(oldName);		
		
		boolean rv = oldFile.renameTo(new File(newName));
		
		/*
		 * Rename (link) can fail cross-device. In that case attempt to copy
		 * and then remove the file.
		 */
		if (rv == false) {
			if (OsPath.copy(oldName, newName)) { // success
				OsPath.delete(oldName);
				return true;
			}
			else {
				return false; // copy also failed
			}
		}
		else {
			return true;
		}
	}

	/**
	 * Create a new directory if ti does not already exist
	 * 
	 * @param dirName: directory name.
	 * @param logger: logger instance for warnings about existing directories. If null there is 
	 * no logging (default)
	 * @return: true on success, including if the directory already exist. False otherwise.
	 */
	public static boolean mkdir(String dirName, Logger logger) {
		File f = new File(dirName);
		
		if (! f.isDirectory()) {
			return f.mkdirs();
		}
		else {
			if (logger != null) {
				logger.warn("Directory already exists: " + dirName);
			}
			return true;
		}
	}
	
	/**
	 * Create a new directory if ti does not already exist
	 * 
	 * @param dirName: directory name.
	 * @return: true on success, including if the directory already exist. False otherwise.
	 */
	public static boolean mkdir(String dirName) {
		return mkdir(dirName, null);
	}
	
	/**
	 * Remove a directory
	 * 
	 * @param dirName: empty directory to delete
	 * 
	 * @return: true on success.
	 */
	public static boolean rmdir(String dirName) {
		File f = new File(dirName);
		return f.delete();
	}
	
	/**
	 * Recursive directory remove. 
	 * 
	 * @param dirName: empty directory to delete
	 * 
	 * @return: true on success.
	 */
	public static boolean rmdirR(String dirName) {
		File dir = new File(dirName);
		
		if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = rmdirR(new File(dir, children[i]).getAbsolutePath());
                if (!success) {
                    return false;
                }
            }
        }
    
        // The directory is now empty so delete it
        return dir.delete();
	}	

	/**
	 * Normalize a pathname. This collapses redundant separators and up-level references so that
	 *  A//B, A/./B and A/foo/../B all become A/B. 
	 * 
	 * @param path
	 * @return	 
	 */
	public static String normPath(String path, Logger logger) {
		try {
			return (new File(path)).getCanonicalPath();
		} catch (IOException e) {
			if (logger != null) {
				logger.fatal("Failed to normalize path: " + path, e);
			}
			throw new RuntimeException("Invalid path: " + path);
		}
	}
	
	public static String normPath(String path) {
		return normPath(path, null);
	}

	/**
	 * Copy a file
	 * 
	 * @param src file to copy
	 * @param dst destination filename
	 * @param logger optional logger. If null no logger messages are written
	 * @return true if file was successfully copied, false otherwise
	 */
	public static boolean copy(String src, String dst, Logger logger) {
		boolean rv = true;
		
		try {
			FileChannel in = (new FileInputStream(src)).getChannel();
			FileChannel out = (new FileOutputStream(dst)).getChannel();
			long transferedBytes = 0;
			long toTransfer = in.size();
			while (transferedBytes < toTransfer) {
				transferedBytes += in.transferTo(transferedBytes, toTransfer - transferedBytes, out);				
			}				
			in.close();
			out.close();
		} catch (FileNotFoundException e) {		
			if (logger != null) {
				logger.error("File not found: " + src, e);
			}
			return false;
		} catch (IOException e) {
			if (logger != null) {
				logger.error("IOException: " + src, e);
			}
			return false;
		}
		
		return rv;
	}
	
	/**
	 * Copy a file without logger
	 * 
	 * @param src file to copy
	 * @param dst destination filename
	 * @return true if file was successfully copied, false otherwise
	 */
	public static boolean copy(String src, String dst) {
		return copy(src, dst, null);
	}

	/**
	 * Delete the file or directory.
	 * 
	 * @param filename: file or directory to delete
	 * @return: true if file was deleted, false otherwise
	 */
	public static boolean delete(String filename) {		
		File f = new File(filename);
		return f.delete();
	}
	
	/**
	 * Delete all files and sub-directories.
	 * 
	 * @param dirName directory with files to delete
	 * @return true if all files were deleted, false otherwise
	 */
	public static boolean deleteAll(String dirName) {
		File dir = new File(dirName);
			
		if (! dir.isDirectory()) {
			return false;
		}
		
		// Delete files and sub-directories in dir
		String[] children = dir.list();
		for (String c: children) {
			File child = new File(dir, c);
			if (child.isFile()) {
				if (child.delete() == false) {
					return false;
				}
			}
			else if (child.isDirectory()) {
				if(deleteAll(child.getAbsolutePath()) == false) {
					return false;
				}
			}
			else {
				return false;
			}
		}
		
		return dir.delete();
	}	
	
	/**
	 * Create list with absolute pathnames.
	 *
	 * @param dir: directory with files
	 * @param filenames: list with relative filesnames
	 *    
	 * @return: list with absolute filenames
	 */
	public static String[] relative2absolute(String dir, String [] files) {        
		String[] absFiles = new String[files.length];
		for (int i = 0; i < files.length; i++) {			
			absFiles[i] = (OsPath.join(dir, files[i]));
		}

		return absFiles;
	}
	
	/**
	 * Create list with absolute pathnames.
	 *
	 * @param dir: directory with files
	 * @param filenames: list with relative filesnames
	 *    
	 * @return: list with absolute filenames
	 */
	public static ArrayList<String> relative2absolute(String dir, ArrayList<String> files) {        
		ArrayList<String> absFiles = new  ArrayList<String>();		
		for (String f: files) {
			absFiles.add(OsPath.join(dir,  f));
		}
		return absFiles;
	}
	
	/**
	 * Create an absolute pathname.
	 *
	 * @param dir: directory with file
	 * @param filename: relative filename
	 *    
	 * @return: absolute filename
	 */
	public static String relative2absolute(String dir, String file) {
		return OsPath.join(dir, file);		
	}
	
	/**
	 * Get a file path relative to a root directory
	 *
	 * @param absolute: absolute filename
	 * @param dir: directory to make filename relative to
	 *    
	 * @return: relative filename, or null if the absolute filename does not contain a file
	 * relative to dir
	 */
	public static String absolute2relative(String absolute, String dir) {		
		if (absolute.contains(dir) == false) {
			return null;
		}
		
		// Get the part of the filename that follows rootDir
		String relativeName = absolute.substring(absolute.indexOf(dir) + dir.length());
		if (relativeName.startsWith("/")) {
			relativeName = relativeName.substring(1);
		}

		return relativeName;
	}
	
	/**
	 * List the content of a directory on the local FS
	 * 
	 * @param dir directory to list
	 * @param logger Logger used to print error messages
	 * @return list of absolute filenames, or null if the directory could not be listed
	 */
	public static String[] listdir(String dir, Logger logger) {
		return listdir(dir, false, logger);
	}
	
	/**
	 * List the content of a directory on the local FS
	 * 
	 * @param dir directory to list
	 * @return list of absolute filenames, or null if the directory could not be listed
	 */
	public static String[] listdir(String dir) {
		return listdir(dir, false, null);
	}
	
	/**
	 * Do a recursive listing of a directory on the local FS
	 * 
	 * @param dir directory to list
	 * @param logger Logger used to print error messages
	 * @return list of absolute filenames, or null if the directory could not be listed
	 */
	public static String[] listdirR(String dir, Logger logger) {
		return listdir(dir, true, logger);
	}
	
	/**
	 * Do a recursive listing of a directory on the local FS
	 * 
	 * @param dir directory to list
	 * @return list of absolute filenames, or null if the directory could not be listed
	 */
	public static String[] listdirR(String dir) {
		return listdir(dir, true, null);
	}

	/**
	 * Calculate a SHA-1 digest for a file. 
	 *
	 * @param filename: absolute filename.
	 *
	 * @return: sha1 digest.
	 */
	//public String sha1file(String filename) {		
	//	try {
	//		logger.debug("Calcaulte SHA-1 finegrprint for file: " + filename);
	//		return Table.sha1file(filename, logger);
	//	} catch (IOException e) {
	//		logger.fatal("Sha1 calculation failed for file: " + filename);
	//		logger.fatal(e.toString());
	//		throw new RuntimeException(e);			
	//	}		
	//}
	
	/**
	 * Return a list of files on the local FS
	 * 
	 * @param dir directory to list
	 * @param recursive true if a recursive listing of files should be done
	 * @param logger Logger used to print error messages
	 * @return list of filenames, or null if the directory could not be listed
	 */
	private static String[] listdir(String dir, boolean recursive, Logger logger) {
		FileFilter fileFilter = new FileFilter() {
			public boolean accept(File file) {				
				//if (! file.isFile()) {
				//	return false;
				//}
				
				// Ignore hidden files
				if (file.isHidden()) {
					return false;
				}
				
				return true;
			}
		};
		
		FileFilter recursiveFileFilter = new FileFilter() {
			public boolean accept(File file) {								
				// Ignore hidden files
				if (file.isHidden()) {
					return false;
				}
				
				return true;
			}
		};
	
		File dirFile = new File(dir);
		File files[];
		if (! recursive) {
			files = dirFile.listFiles(fileFilter);
		}
		else {
			files = dirFile.listFiles(recursiveFileFilter);
		}
	
		if (files == null) {
			if (logger != null) {
				logger.fatal("Directory listing failed for directory: " + dir);				
			}
			return null;
		}
				
		Vector<String> filenames = new Vector<String>(files.length);
		for (int i = 0; i < files.length; i++) {
			
			String fullFilename = OsPath.join(dir, files[i].getName());
			File fullFilePath = new File(fullFilename); 
			if (recursive && fullFilePath.isDirectory()) {
				String[] subdirFiles = listdir(fullFilename, recursive, logger);
				for (String f: subdirFiles) {
					filenames.add(f);
				}
			}
			else {			
				filenames.add(OsPath.join(dir, files[i].getName()));				
			}
		}		
		return filenames.toArray(new String[filenames.size()]);
	}
	
	/**
	 * Check if a list of filepaths (directory and filenames) contains a file.
	 * 
	 * @param files list of filenames
	 * @param filename basename of file to check
	 * @param partialMatch true if partial matches are accepted (on of the filenames contain 
	 * the input string).
	 * @return true if the filename was found in the list, false otherwise
	 */
	public static boolean fileInList(ArrayList<String> files, String filename, boolean partialMatch) {
		for (String f: files) {
			String basename = OsPath.basename(f);
			if (partialMatch) {
				if (basename.contains(filename)) {
					return true;
				}
			}
			else {
				if (basename.equals(filename)) {
					return true;
				}
			}
		}
		
		return false; // match not found
	}
	
	/**
	 * Check if a list of filepaths (directory and filenames) contains a file.
	 * 
	 * @param files list of filenames
	 * @param filename basename of file to check
	 * @param partialMatch true if partial matches are accepted (on of the filenames contain 
	 * the input string).
	 * @return true if the filename was found in the list, false otherwise
	 */
	public static boolean fileInList(String[] files, String filename, boolean partialMatch) {
		for (String f: files) {
			String basename = OsPath.basename(f);
			if (partialMatch) {
				if (basename.contains(filename)) {
					return true;
				}
			}
			else {
				if (basename.equals(filename)) {
					return true;
				}
			}
		}
		
		return false; // match not found
	}
}
