package trash;

import java.util.ArrayList;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.util.Bytes;

import edu.princeton.function.troilkatt.fs.TroilkattFile;

public class TroilkattFile {
	

	/**
	 * Convert a list of TroilkattFile objects to a list of filenames.
	 *
	 * @param  tkList list with TroilkattFile objects.
	 *
	 * @return list with (relative) filenames
	 */
	public static String[] troilkattfiles2localnames(TroilkattFile[] tkList) {
		if (tkList == null) {
			return null;
		}

		String filelist[] = new String[tkList.length];
		for (int i = 0; i < tkList.length; i++) {
			filelist[i] = tkList[i].getFilename();
		}
		return filelist;
	}
	
	
	/**
	 * Convert a list of TroilkattFile objects to a list of HDFS-filenames in String format.
	 * 
	 * @param  hbaseFilelist list with HbaseFile objects.
	 *
	 * @return list with Hbase-filenames
	 */
	public static String[] troilkattfiles2hdfsnames(TroilkattFile[] tkList) {
		if (tkList == null) {
			return null;
		}

		String filelist[] = new String[tkList.length];
		for (int i = 0; i < tkList.length; i++) {
			filelist[i] = tkList[i].getHDFSFilename();
		}
		return filelist;
	}
	
	/**
	 * Convert a list of HDFS filenames to an array of TroilkattFile objects.
	 * 
	 * @param strList: string list with HDFS-filenames to convert.
	 * @param hdfsDir: root directory in HDFS
	 * @param localDir: directory containing file on local filesystem
	 * @param logger: logger instanse.
	 * 
	 * @return list of HbaseFile objects.
	 */
	public static ArrayList<TroilkattFile> strlist2troilkattfilearray(String [] strList, String hdfsDir, String localDir, Logger logger) {
		ArrayList<TroilkattFile> hbfArray = new ArrayList<TroilkattFile>(strList.length);
		for (String f: strList) {			
			if (f.equals("")) {
				logger.warn("Ignoring empty line in filelist");
				continue;
			}
			
			hbfArray.add( new TroilkattFile(f, hdfsDir, localDir, logger) );			
		}
		
		return hbfArray;
	}
	 
	/**
	 * Convert a list of TroilkattFile objects to a list of Strings with the HDFS filenames.
	 * 
	 * @param hbfList: list of HbaseFile objects.
	 * @return: list of String objects (with HDFS filenames)
	 */
	public static String[] troilkattfilearray2strlist(ArrayList<TroilkattFile> tkList) {
		String [] strList = new String[tkList.size()];
		
		for (int i = 0; i < tkList.size(); i++) {
			strList[i] = tkList.get(i).getHDFSFilename();
		}
		
		return strList;
	}
	
	/**
	 * Convert a list of TroilkattFile object to a string list with HDFS filenames
	 * 
	 * @param tkList
	 * @return
	 */
	public static String[] troilkattfiles2strlist(TroilkattFile [] tkList) {
		String [] strList = new String[tkList.length];
		
		for (int i = 0; i < tkList.length; i++) {
			strList[i] = tkList[i].getHDFSFilename();
		}
		
		return strList;
	}		
	
	/**
	 * Convert an array of TroilkattFile objects to a list of Strings with the filenames in the 
	 * HbaseFile representation, then into a string where each file is on a seperate line.
	 * 
	 * @param tkList: list of TroilkattFile objects.
	 * @return: byte array as described above.
	 */
	public static String troilkattfilearray2str(ArrayList<TroilkattFile> tkList) {		
		String s = "";
		for (TroilkattFile hbf: tkList) {
			s = s + hbf.getHDFSFilename() + "\n";
		}
		
		return s;
	}
	
	/**
	 * Convert a list of TroilkattFile objects to a string where is line contains the
	 * HDFS filename of the file.
	 * 
	 * @param tkList
	 * @return
	 */
	public static String troilkattfiles2str(TroilkattFile[] tkList) {		
		String s = "";
		for (TroilkattFile hbf: tkList) {
			s = s + hbf.getHDFSFilename() + "\n";
		}
		
		return s;
	}
	
	/**
	 * Convert a list of TroilkattFile objects to a list of Strings with the HDFS filenames,  
	 * then into a string where each file is on a seperate line and then the string to bytes.
	 * 
	 * @param hbfList: list of HbaseFile objects.
	 * @return: byte array as described above.
	 */
	public static byte[] troilkattfilearray2bytes(ArrayList<TroilkattFile> tkList) {
		return Bytes.toBytes( TroilkattFile.troilkattfilearray2str(tkList) );		
	}
	
	/**
	 * Convert a list of TroilkattFile objects to byte array that contains a string that contains
	 * lines with HDFS filenames.
	 * @param tkList
	 * @return
	 */
	public static byte[] troilkattfiles2bytes(TroilkattFile[] tkList) {
		return Bytes.toBytes( TroilkattFile.troilkattfiles2str(tkList) );		
	}
	
	/**
	 * Create list with absolute pathnames.
	 *
	 * @param files: array with TroilkattFile objects
	 *    
	 * @return: list with absolute filenames
	 */
	public static String[] getAbsoluteFilenames(ArrayList<TroilkattFile> files) {        
		String[] absFiles = new String[files.size()];
				
		for (int i = 0; i < files.size(); i++) {			
			absFiles[i] = files.get(i).getAbsoluteFilename();
		}

		return absFiles;
	}
	
	/**
	 * Create list with realative filenames.
	 *
	 * @param files: array with TroilkattFile objects
	 *    
	 * @return: list with relative filenames
	 */
	public static String[] getRelativeFilenames(ArrayList<TroilkattFile> files) {        
		String[] relFiles = new String[files.size()];
				
		for (int i = 0; i < files.size(); i++) {			
			relFiles[i] = files.get(i).getFilename();
		}

		return relFiles;
	}
	
	/**
	 * Create list with filenames.
	 * 
	 * @param files: array with TroilkattFile objects
	 * @param absoulteFilenames: true if absolute filenames should be returned.
	 *    
	 * @return: list with filenames
	 */
	public static String[] getFilenames(ArrayList<TroilkattFile> files, boolean absoluteFilenames) {
		if (absoluteFilenames) {
			return TroilkattFile.getAbsoluteFilenames(files);
		}
		else {
			return TroilkattFile.getRelativeFilenames(files);
		}
	}

	/**
	 * Remove timestamp and compression format from basename
	 * 
	 * @param HDFSBasename: string in the following format: filename.timestamp.compression
	 * @param logger: logger
	 * @return basename with timestamp and compression removed	 
	 */
	public static String stripBasename(String HDFSBasename, Logger logger) {
		
		int compressionDot = HDFSBasename.lastIndexOf('.');
		if (compressionDot == -1) {
			logger.fatal("HDFS filename does not have compression substring: " + HDFSBasename);
			throw new RuntimeException("HDFS filename does not have compression substring: " + HDFSBasename);
		}		
		String strippedName = HDFSBasename.substring(0, compressionDot);		
		
		int timestampDot = strippedName.lastIndexOf('.');
		if (timestampDot == -1) {
			logger.fatal("HDFS filename does not have timestamp: " + HDFSBasename);
			throw new RuntimeException("HDFS filename does not have timestamp: " + HDFSBasename);
		}
			
		return strippedName.substring(0, timestampDot);		
	}

}
