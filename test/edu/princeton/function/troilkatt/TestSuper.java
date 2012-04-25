package edu.princeton.function.troilkatt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Test superclass for adding commonly used constants and functions.
 */
public class TestSuper {
	public static final String[] linesOfText = {
		"The Rum Tum Tugger is a Curious Cat:",
		"If you offer him pheasant he would rather have grouse.",
		"If you put him in a house he would much prefer a flat,",
		"If you put him in a flat then he'd rather have a house.",
		"If you set him on a mouse then he only wants a rat,",
		"If you set him on a rat then he'd rather chase a mouse."};
	
	// Directory with configuration files used for the unit testing
	protected static final String dataDir = "/home/larsab/troilkatt2/test-data";
	// Temporary directory for storing files created during the unit tests
	protected static final String tmpDir = "/home/larsab/troilkatt2/test-tmp/data";
	protected static final String logDir = "/home/larsab/troilkatt2/test-tmp/log";
	protected static final String outDir = "/home/larsab/troilkatt2/test-tmp/output";
	// Configuration file 
	protected static final String configurationFile = "unitConfig.xml";
	// Log4j properties file
	protected static final String logProperties = "log4j-unit.properties";
	
	// Test root dir in HDFS
	public static String hdfsRoot = "/users/larsab/troilkatt/test/troilkattFS";
	
	/**
	 * Compare two files
	 * 
	 * @param file1 filename of first file
	 * @param file2 filename of second file
	 * @return true if the files are identical, false otherwise
	 * @throws IOException 
	 */
	public boolean fileCmp(String file1, String file2) throws IOException {
		BufferedReader ib1 = new BufferedReader(new FileReader(file1));
		BufferedReader ib2 = new BufferedReader(new FileReader(file2));

		while (true) {
			String str1 = ib1.readLine();
			String str2 = ib2.readLine();
			
			if ((str1 == null) || (str2 == null)) {
				if ((str1 == null) && (str2 == null)) { 
					return true;
				}
				else {
					return false;
				}
			}
			
			if (str1.equals(str2) == false) {
				return false;
			}
		}
	}

	/**
	 * Initialized HDFS test directories.
	 * 
	 * @throws IOException
	 */
	public static void initTestDir() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());		
		
		//OsPath.mkdir(OsPath.join(dataDir, "files"));
		//FSUtils.writeTextFile(OsPath.join(dataDir, "files/file1"), linesOfText);
		//FSUtils.writeTextFile(OsPath.join(dataDir, "files/file2"), linesOfText);
		//FSUtils.writeTextFile(OsPath.join(dataDir, "files/file3"), linesOfText);
		
		OsPath.mkdir(dataDir);
		OsPath.mkdir(tmpDir);
		OsPath.mkdir(logDir);
		
		// Directory listing tests (localFS)			
		String src = OsPath.join(dataDir, "files/file1");
		OsPath.mkdir(OsPath.join(dataDir, "ls"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/file1"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/file2"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/file3"));
		OsPath.mkdir(OsPath.join(dataDir, "ls/subdir1"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/subdir1/file4"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/subdir1/file5"));
		OsPath.mkdir(OsPath.join(dataDir, "ls/subdir2"));
		OsPath.copy(src, OsPath.join(dataDir, "ls/subdir2/file6"));
		OsPath.mkdir(OsPath.join(dataDir, "ls/subdir3"));
		
		// Directory listing tests (HDFS)
		Path srcFile = new Path(OsPath.join(dataDir, configurationFile));
		fs.delete(new Path(hdfsRoot), true);
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file3")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir1/file4")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir1/file5")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir2/file6")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir3")));
		
		// Timestamp tests
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.1.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.2.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.3.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file2.1.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file2.3.bz2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file3.1.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file3.2.none")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.1.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.2.bz2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.3.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file5.2.none")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir2/file6.2.none")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir3")));
		
		// Timestamped directory tests
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/1.tar.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/2.tar.bz2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/invalid")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd/3.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/4.zip")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd-empty")));
		
		// Compression tests (files)
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed-files")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.1.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.1.gz")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.2.bz2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.2.bz2")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.3.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.3.zip")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.4.none")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.4.none")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/invalid.5.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.5.gz")));
		
		// Compression tests (directories)
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed-dirs")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/1.tar.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/1.tar.gz")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/2.tar.bz2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/3.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/3.zip")));
		fs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed/4.none")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/4.none/file1")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/4.none/file1")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/4.none/file2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/4.none/file2")));
		// Invalid zip file
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/5.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/5.zip")));
		
		// Stage unit tests
		String nr = "troilkatt/data/test/input";
		fs.delete(new Path(nr), true);
		fs.mkdirs(new Path(OsPath.join(nr, "")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file1.1.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file2.1.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file3.2.none")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/file4.1.none")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/file5.2.none")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir2/file6.2.none")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir3")));
		
		// Stage unit tests
		nr = "troilkatt/meta/unitPipeline/003-unitStage/1.none";
		fs.delete(new Path("troilkatt/meta/unitPipeline/003-unitStage/"), true);
		fs.mkdirs(new Path(OsPath.join(nr, "")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta3")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir1")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/meta4")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/meta5")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir2")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir2/meta6")));
		fs.mkdirs(new Path(OsPath.join(nr, "subdir3")));
		
		// Mapreduce unit tests
		nr = "troilkatt/data/test/mapreduce/input";
		fs.delete(new Path(nr), true);
		fs.mkdirs(new Path(nr));
		srcFile = new Path(OsPath.join(dataDir, "files/file3.1.gz"));
		// Set one consists of files file[1,2,3].1.gz and file4.1.none
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file1.1.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file2.1.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file3.1.gz")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.4.none")),
				new Path(OsPath.join(nr, "file4.1.none")));
		// Set two consists of files file5.bz2 and file6.zip
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.2.bz2")),
				new Path(OsPath.join(nr, "file5.2.bz2")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.3.zip")),
				new Path(OsPath.join(nr, "file6.2.zip")));
		// Set three consist of the invalid file file7.3.gz
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/invalid.5.gz")),
				new Path(OsPath.join(nr, "invalid.5.gz")));
		// Set four consists of SOFT files
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.gz")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.gz")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.bz2")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.bz2")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.none")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.none")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.bz2")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.bz2")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.gz")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.gz")));
		fs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.none")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.none")));
	}

}
