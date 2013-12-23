package edu.princeton.function.troilkatt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.princeton.function.troilkatt.fs.FSUtils;
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
	protected static final String configurationFileNFS = "unitConfigNFS.xml";
	// Log4j properties file
	protected static final String logProperties = "log4j-unit.properties";
	
	// Test root dir in HDFS
	public static String hdfsRoot = "/user/larsab/troilkatt/test/troilkattFS";
	
	// Test root dir in NFS (or other POSIC system)
	public static String nfsRoot = "/home/larsab/troilkatt/test/troilkattFS/";
	
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
					ib1.close();
					ib2.close();
					return true;
				}
				else {
					ib1.close();
					ib2.close();
					System.err.println(str1 + " or " + str2 + " is null");
					return false;
				}
			}
			
			if (str1.equals(str2) == false) {
				ib1.close();
				ib2.close();
				System.err.println(str1 + " != " + str2);
				return false;
			}
		}		
	}

	/**
	 * Initialized HDFS test directories.
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void initTestDir() throws IOException, InterruptedException {
		FileSystem hdfs = FileSystem.get(new Configuration());		
		
		OsPath.mkdir(OsPath.join(dataDir, "files"));
		//FSUtils.writeTextFile(OsPath.join(dataDir, "files/file1"), linesOfText);
		FSUtils.writeTextFile(OsPath.join(dataDir, "files/file2"), linesOfText);
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
		hdfs.delete(new Path(hdfsRoot), true);
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/file3")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir1/file4")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir1/file5")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ls/subdir2/file6")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ls/subdir3")));
		
		// Timestamp tests
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.1.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.2.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file1.3.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file2.1.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file2.3.bz2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file3.1.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/file3.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.1.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.2.bz2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file4.3.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir1/file5.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "ts/subdir2/file6.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "ts/subdir3")));
		
		// Timestamped directory tests
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/1.tar.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/2.tar.bz2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/invalid")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd/3.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(hdfsRoot, "tsd/4.zip")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "tsd-empty")));
		
		// Compression tests (files)
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed-files")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.1.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.1.gz")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.2.bz2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.2.bz2")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.3.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.3.zip")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.4.none")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.4.none")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/invalid.5.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-files/file1.5.gz")));
		
		// Compression tests (directories)
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed-dirs")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/1.tar.gz")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/1.tar.gz")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/2.tar.bz2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/3.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/3.zip")));
		hdfs.mkdirs(new Path(OsPath.join(hdfsRoot, "compressed-dirs/4.none")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/4.none/file1")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/4.none/file1")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/4.none/file2")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/4.none/file2")));
		// Invalid zip file
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "dirs/5.zip")), 
				new Path(OsPath.join(hdfsRoot, "compressed-dirs/5.zip")));
		
		// Directory listing tests (NFS)
		String nfsSrcFile = OsPath.join(dataDir, configurationFile);
		OsPath.deleteAll(nfsRoot);
		OsPath.mkdir(OsPath.join(nfsRoot, "ls"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/file1"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/file2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/file3"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ls/subdir1"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/subdir1/file4"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/subdir1/file5"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ls/subdir2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ls/subdir2/file6"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ls/subdir3"));
				
		// Timestamp tests (NFS)
		OsPath.mkdir(OsPath.join(nfsRoot, "ts"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file1.1.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file1.2.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file1.3.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file2.1.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file2.3.bz2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file3.1.none"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/file3.2.none"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ts/subdir1"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/subdir1/file4.1.none"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/subdir1/file4.2.bz2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/subdir1/file4.3.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/subdir1/file5.2.none"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ts/subdir2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "ts/subdir2/file6.2.none"));
		OsPath.mkdir(OsPath.join(nfsRoot, "ts/subdir3"));

		// Timestamped directory tests (NFS)
		OsPath.mkdir(OsPath.join(nfsRoot, "tsd"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "tsd/1.tar.gz"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "tsd/2.tar.bz2"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "tsd/invalid"));
		OsPath.mkdir(OsPath.join(nfsRoot, "tsd/3.none"));
		OsPath.copy(nfsSrcFile, OsPath.join(nfsRoot, "tsd/4.zip"));
		OsPath.mkdir(OsPath.join(nfsRoot, "tsd-empty"));

		// Compression tests (files, NFS)
		OsPath.mkdir(OsPath.join(nfsRoot, "compressed-files"));
		OsPath.copy(OsPath.join(dataDir, "files/file1.1.gz"), 
				OsPath.join(nfsRoot, "compressed-files/file1.1.gz"));
		OsPath.copy(OsPath.join(dataDir, "files/file1.2.bz2"), 
				OsPath.join(nfsRoot, "compressed-files/file1.2.bz2"));
		OsPath.copy(OsPath.join(dataDir, "files/file1.3.zip"), 
				OsPath.join(nfsRoot, "compressed-files/file1.3.zip"));
		OsPath.copy(OsPath.join(dataDir, "files/file1.4.none"), 
				OsPath.join(nfsRoot, "compressed-files/file1.4.none"));
		OsPath.copy(OsPath.join(dataDir, "files/file1.4.none"), 
				OsPath.join(nfsRoot, "compressed-files/file1.4.non"));
		OsPath.copy(OsPath.join(dataDir, "files/invalid.5.gz"), 
				OsPath.join(nfsRoot, "compressed-files/file1.5.gz"));

		// Compression tests (directories, NFS)
		OsPath.mkdir(OsPath.join(nfsRoot, "compressed-dirs"));
		OsPath.copy(OsPath.join(dataDir, "dirs/1.tar.gz"), 
		OsPath.join(nfsRoot, "compressed-dirs/1.tar.gz"));
		OsPath.copy(OsPath.join(dataDir, "dirs/2.tar.bz2"), 
		OsPath.join(nfsRoot, "compressed-dirs/2.tar.bz2"));
		OsPath.copy(OsPath.join(dataDir, "dirs/3.zip"), 
		OsPath.join(nfsRoot, "compressed-dirs/3.zip"));
		OsPath.mkdir(OsPath.join(nfsRoot, "compressed-dirs/4.none"));
		OsPath.copy(OsPath.join(dataDir, "dirs/4.none/file1"), 
		OsPath.join(nfsRoot, "compressed-dirs/4.none/file1"));
		OsPath.copy(OsPath.join(dataDir, "dirs/4.none/file2"), 
		OsPath.join(nfsRoot, "compressed-dirs/4.none/file2"));
		// Invalid zip file
		OsPath.copy(OsPath.join(dataDir, "dirs/5.zip"), 
		OsPath.join(nfsRoot, "compressed-dirs/5.zip"));
		
		// Stage unit tests
		String nr = "troilkatt/data/test/input";
		hdfs.delete(new Path(nr), true);
		hdfs.mkdirs(new Path(OsPath.join(nr, "")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file1.1.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file2.1.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file3.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/file4.1.none")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/file5.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir2/file6.2.none")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir3")));
		
		// Stage unit tests
		nr = "troilkatt/meta/unitPipeline/003-unitStage/1.none";
		hdfs.delete(new Path("troilkatt/meta/unitPipeline/003-unitStage/"), true);
		hdfs.mkdirs(new Path(OsPath.join(nr, "")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "meta3")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir1")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/meta4")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir1/meta5")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir2")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "subdir2/meta6")));
		hdfs.mkdirs(new Path(OsPath.join(nr, "subdir3")));
		
		// Mapreduce unit tests
		nr = "troilkatt/data/test/mapreduce/input";
		hdfs.delete(new Path(nr), true);
		hdfs.mkdirs(new Path(nr));
		srcFile = new Path(OsPath.join(dataDir, "files/file3.1.gz"));
		// Set one consists of files file[1,2,3].1.gz and file4.1.none
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file1.1.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file2.1.gz")));
		hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(nr, "file3.1.gz")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.4.none")),
				new Path(OsPath.join(nr, "file4.1.none")));
		// Set two consists of files file5.bz2 and file6.zip
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.2.bz2")),
				new Path(OsPath.join(nr, "file5.2.bz2")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/file1.3.zip")),
				new Path(OsPath.join(nr, "file6.2.zip")));
		// Set three consist of the invalid file file7.3.gz
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/invalid.5.gz")),
				new Path(OsPath.join(nr, "invalid.5.gz")));
		// Set four consists of SOFT files
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.gz")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.gz")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.bz2")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.bz2")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GSE8070_family.soft.6.none")),
				new Path(OsPath.join(nr, "GSE8070_family.soft.6.none")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.bz2")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.bz2")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.gz")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.gz")));
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/GDS2949_full.soft.6.none")),
				new Path(OsPath.join(nr, "GDS2949_full.soft.6.none")));
	}

}
