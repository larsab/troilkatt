package edu.princeton.function.troilkatt;

import java.io.IOException;

import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Test superclass for adding commonly used constants and functions.
 */
public class TestSuperNFS extends TestSuper {

	/**
	 * Initialized NFS test directories.
	 * 
	 * @throws IOException
	 */	
	public static void initNFSTestDir() throws IOException  {		
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
	}

}
