package edu.princeton.function.troilkatt.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.TroilkattTable;
import edu.princeton.function.troilkatt.tools.FilenameUtils;


/**
 * Split raw file into platform specific files and remove overlapping samples. The overlap 
 * is calculated before running this tool. Also, meta-data defines how to split the file.
 */
public class SGERawRemoveOverlapping {
	
	/**
	 * Split a raw file into platform specific parts and remove overlapping samples. The 
	 * split information and overlapping samples to remove are read  from the MongoDB geoMeta 
	 * collection
	 * 
	 * @param inputFilename input raw filename
	 * @param outputDir output directory for platform specific output files
	 * @param tmpDir a directory that may be used to store temporary files.
	 * @param serverAdr MongoDB server IP address
	 * @param serverPort MongoDB server listen port
	 * @return none
	 */
	public static void process(String inputFilename, String outputDir, String tmpDir, String serverAdr, int serverPort) throws IOException {
		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
							
		String seriesID = FilenameUtils.getDsetID(inputFilename);
		HashMap<String, ArrayList<String>> sidp2gsm = getParts(coll, seriesID);
		if (sidp2gsm == null) {
			System.err.println("Could not find meta data about: " + seriesID);
			return;
		}
			
		/*
		 * Unpack raw file
		 */
		ArrayList<String> files = untarRaw(inputFilename, tmpDir); 
		if (files == null) {
			System.err.println("Could not untar raw file: " + inputFilename);				
			return;
		}

		/*
		 * Split CEL files into platform specific files, and remove overlapping
		 */
		Set<String> sidp = sidp2gsm.keySet();
		for (String s: sidp) { // for each platform			
			// Get sample IDs for this platform
			ArrayList<String> gsms = sidp2gsm.get(s);
			
			if (gsms.isEmpty()) {
				System.err.println("No input files for: " + s);
				continue;
			}		
			if (gsms.get(0).equals("none")) {
				// All samples deleted due to overlap
				System.err.println("All samples deleted for: " + s);
				continue;
			}
			
			// Get list of files for each ID
			ArrayList<String> gsmFiles = new ArrayList<String>();
			for (String g: gsms) {
				ArrayList<String> gf = getRawFiles(g, files);
				gsmFiles.addAll(gf);
			}		
			
			if (gsmFiles.size() == 0) {
				System.err.println("No raw files found for: " + s);
				continue;
			}
			
			// Create new tar with sample specific files
			String outputTar = OsPath.join(outputDir, s + ".tar");
			int added = splitCelFiles(gsmFiles, outputTar);							
			if (added == -1) {
				System.err.println("Could not create tar file for series: " + s);					
				continue;
			}
			if (added == 0) {									
				System.err.println("No files added for series: " + s);
				OsPath.delete(outputTar);
			}							
			System.out.println("Added " + added + " files to " + outputTar);
		}			
	}
		
	/**
	 * Get platform specific parts for a series
	 *
	 * @param coll initialized MongoDB collection handle
	 * @param seriesID series ID without platform specifier
	 * @return hash map where the platform-specific series ID is used as key, and 
	 * the value consist of a list with sample IDs in that platform (with samples
	 * removed due to overlap)
	 */
	protected static HashMap<String, ArrayList<String>> getParts(DBCollection coll, String seriesID) {
		HashMap<String, ArrayList<String>> sidp2gsm = new HashMap<String, ArrayList<String>>();
		
		DBCursor cursor = coll.find(new BasicDBObject("meta:id", seriesID));
		cursor.limit(0);
		
		if (cursor.count() == 0) {
			System.err.println("No MongoDB entry for: " + seriesID);
			return null;
		}
		
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
		
		while(cursor.hasNext()) {
			DBObject entry = cursor.next();
		
			String sid = (String) entry.get("meta:id"); // without platform ID
			String sidWithPlatform = (String) entry.get("key");
			if (sid == null) {
				System.err.println("Could not find MongoDB id field for: " + seriesID);
				System.exit(-1);
			}
			
			if (sidp2gsm.containsKey(sidWithPlatform)) { // newer entry already added
				continue;
			}
			 
			String includedSamplesStr = (String) entry.get("calculated:sampleIDs-overlapRemoved");			
			if (includedSamplesStr == null) {
				System.err.println("Could not find MongoDB calculated:sampleIDs-overlapRemoved field for: " + seriesID);
				System.err.println("This is expected for datasets without any overlapping samples");						
			}
			else {
				ArrayList<String> includedSamples = TroilkattTable.string2array(includedSamplesStr);
				sidp2gsm.put(sidWithPlatform,  includedSamples);
				continue;
			}
			
			// No calculated overlap, so series has no overlapping samples
			String allSamplesStr = (String) entry.get("meta:sampleIDs");
			if (allSamplesStr == null) {
				System.err.println("Could not find MongoDB meta:sampleIDs field for: " + seriesID);
				System.exit(-1);
			}
			ArrayList<String> allSamples = TroilkattTable.string2array(allSamplesStr);
			sidp2gsm.put(sidWithPlatform, allSamples);			
		}

		return sidp2gsm;
	}
		
	/**
	 * Helper function to untar a Geo raw file
	 * 
	 * @param tarFilename of the tar file in the local file system
	 * @param dstDir destination directory
	 * @return list with unpacked files on success, null if the file could not be unpacked
	 */
	protected  static ArrayList<String> untarRaw(String tarFilename, String dstDir) {
		 ArrayList<String> files = new  ArrayList<String>();
		try {
			FileInputStream is = new FileInputStream(tarFilename);
			ArchiveInputStream ain = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
			
			final byte[] buffer = new byte[4096]; // use a 4KB buffer			
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
				files.add(outputFilename);
				FileOutputStream fos = new FileOutputStream(outputFilename);
				long fileSize = ae.getSize();
				long bytesRead = 0;
				while (bytesRead < fileSize) {
					int n = ain.read(buffer);
					if (n == -1) { // EOF
						break;
					}
					fos.write(buffer, 0, n);
					bytesRead += n;
				}
				fos.close();				
			}
		} catch (IOException e) {
			System.err.println("Could not unpack: " + tarFilename);
			e.printStackTrace();
			return null;
		} catch (ArchiveException e) {
			System.err.println("Could not unpack: " + tarFilename);
			e.printStackTrace();
			return null;
		}
		return files;
	}

	/**
	 * Helper function to get all input files files that belong to a specific sample 
	 * 
	 * @param gsmID sample ID
	 * @param srcDir directory with input files
	 * @return list of raw files that belong to this sample
	 */
	protected static ArrayList<String> getRawFiles(String gsmID, ArrayList<String> inputFiles) {
		ArrayList<String> gsmFiles = new ArrayList<String>();		

		for (String f: inputFiles) {
			String basename = OsPath.basename(f);
			// Convert everything to lower case to make matching simpler
			basename = basename.toLowerCase();
			gsmID = gsmID.toLowerCase();

			if (basename.startsWith(gsmID) && 
					((basename.contains(".cel.") || basename.endsWith(".cel")))) { 
				// Is a Affymetrix CEL file that can be processed later
				gsmFiles.add(f);
			}
		}

		return gsmFiles;
	}		

	/**
	 * Split CEL files 
	 * 
	 * @param inputFiles list of all input files in source tar. The files that corresponds
	 * to entries in gsms are added to the output file
	 * @param outputTar filename for output tar file
	 * @return number of samples added to tar file. Zero if there are no samples to add
	 * or if all have been deleted due to overlap. -1 if an output tar file could not
	 * be created.
	 */
	protected static int splitCelFiles(ArrayList<String> inputFiles, String outputTar) {
		int nAdded = 0;
				

		try {
			OutputStream os = new FileOutputStream(outputTar);
			ArchiveOutputStream aos = new ArchiveStreamFactory().createArchiveOutputStream("tar", os);
			
			final byte[] buffer = new byte[4096]; // use a 4KB buffer
			for (String f: inputFiles) {
				String arName = OsPath.basename(f);				
				InputStream is = new FileInputStream(f);									
							
				ArchiveEntry ar = new TarArchiveEntry(new File(f), arName);
				aos.putArchiveEntry(ar);	
								
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
				
				nAdded++;
			} // for input files
		
			aos.close();
		} catch (IOException e) {
			System.err.println("Could not create output archive");
			e.printStackTrace();
			OsPath.delete(outputTar);
			return -1;
		} catch (ArchiveException e) {
			System.err.println("Could not create output archive");
			e.printStackTrace();
			OsPath.delete(outputTar);
			return -1;
		}
		return nAdded;
	} 

	
	

	/**
	 * @param args [0] input filename
	 *             [1] output directory
	 *             [2] tmp directory, used to store unpacked files
	 *             [3] mongoDB server IP address
	 *             [4] mongoDB server listen port
	 */
	public static void main(String[] args) throws Exception {			
		if (args.length < 5) {
			System.err.println("Usage: java SGERawRemoveOverlapping inputFilename outputDir tmpDir mongoDBServerIP mongoDBServerPort");
			System.exit(2);
		}
		
		String inputFilename = args[0];
		String outputDir = args[1];
		String tmpDir = args[2];
		String serverAdr = args[3];	 
		int serverPort = Integer.valueOf(args[4]);
		
		process(inputFilename, outputDir, tmpDir, serverAdr, serverPort);
	}

}
