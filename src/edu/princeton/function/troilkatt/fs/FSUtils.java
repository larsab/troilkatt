package edu.princeton.function.troilkatt.fs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Vector;

public class FSUtils {
	/**
	 * Open, read all data, and close a file.
	 *	    
	 * @param filename
	 * 
	 * @return file data
	 * @throws IOException 
	 */
	static public byte[] readFile(String filename) throws IOException {
		File file = new File(filename);
		InputStream is = new FileInputStream(file);

		// Make sure the file is smaller than Integer,MAX_VALUE!
		long length = file.length();
		if (length > Integer.MAX_VALUE) {
			throw new IOException("File is too large (> Integer.MAX_VALUE)");
		}        

		byte[] bytes = new byte[(int)length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
			offset += numRead;
		}

		// Ensure all the bytes have been read in
		if (offset < bytes.length) {
			throw new IOException("Could not completely read file "+ file.getName());
		}
 
		is.close();
				
		return bytes;
	}

	/**
	 * Read lines from a text file
	 *	    
	 * @param filename
	 * @return lines as a list of strings. Newlines are not included.
	 * @throws IOException 
	 */
	static public String[] readTextFile(String filename) throws IOException {
		Vector<String> lines = new Vector<String>();		
		BufferedReader ib = new BufferedReader(new FileReader(filename));

		String str;
		while ((str = ib.readLine()) != null) {
			lines.add(str);
		}
		
		return lines.toArray(new String[lines.size()]);
	}

	/**
	 * Create, write all data, and close a text file.
	 *
	 * @param filename of file to create
	 * @param lines to write to file. Note! a newline is added to each line.
	 * @throws IOException 
	 */
	static public void writeTextFile(String filename, String[] lines) throws IOException {
		File file = new File(filename);
		BufferedWriter os = new BufferedWriter(new FileWriter(file));
		for (String s: lines) {
			os.write(s + "\n");
		}
		os.close();
	}

	/**
	 * Create, write all data, and close a text file.
	 *
	 * @param filename of file to create
	 * @param lines to write to file. Note! a newline is added to each line.
	 * @throws IOException 
	 * @throws IOException 
	 */
	public static void writeTextFile(String filename, ArrayList<String> lines) throws IOException {
		File file = new File(filename);
		BufferedWriter os = new BufferedWriter(new FileWriter(file));
		for (String s: lines) {
			os.write(s + "\n");
		}
		os.close();
	}
	
	/**
	 * Open, append data, and close a text file.
	 *
	 * @param filename of file to create
	 * @param lines to write to file. Note! a newline is added to each line.
	 * @throws IOException 
	 * @throws IOException 
	 */
	public static void appendTextFile(String filename, ArrayList<String> lines) throws IOException {
		File file = new File(filename);
		BufferedWriter os = new BufferedWriter(new FileWriter(file, true));
		for (String s: lines) {
			os.write(s + "\n");
		}
		os.close();
	}

	/**
	 * Create, write all data, and close a text file.
	 *
	 * @param filename
	 * @throws IOException 
	 */
	static public void writeFile(String filename, byte[] data) throws IOException {
		File file = new File(filename);
		OutputStream os = new FileOutputStream(file);
		os.write(data, 0, data.length);		 
		os.close();
	}
	
	/**
	 * Calculate a SHA-1 digest for a file. 
	 *
	 * @param filename: absolute filename
	 *
	 * @return: sha1 digest
	 * @throws IOException 
	 */
	public static String sha1file(String filename) throws IOException {		 	
		final int BLOCKSIZE = 4096;
		String hash = "";
		byte bytes[] = new byte[BLOCKSIZE];

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {			
			throw new RuntimeException("SHA-1 algorithm not implemented");

		}		 		 		 

		File file = new File(filename);
		InputStream is = new FileInputStream(file);

		// Read and SHA-1 4KB chunk at a time
		long offset = 0;
		int numRead = 0;
		while (offset < file.length()) {
			if (file.length() - offset < BLOCKSIZE) {
				long bytesToRead = file.length() - offset;
				numRead = is.read(bytes, 0, (int)(bytesToRead));
			}
			else {
				numRead = is.read(bytes, 0, BLOCKSIZE);
			}
			
			if (numRead < 0) { // -1 is returned for EOF
				break;
			}
			else {
				//System.out.println("Update SHA-1 for 0..." + numRead);
				md.update(bytes, 0, numRead);
				offset += numRead;
			}
		}

		is.close();

		// Ensure all the bytes have been read in
		if (offset < file.length()) {			
			throw new IOException("Could not completely read file "+ file.getName());
		}

		// Get hex-digest
		byte[] digest = md.digest();

		for (int i = 0; i < digest.length; i++) {
			String hex = Integer.toHexString(digest[i]);
			if (hex.length() == 1) {
				hex = "0" + hex;
			}
			hex = hex.substring(hex.length() - 2);
			hash += hex;
		}

		return hash;
	}
}
