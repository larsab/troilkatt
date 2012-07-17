package edu.princeton.function.troilkatt.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Various useful methods
 *
 */
public class Utils {
	/**
	 * Get yes/no answer on stdin
	 * 
	 * @param label message to print to stdout. The format is "<label> [Y/n]: ", with 
	 * the default value in uppercase.
	 * @param defaultYes true if yes is default
	 * @return true if yes, false if not
	 */
	public static boolean getYesOrNo(String label, boolean defaultYes) {
		java.io.BufferedReader stdin = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
		
		while (true)  {
			if (defaultYes) {
				System.out.println(label + " [Y/n]: ");
			}
			else {
				System.out.println(label + " [y/N]: ");
			}
			String line = null;
			try {
				line = stdin.readLine().toLowerCase();
			} catch (IOException e) {
				System.err.println("IOException while reading from stdin: " + e);				
			}
			if (line.equals("y") || line.equals("yes")) {
				return true;
			}
			else if (defaultYes && line.equals("")) {
				return true;
			}
			else if (line.equals("n") || line.equals("no")) {
				return false;
			}
			else if (! defaultYes && line.equals("")) {
				return false;
			}
		}
	}
	
	/**
	 * Remove duplicates and merge two lists.
	 * 
	 * @param l1: first list or null
	 * @param l2: second or null
	 * 
	 * @return: merged list, or null if both l1 and l2 are null
	 */
	public static String[] mergeArrays(String l1[], String l2[]) {
		if (l1 == null) {
			return l2;
		}
		if (l2 == null) {
			return l1;
		}
		
		Vector<String> merged = new Vector<String>(l1.length + l2.length);

		for (String s: l1) {
			if (! merged.contains(s)) {
				merged.add(s);
			}
		}
		for (String s: l2) {
			if (! merged.contains(s)) {
				merged.add(s);
			}
		}

		return merged.toArray(new String[merged.size()]);
	}
	
	/**
	 * Convert a String[] to an ArrayList<String>
	 * 
	 * @param array to convert
	 * @return ArrayList<String>, or null if the input array was null
	 */
	public static ArrayList<String> array2list(String[] array) {
		if (array == null) {
			return null;
		}
		
		ArrayList<String> list = new ArrayList<String>();
		
		for (String s: array) {
			list.add(s);
		}
		
		return list;
	}
}
