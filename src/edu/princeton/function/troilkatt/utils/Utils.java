package edu.princeton.function.troilkatt.utils;

import java.util.ArrayList;
import java.util.Vector;

/**
 * Various useful methods
 *
 */
public class Utils {
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
