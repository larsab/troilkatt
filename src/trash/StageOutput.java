package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;
import java.util.HashMap;

import edu.princeton.function.troilkatt.fs.TroilkattFile;


public class StageOutput {
	String rowKey;
	ArrayList<TroilkattFile> allFiles = null;
	ArrayList<TroilkattFile> newFiles = null;
	ArrayList<TroilkattFile> updatedFiles = null;
	ArrayList<TroilkattFile> deletedFiles = null;
	HashMap<String, byte[]> logs = new HashMap<String, byte[]>();
	HashMap<String, Object> objects = new HashMap<String, Object>();

	public StageOutput(String rowKey) {
		this.rowKey = rowKey;
	}

	public void addAllFiles(ArrayList<TroilkattFile> l) {
		allFiles = l;
		
	}

	public void addNewFiles(ArrayList<TroilkattFile> l) {
		newFiles = l;
		
	}

	public void addUpdatedFiles(ArrayList<TroilkattFile> l) {
		updatedFiles = l;		
	}

	public void addDeletedFiles(ArrayList<TroilkattFile> l) {
		deletedFiles = l;
		
	}

	public void addLog(String filename, byte[] value) {
		logs.put(filename, value);
		
	}

	public void addObject(String key, Object value) {
		objects.put(key, value);
	}

	public ArrayList<TroilkattFile> getAllHbaseFiles() {
		return allFiles;
	}

	public ArrayList<TroilkattFile> getNewHbaseFiles() {
		return newFiles;
	}

	public ArrayList<TroilkattFile> getUpdatedHbaseFiles() {
		return updatedFiles;
	}

	public ArrayList<TroilkattFile> getDeletedHbaseFiles() {
		return deletedFiles;
	}

	public HashMap<String, byte[]> getLogs() {
		return logs;
	}
	
	
}
