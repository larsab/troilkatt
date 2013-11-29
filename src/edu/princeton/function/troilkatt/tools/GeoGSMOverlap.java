package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

/**
 * Calculate sample (GSM) overlap in GEO datsets (GDS) and series (GSE) files.
 *
 * There can be four types of overlapping series, that are dealt differently:
 * 1. Duplicate: series A and B has identical set of samples.
 * 2. Superset: superset series A contains all samples in subsets series B, C,..., N.
 * 3. Proper subset: all samples of dataset A are in dataset B, but B is not a superset.
 * 4. Subset: some, but not all, samples in dataset A are in dataset B.
 * 
 * The GDS->GSM and GSE->GSM mappings are read from the GEO GSM Hbase table. The
 * output is a file with the following format:
 * - List of GSE IDs for series that have a duplicate with an higher ID.
 * - List of superset GSE IDs.
 * - Set of trees built bottom-up by connecting all (proper and non-proper) subsets 
 * to their supersets. Each node in the tree contains the GSE ID of the series 
 * represented by the node, the number of samples in the series, and the GSM IDs of 
 * its samples.
 */
public class GeoGSMOverlap {
	
	/*
	 * Duplicates are special case since these are never considered in the overlap calculation
	 */
	public class Duplicate {		
		// GDS or GSE ID of first dataset/ series
		// Note! This is the gid to be deleted
		public String gid1; // gid1 < gid2     
		// GDS or GSE ID of second dataset/ series
		public String gid2;
		// GSM ID of overlapping samples
		public String[] gsms;
		
		/**
		 * Constructor
		 * 
		 * @param g1 GID1
		 * @param g2 GID2		
		 * @param s array of gsm IDs
		 */
		public Duplicate(String g1, String g2, String[] s) {
			gid1 = g1;
			gid2 = g2;
			gsms = s;
		}
		
		/**
		 * Return number of overlapping samples, which always is the same as
		 * the number of samples in both GID1 and GID2
		 * @return
		 */
		public int getNOverlapping() {
			return gsms.length;
		}
	}
	
	/*
	 * Objects used to hold information about identified supersets.
	 */
	public class Superset {
		// GSE or GDS id of superset
		public String gid;
		// Number of samples in superset
		public int nSamples;
		// Subsets
		public String[] subsetIDs;
		
		/**
		 * Constructor
		 * 
		 * @param g superset GID
		 * @param ns number of samples in superset
		 * @param ids array of subset GIDs
		 */
		public Superset(String g, int ns, String[] ids) {
			gid = g;
			nSamples = ns;
			subsetIDs = ids;			
		}
	}
	
	/*
	 * Class representing sets.
	 */
	public class OverlapSet {
		// GSE or GDS id of set
		public String gid;
		// Number of samples in set
		public int nSamples;
		// Links to all sets that has some sample overlap with this set
		public ArrayList<OverlapLink> subsets;
		// Links to all sets that has some sample overlap with this set 
		public ArrayList<OverlapLink> supersets;
		// Samples to remove (set in removeOverlap)
		public HashSet<String> gsmsToRemove;
		// Clustert ID (set in clusterID)
		public int clusterID;
		
		/**
		 * Constructor
		 * 
		 * @param g GID for this set
		 * @param ns number of samples in the set
		 */
		public OverlapSet(String g, int ns) {
			gid = g;
			nSamples = ns;
			subsets = new ArrayList<OverlapLink>();
			supersets = new ArrayList<OverlapLink>();
			gsmsToRemove = new HashSet<String>();		
			clusterID = -1;
		}		
		
		/**
		 * Get the number of samples in this set that overlap with one or more subsets 
		 * (one sample may be in multiple subsets).
		 * 
		 * @return number of overlapping samples 
		 */
		public int getNOverlapSamples() {
			HashSet<String> gsms = new HashSet<String>();
			
			for (OverlapLink link: subsets) {
				for (String s: link.gsms) {
					gsms.add(s);
				}
			}
			
			return gsms.size();
		}

		/**
		 * Get the total number of samples in subsets
		 * @return
		 */
		public int getNSubSamples() {
			int n = 0;
			for (OverlapLink link: subsets) {
				n = n + link.getOther(this).nSamples;				
			}
			return n;
		}

		/**
		 * Check if this dataset has any subsets
		 * 
		 * @return true if there are no subsets, false otherwise
		 */
		public boolean isLeaf() {
			if (subsets.size() == 0) {
				return true;
			}
			else {
				return false;
			}
		}

		/**
		 * Get a list of subset GIDs
		 * 
		 * @return list of subset GIDs
		 */
		public String[] getSubsetGids() {
			String[] gids = new String[subsets.size()];
			for (int i = 0; i < subsets.size(); i++) {
				OverlapSet sub = subsets.get(i).getOther(this);
				gids[i] = sub.gid;
			}
			return gids;
		}

		/**
		 * A set withot any supersets is a root
		 * 
		 * @return true if this is a superset
		 */
		public boolean isRoot() {
			return supersets.isEmpty();
		}
	}

	/**
	 * Class representing overlap between sets
	 */
	public class OverlapLink {
		// Links to overlapping set
		OverlapSet link1;
		OverlapSet link2;
		// GSM ID of overlapping samples
		String[] gsms;
		
		/**
		 * Constructor that also adds subset lunks o1->o2 and o1<-o2
		 * 
		 * @param o1 first overlap object
		 * @param o2 second overlap object
		 * @param g gsm IDs of overlapping samples
		 */
		public OverlapLink(OverlapSet o1, OverlapSet o2, String[] g) {
			link1 = o1;
			link2 = o2;
			gsms = g;
			link1.subsets.add(this);
			link2.subsets.add(this);
			link1.supersets.add(this);
			link2.supersets.add(this);
		}
		
		/**
		 * Get number of overlapping samples
		 * 
		 * @return number of overlapping samples
		 */
		public int getNOverlapping() {
			return gsms.length;
		}
		
		/**
		 * Remove this link from the subset and supersets lists of the linked objects
		 */
		public void unlink() {
			link1.subsets.remove(this);
			link2.subsets.remove(this);
			link1.supersets.remove(this);
			link2.supersets.remove(this);
		}

		/**
		 * Get reference to the dataset that is not sup
		 * 
		 * @param sup 
		 * @return the linked object that is not sup
		 */
		public OverlapSet getOther(OverlapSet sup) {
			if (link1 == sup) {
				return link2;
			}
			else if (link2 == sup) {
				return link1;
			}
			else {
				throw new RuntimeException("Provided object is neither link1 nor link2");
			}
		}		
	}
	
	/*
	 * Data structures updated as overlap lines are added
	 */
	// List of all datasets/ series IDs
	protected ArrayList<String> allIDs;
	/* List of all duplicate datasets/ series
	 * For each pair of duplicates (O[i], O[j]) and (O[j], O[i]) only the pair where
	 * gid1 < gid2 is added */
	protected ArrayList<Duplicate> duplicates; 
	/* Hash map, with key: superset ID, and value: superset object 
	 * Key: gid, value: overlap object
	 * This data structure has one object for each dataset/series that has at
	 * least one overlapping sample. But it does not include duplicates */
	protected HashMap<String, OverlapSet> overlap;
	// List of overlap links (edges)
	protected ArrayList<OverlapLink> subsetLinks;
	
	/*
	 * Dataset structures calculated once all overlap lines are added
	 */
	// List of all supersets (initialized in removeSupersets)
	protected ArrayList<Superset> supersets;
	// List of all tree roots (sets that are not subsets of other sets)
	protected ArrayList<OverlapSet> treeRoots;
	// List of all sets that have been removed due to too few samples
	protected ArrayList<OverlapSet> minSamplesRemoved;
	// Clusters: key=cluster ID, value=list of sets in cluster
	protected HashMap<Integer, ArrayList<OverlapSet>> clusters;
	
	/**
	 * Constructor
	 */
	public GeoGSMOverlap() {
		allIDs = new ArrayList<String>();
		duplicates = new ArrayList<Duplicate>();
		overlap = new HashMap<String, OverlapSet>();
		subsetLinks = new ArrayList<OverlapLink>();
		supersets = new ArrayList<Superset>();
		treeRoots = new ArrayList<OverlapSet>();
		minSamplesRemoved = new ArrayList<OverlapSet>();
		clusters = new HashMap<Integer, ArrayList<OverlapSet>>();
	}
	
	/**
	 * Reset parser
	 */
	public void reset() {
		allIDs.clear();
		duplicates.clear();
		overlap.clear();
		subsetLinks.clear();
		supersets.clear();
		treeRoots.clear();
		minSamplesRemoved.clear();
		clusters.clear();
	}
	
	/**
	 * Add a line with GSM overlap information. This line is output by GSMOverlap
	 * and has the following format:
	 * 
	 * The line format for the output file is:
	 *   GID_i<tab>GID_j<tab>overlap count,GID_i sample count,GID_j sample count<tab>
	 *   [overlapping samples]<tab>meta_i<tab>meta_j<newline>
	 * 
	 * where [overlapping samples] is the list of overlapping samples in the following format
	 *   GSM_1,GSM_2,..,GSM_N<tab>
	 * 
	 * and meta_i contains meta data read from the GEO meta data table for GID_i
	 *   updateDate<tab>Organism1,Organism2...OrganismN
	 *   
	 * @param line to parse
	 * @return true if line was added. Also a new Overlap object is added to the overlap list
	 * @throws ParseException 
	 */
	public boolean addOverlapLine(String line) throws ParseException {
		String[] parts = line.split("\t");
		if ((parts.length != 8) && (parts.length != 4)) {			
			throw new ParseException("Could not parse line (invalid column count): " + line);
		}
				
		/*
		 * Parse line
		 */
		String gid1 = parts[0];
		String gid2 = parts[1];
		@SuppressWarnings("unused")
		int nOverlapping = 0;
		int nSamples1 = 0;
		int nSamples2 = 0;
		try {
			String[] subParts = parts[2].split(",");
			if (subParts.length != 3) {
				throw new ParseException("Could not parse line (invalid sample count): " + line);
			}
			nOverlapping = Integer.valueOf(subParts[0]);
			nSamples1 = Integer.valueOf(subParts[1]);
			nSamples2 = Integer.valueOf(subParts[2]);
		} catch (NumberFormatException e) {
			throw new ParseException("Could not parse line (invalid sample count number): " + line);
		}
		
		String[] gsms = parts[3].split(",");
		if (gsms.length < 1) {
			throw new ParseException("Could not parse line (no gsms): " + line);
		}
		
		/*
		 * Make sure gid1 < gid2
		 */
		if (compareIDs(gid1, gid2) > 0) {
			// Switch
			//String tmp = gid1;
			//gid1 = gid2;
			//gid2 = tmp;
			//int tmp2 = nSamples1;
			//nSamples1 = nSamples2;
			//nSamples2 = tmp2;
			
			// Ignore
			return true;
			
			//throw new ParseException(String.format("Invalid overlap pair: %s > %s", gid1, gid2));
		}
		
		/*
		 * Update data structures
		 */		
		// Only the pairs where gid1 < gid2 are added to the duplicate and overlap lists
		if (gid1.substring(0, 2).equals(gid2.substring(0, 2)) == false) { 
			// Must be pairs between two series or two datasets
			return false;
		}	
		else {
			// o[i] covers o[j]
			OverlapSet sup = overlap.get(gid1);
			if (sup == null) {
				sup = new OverlapSet(gid1, nSamples1);
				overlap.put(gid1, sup);
			}			
			OverlapSet sub = overlap.get(gid2);
			if (sub == null) {
				sub = new OverlapSet(gid2, nSamples2);
				overlap.put(gid2, sub);
			}
			OverlapLink link = new OverlapLink(sup, sub, gsms);
			subsetLinks.add(link);						
			return true;
		}
		//else {
		//	return false;
		//}
	}
	
	
	
	/**
	 * Find clusters. This function will initialize the clusters data structure.
	 */
	public void findClusters() {
		// Reset cluster datastructure and fields
		clusters.clear();
		for (String g: overlap.keySet()) {
			OverlapSet o = overlap.get(g);
			o.clusterID = -1;
		}
		
		HashSet<OverlapSet> visitedNodes = new HashSet<OverlapSet>();
				
		// Iterate over all sets starting from each set 		 
		int clusterID = 0;
		for (String gid: overlap.keySet()) {
			OverlapSet o = overlap.get(gid);
			
			if (o.clusterID != -1) {
				// Already visited
				continue;
			}
			
			// Traverse all reachable nodes
			visitedNodes.clear();
			findClustersR(clusterID, o, visitedNodes);
			
			// Add cluster elements to cluster map
			ArrayList<OverlapSet> newCluster = clusters.get(clusterID);
			if (newCluster == null) { 
				newCluster = new ArrayList<OverlapSet>();
				clusters.put(clusterID, newCluster);
			}
			// else: old cluster is merged into this one
			
			for (OverlapSet v: visitedNodes) {
				newCluster.add(v);
			}
			
			clusterID++;			
		}
	}
	
	/**
	 * Recursive helper function for finding clusters. The algorithm will traverse the tree
	 * using DFS. Nodes (sets) that have not a clusterID will be marked with the current cluster
	 * ID. Nodes that already are marked with an clusterID will be updated to the new ID, and
	 * so will all other nodes belonging to the same cluster.
	 * 
	 * @param clusterID all visited nodes are marked with this clsuter ID
	 * @param sup node to visit
	 * @param visitedNodes list of previously visited nodes. Needed to avoid loops.
	 * @return none
	 */
	private void findClustersR(int clusterID, OverlapSet sup, HashSet<OverlapSet> visitedNodes) {	
		if (sup.clusterID != -1) { // previously marked
			// Find all sets in the same cluster, and remove the set from the cluster map
			ArrayList<OverlapSet> oldCluster = clusters.remove(sup.clusterID);
			if (oldCluster == null) {
				throw new RuntimeException("Could not find old clsuter for GID: " + sup.gid);
			}
			
			// Update cluster information
			for (OverlapSet o: oldCluster) {
				o.clusterID = clusterID;
			}
			// Insert old cluster back to the cluster map using the new ID as key or
			// merge with an existing map
			if (clusters.containsKey(clusterID)) {
				clusters.get(clusterID).addAll(oldCluster);
			}
			else {
				clusters.put(clusterID, oldCluster);
			}
			
			// Already visited all subclusters when the old cluster was found
			return;
		}
		
		sup.clusterID = clusterID;
		visitedNodes.add(sup);
		
	
		// Visit all unvisited childern
		for (OverlapLink l: sup.subsets) {
			OverlapSet sub = l.getOther(sup);
			if (visitedNodes.contains(sub)) {
				continue;
			}
						
			// Subset is a superset and has not been visited				
			findClustersR(clusterID, sub, visitedNodes);			
		}
	}
	
	/**
	 * Remove duplicates
	 */
	public ArrayList<String> removeDuplicates() {
		HashSet<OverlapSet> toRemove = new HashSet<OverlapSet>();
		
		// Find duplicates
		for (OverlapLink l: subsetLinks) {
			if ((l.getNOverlapping() == l.link1.nSamples) && (l.link1.nSamples == l.link2.nSamples)) {
				// Is duplicate, add oldest GID to remove list (gid1 always older than gid2)
				if (toRemove.contains(l.link1) == false) {
					duplicates.add(new Duplicate(l.link1.gid, l.link2.gid, l.gsms));
				}
				toRemove.add(l.link1);
			}
		}
		
		// Remove duplicates
		ArrayList<String> removedGids = new ArrayList<String>();
		for (OverlapSet o: toRemove) {
			removeSet(o);
			removedGids.add(o.gid);
		}
		
		return removedGids;
	}
	
	/**
	 * Remove from the overlap data structure all datasets/series with fewer than the 
	 * specified number of samples.
	 * 
	 * @param minSamples datasets needs at least this many samples in order not to be
	 * removed
	 * @return list of dataset/series IDs for datasets/series that were removed
	 */
	public ArrayList<String> removeSmallSets(int minSamples) {
		ArrayList<OverlapSet> removedSets = new ArrayList<OverlapSet>();
				
		for (String gid: overlap.keySet()) {
			OverlapSet sup = overlap.get(gid);
			if (sup.nSamples < minSamples) {				
				removedSets.add(sup);			
			}
		}
		
		ArrayList<String> removedGids = new ArrayList<String>();
		for (OverlapSet sup: removedSets) { 
			removeSet(sup);			
			minSamplesRemoved.add(sup);
			removedGids.add(sup.gid);
		}
		
		return removedGids;
	}
	
	/**
	 * Remove supersets from the overlap data structure.
	 *
	 * @return list of dataset/series IDs for datasets/series that were removed
	 */
	public ArrayList<String> removeSupersets() {
		ArrayList<OverlapSet> removedSets = new ArrayList<OverlapSet>();
		
		for (String gid: overlap.keySet()) {
			OverlapSet sup = overlap.get(gid);
			if ((sup.nSamples == sup.getNOverlapSamples()) && (sup.nSamples == sup.getNSubSamples())) {
				removedSets.add(sup);								
				
				Superset d = new Superset(gid, sup.nSamples, sup.getSubsetGids());
				supersets.add(d);
			}
		}
		
		ArrayList<String> removedGids = new ArrayList<String>();
		for (OverlapSet sup: removedSets) { 
			removeSet(sup);	
			removedGids.add(sup.gid);			
		}
		
		return removedGids;
	}

	/**
	 * Create a hierarchical order of supersets and subsets by considering each overlap case A->B and 
	 * A<-B, and then removing the link A<-B if A has more sampels than B (and vice versa). Also add 
	 * tree roots, defined as sets that are not subsets of any other subsets.
	 * 
	 * @return none
	 */
	protected void orderSets() {
		for (OverlapLink l: subsetLinks) {
			if ((l.link1.nSamples < l.link2.nSamples) ||
					((l.link1.nSamples == l.link2.nSamples) && (compareIDs(l.link1.gid, l.link2.gid) < 0))) {
				// Link2 is superet of Link1
				l.link1.subsets.remove(l);
				l.link2.supersets.remove(l);
				//System.err.println(l.link2.gid + " is superset of " + l.link1.gid);
			}
			else {
				// Link1 is superset of Link2
				l.link2.subsets.remove(l);
				l.link1.supersets.remove(l);
				//System.err.println(l.link1.gid + " is superset of " + l.link2.gid);
			}
		}
			
		// Roots are those sets that are not in the allSubsets data structure
		updateTreeRoots();
	}
	
	/**
	 * Reduce overlap by using the following rules:
	 * 1. Do a depth-first search starting from each superset
	 * 2. For each set remove the samples from the subset, if the subset has > N samples.
	 *    Otherwise keep the subset samples in the superset.
	 * 3. Delete sets with fewer than M samples after removing the subset samples 
	 * Note! The overlap graph may contain loops
	 *    
	 * @param minSamples minimum number of samples required for a dataset/series. Sets with
	 * fewer than minSamples are removed.
	 * @param maxOverlap maximum number of allowed sample overlap between superset and subset.
	 * If a superset has more than maxOverlap samples these are removed from the superset
	 * @return list of dataset/series IDs for datasets/series that were removed due to 
	 * them ending up with too few samples
	 */
	public ArrayList<String> reduceOverlap(int minSamples, int maxOverlap) {
		orderSets();
		
		HashSet<String> visitedGids = new HashSet<String>(); // avoid loops
		ArrayList<String> removedGids = new ArrayList<String>();
		
		while (true) { // loop until no more changes
			boolean changes = false;
			// Find samples to remove from each dataset starting from a tree root
			for (OverlapSet sup: treeRoots) {
				if (reduceOverlapR1(maxOverlap, sup, visitedGids)) {
					changes = true;
				}
			}
			if (changes == false) {
				break;
			}
		}
		
		visitedGids.clear();
		// Delete subsets with too few samples		
		for (OverlapSet sup: treeRoots) {				
			reduceOverlapR2(minSamples, maxOverlap, sup, visitedGids, removedGids);					
		}
		// Delete roots with too few samples
		for (OverlapSet sup: treeRoots) {
			int remainingSamples = sup.nSamples	- sup.gsmsToRemove.size(); 
			if (remainingSamples < minSamples) {
				removeSet(sup);
				removedGids.add(sup.gid);								
				minSamplesRemoved.add(sup);		
				//System.err.println("Remove: " + sup.gid);
			}
		}
		
		// Also need to update treeRoots since some trees may have been split
		// and other may have been removed
		updateTreeRoots();
		
		// Delete links where overlap has been removed
		visitedGids.clear();				
		for (OverlapSet sup: treeRoots) {				
			reduceOverlapR3(sup, visitedGids);					
		}
		
		// Also need to update treeRoots since some trees may have been split
		// when links were removed
		updateTreeRoots();
		
		return removedGids;
	}
	
	/**
	 * Recursive helper function for reducing overlapping samples. This function does the removal
	 * of overlapping samples of supersets.
	 * 
	 * @param maxOverlap maximum number of allowed sample overlap between superset and subset.
	 * If a superset has more than maxOverlap samples these are removed from the superset
	 * @param sup current superset
	 * @visitedGids list of previously visited supersets, needed to avoid loops.
	 * @return true if at least one set was changed
	 */
	private boolean reduceOverlapR1(int maxOverlap, OverlapSet sup, HashSet<String> visitedGids) {		
		boolean rv = false;
		
		if (visitedGids.contains(sup.gid)) { // already visited
			return rv;
		}
		visitedGids.add(sup.gid);
		
		if (sup.isLeaf() == true) { // Leafs have no overlap to remove
			return rv;
		}
		
		for (OverlapLink l: sup.subsets) {
			OverlapSet sub = l.getOther(sup);			
						
			// Chick child sets first				
			if (reduceOverlapR1(maxOverlap, overlap.get(sub.gid), visitedGids)) {
				rv = true;
			}
	
			// Remove overlapping samples from superset
			if (l.getNOverlapping() > maxOverlap) {
				for (String g: l.gsms) {
					sup.gsmsToRemove.add(g);
				}
				rv = true;
			}
		}
		
		return rv;
	}

	/**
	 * Recursive helper function for reducing overlapping samples. This function deletes 
	 * subsets with too few samples. It is run after the superset samples are removed.
	 * 
	 * @param minSamples minimum number of samples required for a dataset/series. Sets with
	 * fewer than minSamples are removed.
	 * @param maxOverlap maximum number of allowed sample overlap between superset and subset.
	 * If a superset has more than maxOverlap samples these are removed from the superset
	 * @param sup current superset
	 * @param visitedGids list of previously visited supersets, needed to avoid loops.
	 * @param removedGids list of GIDs of removed sets.
	 */
	private void reduceOverlapR2(int minSamples, int maxOverlap, OverlapSet sup, HashSet<String> visitedGids, ArrayList<String> removedGids) {
		if (visitedGids.contains(sup.gid)) { // already visited
			return;
		}
		visitedGids.add(sup.gid);
		
		if (sup.isLeaf() == true) { // no subsets to check	
			return;
		}

		// List of subsets to chek
		// A seperate list is necessary since a removal of a subset may modify sup.subsets
		ArrayList<OverlapLink> toCheck = new ArrayList<OverlapLink>(sup.subsets);
		for (OverlapLink l: toCheck) {
			OverlapSet sub = l.getOther(sup);
			// Check subsets-subsets first
			if (overlap.get(sub.gid) == null) { // has been deleted
				return;
			}
			reduceOverlapR2(minSamples, maxOverlap, overlap.get(sub.gid), visitedGids, removedGids);
	
			// Remove datasets with too few samples
			int remainingSamples = sub.nSamples - sub.gsmsToRemove.size(); 
			if (remainingSamples < minSamples) {
				removeSet(sub);
				removedGids.add(sub.gid);								
				minSamplesRemoved.add(sub);		
				//System.err.println("Remove: " + sub.gid);
				
				/*
				 * Attempt to add some of the GSMs in the removed subset into this
				 * superset 
				 */					
				// Find remaining samples that overlap with superset samples
				ArrayList<String> uniqueSamples = new ArrayList<String>();
				for (String s: l.gsms) {
					if (sub.gsmsToRemove.contains(s) == false) {
						uniqueSamples.add(s);
					}
				}

				if (uniqueSamples.size() <= maxOverlap) {
					// Undelete any samples that overlap with the subset to be deleted
					for (String u: uniqueSamples) {
						sup.gsmsToRemove.remove(u);
					}
				}
			}
		}
	}
	
	/**
	 * Recursive helper function for reducing overlapping samples. This function deletes 
	 * links where all overlapping samples have been removed.
	 * 
	 * @param sup current superset
	 * @param visitedGids list of previously visited supersets, needed to avoid loops.
	 */
	private void reduceOverlapR3(OverlapSet sup, HashSet<String> visitedGids) {
		if (visitedGids.contains(sup.gid)) { // already visited
			return;
		}
		visitedGids.add(sup.gid);
		
		if (sup.isLeaf() == true) { // no subsets to check	
			return;
		}

		// List of subsets to chek:
		// A seperate list is necessary since a link removal may modify sup.subsets
		ArrayList<OverlapLink> toCheck = new ArrayList<OverlapLink>(sup.subsets);
		for (OverlapLink l: toCheck) {
			OverlapSet sub = l.getOther(sup);
			// Check subsets-subsets first			
			reduceOverlapR3(overlap.get(sub.gid), visitedGids);
	
			// Remove link where overlap has been removed too few samples
			boolean overlapRemains = false;
			for (String gid: l.gsms) {
				if (sup.gsmsToRemove.contains(gid) == false) {
					overlapRemains = true;
					break;
				}
			}
			if (overlapRemains == false) {
				sup.subsets.remove(l);
				sub.supersets.remove(l);
			}
		}
	}
	
	/**
	 * Return a list of dataset/ series IDs for duplicates to be deleted.
	 * 
	 * @return list of dataset/ series IDs for duplicates
	 */
	public HashSet<String> getDuplicateIDs() {
		HashSet<String> gids = new HashSet<String>();
		
		for (Duplicate d: duplicates) {
			gids.add(d.gid1);
		}
		
		return gids;
	}

	/**
	 * Return a list of duplicate series/ datasets found
	 * 
	 * @return duplicate objects
	 */
	public ArrayList<Duplicate> getDuplicates() {
		return duplicates;
	}
	
	/**
	 * Return true if the specified ID is found in the duplicates list
	 * 
	 * @param g ID to check
	 * @return true if the GID is a duplicate
	 */
	public boolean isInDuplicates(String g) {
		for (Duplicate d: duplicates) {
			if (g.equals(d.gid1)) {
				return true;
			}
		}
		
		// not a duplicate
		return false;
	}

	/**
	 * Return a list of superset dataset/ series IDs
	 * 
	 * @return list of dataset/ series IDs for supersets
	 */
	public HashSet<String> getSupersetIDs() {
		HashSet<String> gids = new HashSet<String>();
		
		for (Superset s: supersets) {
			gids.add(s.gid);
		}
		
		return gids;
	}
	
	/**
	 * Return a list of superset series/ datasets
	 * 
	 * @return list of superset objects
	 */
	public ArrayList<Superset> getSupersets() {
		return supersets;
	}
	
	/**
	 * Return a list of series/ datasets IDs removed due to not meeting minSamples requirement
	 *
	 * @return list of dataset/ series IDs for removed samples
	 */
	public HashSet<String> getRemovedIDs() {
		HashSet<String> gids = new HashSet<String>();
		
		for (OverlapSet m: minSamplesRemoved) {
			gids.add(m.gid);
		}
		
		return gids;
	}
	
	/**
	 * Return a list of series/ datasets removed due to not meeting minSamples requirement
	 *
	 * @return list of dataset/ series IDs for removed samples
	 */
	public ArrayList<OverlapSet> getRemoved() {
		return minSamplesRemoved;
	}
	
	/**
	 * Return a hashmap that contains an entry for each dataset/series that has samples
	 * to be removed. In the hashmap the dataset/series ID is used as key, and the value 
	 * is a list of sample IDs that should be removed in this dataset/series. 
	 * 
	 * return hashmap as described above
	 */
	public HashMap<String, HashSet<String>> getRemovedSamples() {
		HashMap<String, HashSet<String>> toRemove = new HashMap<String, HashSet<String>>();
		
		for (String gid: overlap.keySet()) {
			OverlapSet sup = overlap.get(gid);
			if (sup.gsmsToRemove.size() > 0) {
				toRemove.put(gid, sup.gsmsToRemove);
			}
		}
		
		return toRemove;
	}
	
	/**
	 * Get cluster of connected sets
	 * 
	 * @param minClusterSize minimum number of elements in a cluster
	 * @return list of lists of dataset IDs. Each list contains the IDs of the sets in a cluster
	 */
	public ArrayList<ArrayList<String>> getClusterIDs(int minClusterSize) {
		ArrayList<ArrayList<String>> rv = new ArrayList<ArrayList<String>>();
		
		for (Integer cid: clusters.keySet()) {
			ArrayList<OverlapSet> cl = clusters.get(cid);
			if (cl.size() < minClusterSize) {
				continue;
			}
			
			ArrayList<String> clid = new ArrayList<String>();			
			for (OverlapSet o: cl) {				
				clid.add(o.gid);
			}
			rv.add(clid);
		}
		
		return rv;
	}
	
	/**
	 * Get cluster of connected sets
	 * 
	 * @param minClusterSize minimum number of elements in a cluster
	 * @return list of lists of dataset IDs. Each list contains the sets in a cluster
	 */
	public ArrayList<ArrayList<OverlapSet>> getClusters(int minClusterSize) {
		ArrayList<ArrayList<OverlapSet>> rv = new ArrayList<ArrayList<OverlapSet>>();
		
		for (Integer cid: clusters.keySet()) {
			ArrayList<OverlapSet> cl = clusters.get(cid);
			if (cl.size() < minClusterSize) {
				continue;
			}
			
			rv.add(cl);
		}
		
		return rv;
	}

	/**
	 * Helper function to compare two dataset/series IDs
	 * 
	 * @param gid1 ID that starts with either GSE or GDS and that may contain a "-" for subsets
	 * @param gid2 ID of the same type as gid1
	 * @return -1 if gid1 < gid2, 0 if gid1 == gid2, and 1 if gid1 > gid2
	 */
	public static int compareIDs(String gid1, String gid2) {
		int id1;
		int id2;
		//int id1p2 = 0;
		//int id2p2 = 0;
		
		if (gid1.contains("GSE")) {
			gid1 = gid1.replace("GSE", "");
		}
		if (gid1.contains("GDS")) {
			gid1 = gid1.replace("GDS", "");
		}
		if (gid1.contains("-")) {
			String parts[] = gid1.split("-");
			id1 = Integer.valueOf(parts[0]);
			//id1p2 = Integer.valueOf(parts[1]);
		}
		else {
			id1 = Integer.valueOf(gid1);
		}
		
		if (gid2.contains("GSE")) {
			gid2 = gid2.replace("GSE", "");
		}
		if (gid2.contains("GDS")) {
			gid2 = gid2.replace("GDS", "");
		}
		if (gid2.contains("-")) {
			String parts[] = gid2.split("-");
			id2 = Integer.valueOf(parts[0]);
			//id2p2 = Integer.valueOf(parts[1]);
		}
		else {
			id2 = Integer.valueOf(gid2);
		}
		
		if (id1 < id2) {
			return -1;			
		}
		else if (id1 > id2) {
			return 1;
		}
		else {
			//if (id1p2 < id2p2) {
			//	return -1;
			//}
			//else if (id1p2 > id2p2) {
			//	return 1;
			//}
			//else {
			//	return 0;
			//}
			
			// Is actually an error if there are overlapping samples between two platforms
			return 0;
		}
	}	
	
	/**
	 * Helper function to update the treeRoots data structure
	 */
	private void updateTreeRoots() {
		treeRoots.clear();
		for (String gid: overlap.keySet()) {
			OverlapSet o = overlap.get(gid);
			if (o.isRoot()) { // is root
				treeRoots.add(o);
			}
		}
	}

	/**
	 * Helper function to delete all subset links for a superset
	 * 
	 * @param sup superset for which to delete subset links
	 */
	private void removeSet(OverlapSet sup) {
		ArrayList<GeoGSMOverlap.OverlapLink> toRemove = new ArrayList<GeoGSMOverlap.OverlapLink>(sup.subsets);
		for (OverlapLink link: toRemove) { // for each subset...
			link.unlink();
			subsetLinks.remove(link);
		}
		
		toRemove = new ArrayList<GeoGSMOverlap.OverlapLink>(sup.supersets);
		for (OverlapLink link: toRemove) { // for each superset...
			link.unlink();			
		}
		
		overlap.remove(sup.gid);
	}
	
	/**
	 * Find and remove overlapping samples: that is do the following operations:
	 * 1. Remove duplicates
	 * 2. Remove small datasets
	 * 3. Remove supersets
	 * 4. Remove overlapping samples (and remove small datasets)
	 * 
	 * The results can be retrieved using the getter functions.
	 * 
	 * @param minSamples minimum number of samples required for a dataset/series. Sets with
	 * fewer than minSamples are removed.
	 * @param maxOverlap maximum number of allowed sample overlap between superset and subset.
	 * If a superset has more than maxOverlap samples these are removed from the superset
	 */
	public void find(int minSamples, int maxOverlap) {
		removeDuplicates();
		removeSmallSets(minSamples);
		removeSupersets();
		reduceOverlap(minSamples, maxOverlap);
		findClusters();
	}
	
	/**
	 * Find overlap:
	 * 1. Remove duplicates
	 * 2. Remove small datasets
	 * 3. Remove supersets
	 * 4. Remove overlapping samples (and remove small datasets)
	 * 
	 * Output file format, one line per datasets that has one or more, or all, datasets
	 * removed. The line format is for a dataset/series that should be deleted: 
	 *   
	 *   dataset/series ID<tab>all<newline>
	 *   
	 * and for a dataset/series where N of the samples should be deleted:
	 * 
	 *   dataset/series ID<tab>sample ID 1, sample ID 2, ..., sample ID N<newline>
	 * 
	 * @param os output file a list of datasets and samples removed are output to this file. The 
	 * file format description is above.
	 * @param log logfile where information about duplicate, supersets, removed datasets, clusters,
	 * etc is written
	 * @param minSamples minimum number of samples required for a dataset/series. Sets with
	 * fewer than minSamples are removed.
	 * @param maxOverlap maximum number of allowed sample overlap between superset and subset.
	 * If a superset has more than maxOverlap samples these are removed from the superset
	 * @return none
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void find(BufferedWriter os, BufferedWriter log,
			int minSamples, int maxOverlap) throws IOException {	
		
		/*
		 * 1. Find clusters
		 * 
		 * Note! this is done for logging reasons only
		 */
		findClusters();		
		// Log file
		ArrayList<ArrayList<String>> clusterGids = getClusterIDs(2);
		log.write("start: clusters before overlap reduction (" + clusterGids.size() + ")\n");
		for (int i = 0; i < clusterGids.size(); i++) {			
			ArrayList<String> cluster = clusterGids.get(i);
			log.write(cluster.get(0));
			for (int j = 1; j < cluster.size(); j++) {
				log.write("\t" + cluster.get(j));
			}
			log.write("\n");
		}
		log.write("end: clusters before overlap\n\n");
		
		// Cluster information is not written to output file
		
		/*
		 * 2. Remove duplicats
		 */
		ArrayList<String> duplicates = removeDuplicates();
		log.write("start: duplicates removed (" + duplicates.size() + ")\n");
		for (String d: duplicates) {			
			log.write(d + "\n");
		}
		log.write("end: duplicates removed\n\n");
		
		/*
		 * 2. Remove datasets/series with too few samples
		 */		
		ArrayList<String> removed1 = removeSmallSets(minSamples);
		log.write("start: small datasets removed due to fewer than "  + minSamples + " samples (" + removed1.size() + ")\n");					
		for (String gid: removed1) {
			log.write(gid + "\n");
		}
		log.write("end: small datasets removed\n\n");
		
		/*
		 * 3. Supersets
		 */
		
		ArrayList<String> removed2 = removeSupersets();				
		log.write("start: supersets removed (" + removed2.size() + ")\n");
		for (String gid: removed2) {
			log.write(gid + "\n");
		}
		log.write("end: supersets removed.\n\n");		
				
		/*
		 * 4. Reduce overlap
		 */
		log.write("start: overlap removed datasets with fewer than " + minSamples + " samples after overlap reduction\n");
		while (true) {			
			ArrayList<String> removed3 = reduceOverlap(minSamples, maxOverlap);
			if (removed3.size() > 0) {
				for (String gid: removed3) {
					log.write(gid + "\n");
				}
			}
			else {
				break;
			}		
		}
		log.write("end: overlap remvoed datasets\n\n");
		
		// Log (remaining clusters)		
		findClusters();
		clusterGids = getClusterIDs(2);
		log.write("start: clusters after overlap reduction (" + clusterGids.size() + ")\n");
		for (int i = 0; i < clusterGids.size(); i++) {
			ArrayList<String> cluster = clusterGids.get(i);
			log.write(cluster.get(0));
			for (int j = 1; j < cluster.size(); j++) {
				log.write("\t" + cluster.get(j));
			}
			log.write("\n");
		}
		log.write("end: clusters after overlap reduction\n\n");
		
		// Log remaining overlap
		
		
		
		/*
		 * Create output file, and also write same info to the log file in a human
		 * readable format.
		 */		
		ArrayList<Duplicate> duplicateGids = getDuplicates();
		ArrayList<Superset> supersets = getSupersets();
		ArrayList<OverlapSet> removed = getRemoved();
		HashMap<String, HashSet<String>> removedSamples = getRemovedSamples();
		
		// Duplicates
		log.write("Duplicates (" + duplicates.size() + "):\n");
		for (Duplicate d: duplicateGids) {
			// Output
			os.write(d.gid1 + "\tall\n");			
			// Log
			log.write(d.gid1 + "\tduplicate of\t" + d.gid2 + "\n");			
		}
		
		// Supersets
		log.write("\nSupersets (" + supersets.size() + "):\n");
		for (Superset s: supersets) {
			// Output
			os.write(s.gid + "\tall\n");
			
			// log
			log.write(s.gid + "\tsuperset for");
			for (String g: s.subsetIDs) {
				log.write("\t" + g);
			}
			log.write("\n");
		}
		
		// Removed datasets/series
		log.write("\nRemoved due too few samples(" + removed.size() + "):\n");
		for (OverlapSet o: removed) {
			// output
			os.write(o.gid +"\tall\n");
			
			// log
			log.write(o.gid + "\tsamples before and after\t" + o.nSamples + "\t" + String.valueOf(o.nSamples - o.gsmsToRemove.size()) + "\n");
		}
		
		// Removed samples
		log.write("\nstart: samples removed (" + removedSamples.size() + " sets):\n");
		for (String gid: removedSamples.keySet()) {
			HashSet<String> samples = removedSamples.get(gid);
			
			// output
			os.write(gid + "\t");
			String sampleStr = "";
			for (String s: samples) {
				sampleStr = sampleStr + s + ", ";
			}
			// Remove last  ", ";
			os.write(sampleStr.substring(0, sampleStr.length() -2) + "\n");
			
			// log
			log.write(gid + "\tremove (" + removedSamples.get(gid).size() + ")");
			for (String gsm: removedSamples.get(gid)) {
				log.write("\t" + gsm);
			}
			log.write("\n");
		}		
		log.write("end: samples removed");
	}
	
	/**
	 * Read the file with dataset and samples to delete that was created in find()
	 * 
	 * @param filename file to read
	 * @param deletedDatasets list where deleted datasets are added
	 * @param deletedSamples map where the samples to delete for each dataset are added
	 * @param providedLogger optional logger. If null, no logging information is written
	 * @throws IOException 
	 */
	public static void readOverlapFile(String filename, ArrayList<String> deletedDatasets,
			HashMap<String, String[]> deletedSamples, Logger providedLogger) throws IOException {
		// The file format is described in detail in FindGSMOverlap	
		BufferedReader ins = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = ins.readLine()) != null) {
			String parts[] = line.split("\t");			
			// each line should have two tab separated parts
			if (parts.length != 2) {
				ins.close();
				if (providedLogger != null) {
					providedLogger.error("Invalid line in overlap file: " + filename);
				}
				throw new IOException("Invalid line in overlap file");
			}
			String gid = parts[0];			

			if (parts[1].equals("all")) { // All samples should be deleted (i.e. the entire file)
				deletedDatasets.add(gid); 
			}
			else {
				String samples[] = parts[1].split(",");
				// Remove whitespace
				for (int i = 0; i < samples.length; i++) {
					samples[i] = samples[i].trim(); 
				}
				deletedSamples.put(gid, samples);
			}
		} 
		ins.close();
	}
	
	
	public static void readOverlapFile(String filename, ArrayList<String> deletedDatasets,
			HashMap<String, String[]> deletedSamples) throws IOException {
		readOverlapFile(filename, deletedDatasets, deletedSamples, null);
	}
	
	/**
	 * Parse the headers from a soft file to get the columns to delete
	 * 
	 * @param toDelete list with GSM sample IDs to delete
	 * @param headerLine first line in PCL file
	 * @return list of column indexes for the samples to be deleted in this PCL files, or
	 * null if the headerLine does not contain all columns to be deleted
	 */
	public static int[] getDeleteColumnIndexes(String[] toDelete, String headerLine) {				
		int[] deleteColumnIndexes = new int[toDelete.length];
		Arrays.fill(deleteColumnIndexes, -1);		

		String cols[] = headerLine.split("\t");
		if (cols.length < 4) {
			// Too few columns
			return null;
		}			
		
		for (int i = 0; i < toDelete.length; i++) { // loop over samples
			for (int j = 3; j < cols.length; j++) { // loop over columns
				// The sample ID is always in the sample header (sampleID: sample title)
				if (cols[j].startsWith(toDelete[i] + ":")) {
					deleteColumnIndexes[i] = j;
					
				}
			}
		}

		// Make sure all samples where found
		for (int i = 0; i < toDelete.length; i++) {
			if (deleteColumnIndexes[i] == -1) {				
				return null;
			}
		} 
		
		return deleteColumnIndexes;
	}
	
	/**
	 * Return a line with some columns deleted. 
	 * 
	 * Note! This method does not check if all columns to be deleted are in the line.
	 * If a line has too few columns it is returned as it was and no error is reported.
	 * 
	 * @param line to parse
	 * @param array of column indexes to delete.
	 * @return line with some columns deleted
	 */
	public static String deleteColumnsFromLine(String line, int[] deleteColumnIndexes) {
		String[] cols = line.split("\t");
		String outputLine = cols[0] + "\t" + cols[1] + "\t" + cols[2];
		for (int i = 3; i < cols.length; i++) {
			boolean includeColumn = true;
			for (int j = 0; j < deleteColumnIndexes.length; j++) {
				if (deleteColumnIndexes[j] == i) {
					includeColumn = false;					
					break;
				}
			}
			if (includeColumn) {
				outputLine = outputLine + "\t" + cols[i];
			}
		}
		
		return outputLine;
	}
	
	/**
	 * Main function
	 * 
	 * @param argv command line arguments
	 *   0: input filename
	 *   1: output filename (with samples to delete)
	 *   2: log filename
	 *   3: minSamples
	 *   4: maxOverlap
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String argv[]) throws ParseException, IOException {
		GeoGSMOverlap overlap = new GeoGSMOverlap();
		if (argv.length != 5) {
			System.err.println("Usage: inputFilename outputFilename logFilename minSamples maxOverlap");
			System.exit(-1);
		}
		String inputFilename = argv[0];
		String outputFilename = argv[1];
		String logFilename = argv[2];
		int minSamples = Integer.valueOf(argv[3]);
		int maxOverlap = Integer.valueOf(argv[4]);
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		String line;
		while ((line = ins.readLine()) != null) {
			overlap.addOverlapLine(line);			
		} 
		BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
		BufferedWriter log = new BufferedWriter(new FileWriter(logFilename));
		overlap.find(os, log, minSamples, maxOverlap);
		os.close();
		ins.close();
		log.close();
	}
}
