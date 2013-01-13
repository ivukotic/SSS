package edu.uchicago.SSSserver;

import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataContainer {

	final Logger logger = LoggerFactory.getLogger(DataSetsBuffer.class);

	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	ArrayList<tree> summedTrees = new ArrayList<tree>();
	private int processedfiles;
	private int totalfiles;

	public void add(Dataset ds) {
		dSets.add(ds);
	}

	public long getInputSize() {
		long res = 0;
		for (Dataset ds : dSets) {
			res += ds.getSize();
		}
		if (res > 0)
			return res;
		else {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return getInputSize();
		}
	}

	public String getTreeDetails() {
		processedfiles = 0;
		totalfiles = 0;
		summedTrees.clear();
		boolean first = true;

		for (Dataset ds : dSets) {
			ArrayList<tree> ts = getTrees(ds);
			if (ts.size() == 0)
				continue;

			processedfiles += ds.processed;
			totalfiles += ds.alRootFiles.size();
			if (first) {
				for (tree t : ts) {
					tree crtree = new tree(t.name, t.events, t.size);
					crtree.branches.addAll(t.branches);
					summedTrees.add(crtree);
				}
				first = false;
			} else {
				for (tree t : ts) {
					boolean found = false;
					for (tree st : summedTrees) {
						if (st.name.equalsIgnoreCase(t.name)) {
							st.events += t.events;
							st.size += t.size;
							for (branch b : st.branches) {
								b.size += t.getBranchSize(b.name);
							}
							found = true;
							continue;
						}
					}
					if (!found) {
						logger.error("This file contains a tree not seen in the first file. Tree skipped.");
					}
				}
			}
		}

		String res = summedTrees.size() + "\n";
		for (tree st : summedTrees) {
			res += st.name + ":" + st.events + ":" + st.size + ":" + st.getNBranches() + "\n";
		}
		res += totalfiles + ":" + processedfiles + "\n";
		logger.info("total files: " + totalfiles + "\tprocessed files: " + processedfiles + "\n");
		return res;
	}

	private ArrayList<tree> getTrees(Dataset ds) {
		logger.info("getting tree info from DS: " + ds.name);
		ArrayList<tree> res = ds.getTrees();
		if (res.size() == 0) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			res = getTrees(ds);
		} else {
			logger.info("not yet ready. trying again.");
		}
		return res;
	}

	private tree getTree(String name) {
		for (tree st : summedTrees) {
			if (st.name.equalsIgnoreCase(name)) {
				return st;
			}
		}
		return null;
	}

	public String getOutputEstimate(String mainTree, HashSet<String> treesToCopy, HashSet<String> branchesToKeep, String cutCode) {
		if (mainTree.equalsIgnoreCase("undefined") || branchesToKeep.size() == 0)
			return "0:0:0:0";
		logger.info("mainTree:" + mainTree);
		for (String t : treesToCopy)
			logger.info("tree to copy:" + t);

		long inpEvents = 0;
		long estSize = 0;
		long estEvents = 0;
		int estBranches = 0;

		if (treesToCopy.contains(mainTree)) {
			treesToCopy.remove(mainTree);
		}

		for (String ttc : treesToCopy) {
			logger.info("adding to the estimate a full size of treeToCopy: " + ttc);
			tree t = getTree(ttc);
			if (t != null)
				estSize += t.size;
			else
				logger.error("can't be that tree was not found");
		}

		tree t = getTree(mainTree);
		inpEvents = t.events;
		ArrayList<branch> brs = t.getBranches();
		for (branch b : brs) {
			for (String inp : branchesToKeep) {
				String matchString = inp.replace("*", "\\w+");
				if (b.name.matches(matchString)) {
					logger.debug("branch selected: " + b.name);
					estSize += b.size;
					estBranches++;
				}
			}
		}
		
		estEvents=inpEvents;
		
		return inpEvents + ":" + estEvents + ":" + estSize + ":" + estBranches;
	}
}
