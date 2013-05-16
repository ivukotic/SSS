package edu.uchicago.SSSserver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

public class DataContainer {

	final private static Logger logger = Logger.getLogger(DataContainer.class);

	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	ArrayList<tree> summedTrees = new ArrayList<tree>();
	private int processedfiles;
	private int totalfiles;
	private long inpEvents;
	private long estSize;
	private long estEvents;
	private int estBranches;

	public void add(Dataset ds) {
		dSets.add(ds);
	}

	public long getInputSize() {
		long res = 0;
		boolean needswait = false;
		for (Dataset ds : dSets) {
			long dsa = ds.getSize();
			if (dsa == -1) {
				logger.info("ds not known or not in fax.");
				return -1L;
			}
			if (dsa == 0) {
				logger.info("size of this ds not yet known.");
				needswait = true;
			}
			res += dsa;
		}
		if (needswait == false)
			return res;
		else {
			try {
				logger.info("sleeping for 1 second before retry.");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return getInputSize();
		}
	}

	private tree getTree(String name) {
		for (tree t : summedTrees) {
			if (t.getName().equals(name)) {
				logger.debug("tree already exists in summ will return that one");
				return t;
			}
		}

		logger.debug("a new tree will create it from scratch in summ.");
		tree t = new tree();
		summedTrees.add(t);
		return t;
	}

	public void updateTrees() {

		processedfiles = 0;
		totalfiles = 0;
		summedTrees.clear();

		for (Dataset ds : dSets) {

			logger.info("getting tree info from DS: " + ds.name + " ...");
			ArrayList<tree> ts = getTrees(ds);
			if (ts.size() == 0)
				continue;

			processedfiles += ds.processed;
			totalfiles += ds.alRootFiles.size();

			for (tree t : ts) {
				// logger.debug("adding tree:"+t.getName());
				getTree(t.getName()).add(t);
				// logger.debug("added. done.");
			}

			logger.info("getting tree info from DS: " + ds.name + " DONE.");
		}
	}

	// this one starts chain reaction of inspect-ing root files
	public String getTreeDetails() {
		logger.debug("Updating tree details ...");
		updateTrees();

		String res = summedTrees.size() + "\n";
		for (tree st : summedTrees) {
			res += st.getName() + ":" + st.getEvents() + ":" + st.getSize() + ":" + st.getNBranches() + "\n";
		}
		res += totalfiles + ":" + processedfiles + "\n";
		logger.debug("total files: " + totalfiles + "\tprocessed files: " + processedfiles + "\n");

		logger.debug("Updating tree details DONE");
		return res;
	}

	private ArrayList<tree> getTrees(Dataset ds) {
		logger.debug("getting trees ...");
		ArrayList<tree> res = ds.getTrees();

		logger.debug("got " + res.size() + " trees.");
		return res;
	}

	public void updateOutputEstimate(String mainTree, HashSet<String> treesToCopy, HashSet<String> branchesToKeep, String cutCode) {
		if (mainTree.equalsIgnoreCase("undefined") || branchesToKeep.size() == 0)
			return;
		logger.info("getting estimates. mainTree:" + mainTree);
		for (String t : treesToCopy)
			logger.info("tree to copy:" + t);

		inpEvents = 0;
		estSize = 0;
		estEvents = 0;
		estBranches = 0;

		if (treesToCopy.contains(mainTree)) {
			treesToCopy.remove(mainTree);
		}

		for (String ttc : treesToCopy) {
			logger.info("adding to the estimate a full size of treeToCopy: " + ttc);
			tree t = getTree(ttc);
			if (t != null)
				estSize += t.getSize();
			else
				logger.error("can't be that tree was not found");
		}

		tree t = getTree(mainTree);
		inpEvents = t.getEvents();
		for (Map.Entry<String, Long> b : t.getBranches().entrySet()) {
			for (String inp : branchesToKeep) {
				String matchString = inp.replace("*", "\\w+");
				if (b.getKey().matches(matchString)) {
					logger.debug("branch selected: " + b.getKey() + "\tsize: " + b.getValue());
					estSize += b.getValue();
					estBranches++;
				}
			}
		}

		estEvents = inpEvents;

		return;
	}

	public String getOutputEstimate() {
		return inpEvents + ":" + estEvents + ":" + estSize + ":" + estBranches;
	}

	public void insertJob(String outdataset, String mainTree, HashSet<String> treesToCopy, HashSet<String> branchesToKeep, String cutCode, String deliverTo) {

		String inputdatasets = "";
		for (Dataset ds : dSets) {
			inputdatasets += ds.name + "\n";
		}
		inputdatasets = inputdatasets.trim();

		String branches = "";
		for (String br : branchesToKeep) {
			branches += br + "\n";
		}
		branches = branches.trim();
		String tToCopy = "";
		for (String tr : treesToCopy) {
			tToCopy += tr + "\n";
		}
		tToCopy = tToCopy.trim();

		Submitter s = new Submitter();
		s.setValues(inputdatasets, outdataset, branches, cutCode, mainTree, tToCopy, deliverTo, inpEvents, getInputSize(), estSize, estEvents);

		for (Dataset ds : dSets) {
			ArrayList<RootFile> arf = ds.alRootFiles;
			for (RootFile rf : arf) {
				s.setFile(rf.getFullgLFN(), rf.size, rf.getEventsInTree(mainTree));
			}
		}
		s.start();

	}
}
