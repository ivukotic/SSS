package edu.uchicago.SSSserver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataContainer {

	final Logger logger = LoggerFactory.getLogger(DataContainer.class);

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

	private tree getTree(String name) {
		for (tree t : summedTrees) {
			if (t.getName().equals(name)) {
				return t;
			}
		}
		tree t=new tree();
		summedTrees.add(t);
		return t;
	}

	private void updateTrees() {

		processedfiles = 0;
		totalfiles = 0;
		summedTrees.clear();

		for (Dataset ds : dSets) {
			ArrayList<tree> ts = getTrees(ds);
			if (ts.size() == 0)
				continue;

			processedfiles += ds.processed;
			totalfiles += ds.alRootFiles.size();

			for (tree t : ts) {
				tree st=getTree(t.getName());
				st.add(t);
			}

		}
	}

	public String getTreeDetails() {
		updateTrees();

		String res = summedTrees.size() + "\n";
		for (tree st : summedTrees) {
			res += st.getName() + ":" + st.getEvents() + ":" + st.getSize() + ":" + st.getNBranches() + "\n";
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
		} 
		return res;
	}

	public String getOutputEstimate(String mainTree, HashSet<String> treesToCopy, HashSet<String> branchesToKeep, String cutCode) {
		if (mainTree.equalsIgnoreCase("undefined") || branchesToKeep.size() == 0)
			return "0:0:0:0";
		logger.info("getting estimates. mainTree:" + mainTree);
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

		return inpEvents + ":" + estEvents + ":" + estSize + ":" + estBranches;
	}

	public void createCondorInputFiles(String mainTree, HashSet<String> treesToCopy, HashSet<String> branchesToKeep, String cutCode) {
		SimpleDateFormat sDF = new SimpleDateFormat("SSS_ddMMyy-hhmmss.");
		String fn = sDF.format(new Date());

		// njobs=0;
		// decide how many files per job to submit.

		try { // input files - this should be split into number of jobs.
			FileWriter fstream = new FileWriter(fn + "inputFileList");
			BufferedWriter out = new BufferedWriter(fstream);
			for (Dataset ds : dSets) {
				ArrayList<RootFile> arf = ds.alRootFiles;
				for (RootFile rf : arf)
					out.write(rf.getFullgLFN());
			}
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		try { // variables to keep
			FileWriter fstream = new FileWriter(fn + "inputFileList");
			BufferedWriter out = new BufferedWriter(fstream);
			for (Dataset ds : dSets) {
				ArrayList<RootFile> arf = ds.alRootFiles;
				for (RootFile rf : arf)
					out.write(rf.getFullgLFN());
			}
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

	}
}
