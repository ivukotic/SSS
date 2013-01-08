package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSetsBuffer {

	final Logger logger = LoggerFactory.getLogger(DataSetsBuffer.class);

	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	private Dataset getDSfromName(String name) {
		for (Dataset ds : dSets) {
			if (ds.name.equalsIgnoreCase(name)) {
				logger.info("DS: " + name + " FOUND in BUFFER.");
				return ds;
			}
		}
		logger.info("DS: " + name + "not in BUFFER.");
		return null;
	}

	// returns size in bytes of the DS
	public long getInputSize(String[] sDSs) {
		long res = 0;
		for (String DS : sDSs) {
			res += getDSSize(DS);
		}
		return res;
	}

	private long getDSSize(String ds) {
		
		Dataset DS=getDSfromName(ds);
		if (DS==null){
			logger.info("DS not found in the buffer. Adding it.");
			Dataset dSet = new Dataset(ds);
			dSets.add(dSet);
		}
		else{
			long res = DS.getSize();
			if (res > 0)
				return res;
		}

		return getDSSize(ds);
	}

	public String getTreeDetails(String[] sDSs) {
		int processedfiles = 0;
		int totalfiles = 0;
		ArrayList<tree> summedTrees = new ArrayList<tree>();
		boolean first = true;
		for (String DS : sDSs) {
			Dataset ds = getDSfromName(DS);
			if (ds == null) {
				logger.error("ds not found. should not happen.");
				break;
			}

			ArrayList<tree> ts = getTrees(ds);
			if (ts.size() == 0)
				continue;

			processedfiles += ds.processed;
			totalfiles += ds.alRootFiles.size();
			if (first) {
				for (tree t : ts) {
					tree crtree=new tree(t.name, t.events, t.size);
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
							for (branch b:st.branches){
								b.size+=t.getBranchSize(b.name);
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
		res+=totalfiles+":"+processedfiles+"\n";
		logger.info("total files: "+totalfiles+"\tprocessed files: "+processedfiles+"\n");
		return res;
	}

	private ArrayList<tree> getTrees(Dataset ds) {
		logger.info("getting tree info from DS: "+ds.name);
		ArrayList<tree> res = ds.getTrees();
		if (res.size() == 0) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			res = getTrees(ds);
		}else{
			logger.info("not yet ready. trying again.");
		}
		return res;
	}

}
