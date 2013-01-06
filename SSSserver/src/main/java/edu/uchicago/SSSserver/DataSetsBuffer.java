package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSetsBuffer {

	final Logger logger = LoggerFactory.getLogger(DataSetsBuffer.class);

	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	// returns size in bytes of the DS
	public long getInputSize(String[] sDSs) {
		long res = 0;
		for (String DS : sDSs) {
			res += getDSSize(DS);
		}
		return res;
	}

	private long getDSSize(String ds) {
		long res = -1;
		for (Dataset DS : dSets) {
			if (DS.name.equalsIgnoreCase(ds)) {
				logger.info("getDSSize DS: " + ds + " FOUND in the BUFFER");
				res = DS.getSize();
				if (res>0)
					return res;
			}
		}

		if (res == -1) {
			logger.info("DS not found in the buffer. Adding it.");
			Dataset dSet = new Dataset(ds);
			dSets.add(dSet);
		}
		
		try {
			logger.info("waiting 1 second.");
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return getDSSize(ds);
	}
	

	public String getTreeDetails(String[] sDSs) {
		
		ArrayList<tree> summedTrees=new ArrayList<tree>();
		boolean first=true;
		for (String DS : sDSs) {
			ArrayList<tree> ts = getTrees(DS);
			for (tree t: ts){
				if (first) {
					summedTrees.add(new tree(t.name,t.events,t.size));
					first=false;
					// need to copy branches too.
				}else{
					boolean found=false;
					for (tree st: summedTrees){
						if (st.name.equalsIgnoreCase(t.name) ){
							st.events+=t.events;
							st.size+=t.size;
							// need to addup branches too.
							found=true;
							continue;
						}
					}
					if (!found){
						logger.error("This file contains a tree not seen in the first file. Tree skipped.");
					}
				}
			}
		}

		String res=summedTrees.size()+"\n";
		for (tree st:summedTrees){
			res+=st.name+":"+st.events+":"+st.size+":"+st.getNBranches()+"\n";
		}
		return res;
	}

	private ArrayList<tree> getTrees(String ds){

		for (Dataset DS : dSets) {
			if (DS.name.equalsIgnoreCase(ds)) {
				logger.info("getTrees  DS: " + ds + " FOUND in the BUFFER.");

				ArrayList<tree> res=DS.getTrees();
				if (res.size()==0) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					res=getTrees(ds);
				}
				
				return res;
			}
		}
		logger.error("Should not happen: DS is not in buffer.");
		return null;
	}
	
}
