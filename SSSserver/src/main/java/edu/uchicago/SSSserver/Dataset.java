package edu.uchicago.SSSserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dataset {

	final Logger logger = LoggerFactory.getLogger(Dataset.class);

	public String name;
	public String gLFNpath;
	private long size;
	public long events;
	public ArrayList<RootFile> alRootFiles;
	ArrayList<tree> summedTrees;
	public int processed;

	Dataset(String na) {
		name = na;
		alRootFiles = new ArrayList<RootFile>();
		summedTrees = new ArrayList<tree>();
		processed = 0;
		queryDQ2();
	}

	private void setPath(String pat) {
		gLFNpath = pat;
		for (RootFile rf : alRootFiles) {
			rf.setGLFN(gLFNpath);
		}
	}

	public Long getSize() {
		size = 0;
		for (RootFile rf : alRootFiles) {
			size += rf.size;
		}
		return size;
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

	public ArrayList<tree> getTrees() {
		if (processed == alRootFiles.size()) return summedTrees;
		updateTrees();
		return summedTrees;
	}

	private void updateTrees() {
		processed = 0;
		summedTrees.clear();
		for (RootFile rf : alRootFiles) {

			ArrayList<tree> ts = rf.getTrees();
			if (!rf.done || ts.size() == 0)
				continue;

			processed++;

			for (tree t : ts) {
				tree st = getTree(t.getName());
				st.add(t);
			}

		}
	}

	private void queryDQ2() {

		try {
			Runtime rt = Runtime.getRuntime();
			String comm = "dq2-ls -f " + name;
			logger.info("executing >" + comm + "<");
			Process pr = rt.exec(comm);

			BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

			String line = null;
			while ((line = input.readLine()) != null) {
				if (line.indexOf(".root") > 0) {
					logger.debug(line);
					String[] r = line.split("\t");
					if (r.length == 5)
						alRootFiles.add(new RootFile(r[1], r[2], Long.parseLong(r[4])));
				}
			}

			int exitVal = pr.waitFor();
			if (exitVal != 0) {
				logger.error("Problem with 'dq2-ls -f'. Exited with code " + exitVal);
				return;
			}

			// get gLFN path
			comm = "dq2-list-files -p " + name;
			logger.info("executing >" + comm + "<");
			Process pr1 = rt.exec(comm);
			BufferedReader input1 = new BufferedReader(new InputStreamReader(pr1.getInputStream()));
			line = null;
			if ((line = input1.readLine()) != null) {
				logger.debug(line);
				line = line.substring(0, line.lastIndexOf("/"));
				logger.info("DS gLFN: " + line);
			}

			exitVal = pr1.waitFor();
			if (exitVal != 0) {
				logger.error("PROBLEM Exited with code " + exitVal);
			} else {
				setPath(line);
			}
			logger.debug("dq2-list-files finished OK.");
		} catch (Exception e) {
			logger.error(e.toString());
			e.printStackTrace();
		}

	}

}
