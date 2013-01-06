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
	ArrayList<tree> trees;

	public float processed;

	Dataset(String na) {
		name = na;
		alRootFiles = new ArrayList<RootFile>();
		trees = new ArrayList<tree>();
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

	public ArrayList<tree> getTrees() {
		if (processed < 1)
			updateTrees();
		return trees;
	}

	public void updateTrees() {
		processed = 0;
		boolean first = true;
		for (RootFile rf : alRootFiles) {
			ArrayList<tree> ts = rf.getTrees();
			for (tree t : ts) {
				if (first) {
					trees.add(new tree(t.name, t.events, t.size));
					first = false;
					// need to copy branches too.
				} else {
					boolean found = false;
					for (tree st : trees) {
						if (st.name.equalsIgnoreCase(t.name)) {
							st.events += t.events;
							st.size += t.size;
							// need to addup branches too.
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

		} catch (Exception e) {
			logger.error(e.toString());
			e.printStackTrace();
		}

	}

}
