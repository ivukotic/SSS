package edu.uchicago.SSSserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootFile {

	final Logger logger = LoggerFactory.getLogger(RootFile.class);

	public String name;
	public String guid;
	public long size;
	private String gLFN;
	private ArrayList<tree> trees;
	boolean started;

	RootFile(String na, String gu, long si) {
		name = na;
		guid = gu;
		size = si;
		logger.info("added file: " + toString());
		trees = new ArrayList<tree>();
		started = false;
	}

	public ArrayList<tree> getTrees() {
		if (trees.size() == 0 && !started) {
			Inspector i = new Inspector();
			started=true;
			i.start();
		}
		return trees;
	}

	public void setGLFN(String glfn) {
		gLFN = glfn;
	}

	private class Inspector extends Thread {

		public void run() {
			try {
				if (gLFN.equalsIgnoreCase(""))
					return;
				Runtime rt = Runtime.getRuntime();
				String comm = "./inspector " + gLFN + "/" + name;
				logger.info("executing >" + comm + "<");
				Process pr = rt.exec(comm);
				BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

				String line = null;
				line = input.readLine();
				logger.debug(line);
				if (line == null) {
					logger.error("problem. nothing returned.");
				} else {
					int nt = Integer.parseInt(line.trim());
					for (int i = 0; i < nt; i++) {
						line = input.readLine();
						logger.debug(line);
						String[] parts = line.split(":");
						if (parts.length != 4) {
							logger.error("problem in getting tree name, size, events");
							continue;
						}
						Integer events = Integer.parseInt(parts[1]);
						Long size = Long.parseLong(parts[2]);
						Integer branches = Integer.parseInt(parts[3]);

						tree toa = new tree(parts[0], events, size);
						for (int j = 0; j < branches; j++) {
							line = input.readLine().trim();
							if (line == null)
								continue;
							String[] br = line.split(":");
							toa.addBranch(br[0], Long.parseLong(br[1]));
						}
						trees.add(toa);
					}
				}

			} catch (Exception e) {
				logger.error("unrecognized exception: " + e.getMessage());
			}
		}

	}

	public String toString() {
		return "ROOT file: " + name + "\tGUID: " + guid + "\t bytes: " + size;
	}
}
