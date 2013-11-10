package edu.uchicago.SSSserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class RootFile {

	final private static Logger logger = Logger.getLogger(RootFile.class);

	public String name;
	public String guid;
	public long size;
	private String gLFN;
	private ArrayList<tree> trees;
	boolean started;
	boolean done;

	RootFile(String na, String gu, long si) {
		name = na;
		guid = gu;
		size = si;
		logger.info("added file: " + toString());
		gLFN="";
		trees = new ArrayList<tree>();
		started = false;
		done = false;
	}

	public ArrayList<tree> getTrees() {
		if (trees.size() == 0 && !started && !done) {
			Inspector i = new Inspector();
			started = true;
			i.start();
		}
		return trees;
	}

	public Long getEventsInTree(String treename) {
		for (tree t : trees) {
			if (t.getName().equals(treename))
				return t.getEvents();
		}
		return 0L;
	}

	public void setGLFN(String glfn) {
		gLFN = glfn;
	}

	public String getFullgLFN() {
		return gLFN + "/" + name;
	}

	private class Inspector extends Thread {

		public void run() {
			try {
				if (gLFN.equalsIgnoreCase(""))
					return;
				trees.clear();
				logger.info("Inspecting  >" + gLFN + "/" + name + "<");
				Process p = new ProcessBuilder("./inspector", gLFN + "/" + name).start();
				BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
				BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				
				while (!input.ready() && !error.ready())
					sleep(1000);
				logger.error(error.readLine());
				
				String line;

				Integer nt = -1;
				while ((line = input.readLine()) != null) {
					logger.info(line);
					if (line.startsWith("ntrees:")) {
						nt = Integer.parseInt(line.substring(7));
						break;
					}
				}
				if (nt == -1) {
					logger.error("problem: should have returned number of trees in file.");
					started = false;
					return;
				} else {
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
							line = input.readLine();
							if (line == null)
								continue;
							else
								line = line.trim();
							String[] br = line.split("\t");
							if (br.length == 2) {
								toa.addBranch(br[0], Long.parseLong(br[1]));
							} else {
								logger.error("problematic line" + line);
							}
						}
						logger.info(toa.toString());
						trees.add(toa);
					}
				}

			} catch (Exception e) {
				logger.error("unrecognized exception: " + e.getMessage());
				logger.error(e.toString());
				e.printStackTrace();
				started = false;
				done = false;
			}
			done = true;

		}

	}

	public String toString() {
		return "ROOT file: " + name + "\t bytes: " + size;// + "\tGUID: " +
															// guid;
	}
}
