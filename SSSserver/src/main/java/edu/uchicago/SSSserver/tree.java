package edu.uchicago.SSSserver;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class tree {

	final Logger logger = LoggerFactory.getLogger(tree.class);

	private String name = "";
	private Long events;
	private Long size;

	private HashMap<String, Long> m_branches = new HashMap<String, Long>();

	tree() {
		events = 0L;
		size = 0L;
	}

	tree(String na, Integer ev, Long si) {
		name = na;
		events = ev.longValue();
		size = si;
		logger.debug("created tree: " + toString());
	}

	public void addBranch(String name, long size) {
		m_branches.put(name, size);
	}

	public String getName() {
		return name;
	}

	public Long getEvents() {
		return events;
	}

	public Long getSize() {
		return size;
	}

	public int getNBranches() {
		return m_branches.size();
	}

	public Long getBranchSize(String brname) {
		Long res = m_branches.get(brname);
		if (res != null)
			return res;
		logger.info("Branch: " + brname + " not found!");
		return null;
	}

	public HashMap<String, Long> getBranches() {
		return m_branches;
	}

	public void add(tree treeToAdd) {
		// logger.debug("Adding tree: " + treeToAdd.name +
		// " on existing tree named: " + name);
		if (!name.equals(treeToAdd.name)) {
			if (name.isEmpty()) {
				name = treeToAdd.name;
				m_branches.putAll(treeToAdd.m_branches);
				events += treeToAdd.getEvents();
				size += treeToAdd.getSize();
				return;
			} else {
				logger.error("Can't sum up different trees: " + name + " and " + treeToAdd.name);
				return;
			}
		}

		// logger.debug("Adding events,size ");
		events += treeToAdd.getEvents();
		size += treeToAdd.getSize();

		// logger.info("Adding branches. ");
		for (Map.Entry<String, Long> br : m_branches.entrySet()) {
			Long ntbs = treeToAdd.getBranchSize(br.getKey());
			if (ntbs != null) {
				Long prevSize = br.getValue();
				br.setValue(prevSize + ntbs);
			}
			// else
			// logger.debug("branch: "+br.getKey()+" is missing from one of the files.");
		}

		// logger.debug("Branches added. ");
	}

	public String toString() {
		return "tree:" + name + " entries:" + events + " size:" + size + " branches:" + m_branches.size();
	}
}
