package edu.uchicago.SSSserver;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class tree {

	final Logger logger = LoggerFactory.getLogger(tree.class);
	
	private String name="";
	private long events;
	private long size;
	
	private HashMap<String,Long> m_branches=new HashMap<String, Long>();
	
	tree(){}
	tree(String na, long ev, long si){
		name=na;
		events=ev;
		size=si;
		logger.debug("created tree: "+toString());
	}
	
	public void addBranch(String name, long size){
		m_branches.put(name, size);
	}
	
	public String getName(){
		return name;
	}
	
	public long getEvents(){
		return events;
	}

	public long getSize(){
		return size;
	}
	
	public int getNBranches(){
		return m_branches.size();
	}

	public long getBranchSize(String brname){
		return m_branches.get(brname);
	}
	
	public HashMap<String,Long> getBranches(){
		return m_branches;
	}
	
	public void add(tree treeToAdd){
		if (!name.equals(treeToAdd.name)){
			if (name.isEmpty()) {
				name=treeToAdd.name;
				for (Map.Entry<String,Long> br:treeToAdd.getBranches().entrySet()){
					m_branches.put(br.getKey(), 0L);
				}
			}else{
				logger.error("Can't sum up different trees: "+name+" and "+treeToAdd.name);
				return;
			}
		}
		events+=treeToAdd.getEvents();
		size+=treeToAdd.getSize();
		for (Map.Entry<String,Long> br:m_branches.entrySet()){
			Long ntbs=treeToAdd.getBranchSize(br.getKey());
			if (ntbs==null) {
				logger.error("branch: "+br.getKey()+" does is missing from one of the files.");
			}else{
				br.setValue(br.getValue()+ntbs);
			}
		}
		
	}
	
	public String toString(){
		return "tree:"+name+" entries:"+events+" size:"+size+" branches:"+m_branches.size();
	}
}
