package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class tree {

	final Logger logger = LoggerFactory.getLogger(tree.class);
	
	public String name;
	public long events;
	public long size;
	
	public ArrayList<branch> branches = new ArrayList<branch>();
	
	tree(){}
	tree(String na, long ev, long si){
		name=na;
		events=ev;
		size=si;
		logger.debug("created tree: "+toString());
	}
	
	public void addBranch(String name, long size){
		branches.add(new branch(name,size));
	}
	
	public int getNBranches(){
		return branches.size();
	}
	
	public ArrayList<branch> getBranches(){
		return branches;
	}
	
	public long getBranchSize(String brname){
		for (branch b: branches){
			if (b.name.equalsIgnoreCase(brname))
				return b.size;
		}
		logger.warn("looking for unexisting branch");
		return 0;
	}
	
	public long getBranchSize(int brindex){
		return branches.get(brindex).size;
	}
	
	public String toString(){
		return "tree:"+name+" entries:"+events+" size:"+size+" branches:"+branches.size();
	}
}
