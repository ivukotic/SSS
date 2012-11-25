package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dataset {

	final Logger logger = LoggerFactory.getLogger(Dataset.class);
	
	public String name;
	public int size;
	public int events;
	public ArrayList<RootFile> alRootFiles;
	
	public class RootFile{
		public String name;
		public String guid;
		public int si;
		public int events;
		RootFile(String na, String gu, int si){
			name=na; guid=gu; size=si;
		}
	}
	
	Dataset(String na){
		name=na;
		alRootFiles=new ArrayList<RootFile>();
	}
	public void addFile(String na, String gu, int si){
		alRootFiles.add(new RootFile(na, gu, si));
		logger.info("adding file: "+na+"\tGUID: "+gu+"\t bytes: "+si);
		size+=si;
	}
}
