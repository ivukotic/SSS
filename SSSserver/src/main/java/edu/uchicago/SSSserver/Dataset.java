package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dataset {

	final Logger logger = LoggerFactory.getLogger(Dataset.class);
	
	public String name;
	public long size;
	public long events;
	public ArrayList<RootFile> alRootFiles;
	
	public class RootFile{
		public String name;
		public String guid;
		public long si;
		public long events;
		RootFile(String na, String gu, long si){
			name=na; guid=gu; size=si;
		}
	}
	
	Dataset(String na){
		name=na;
		alRootFiles=new ArrayList<RootFile>();
	}
	public void addFile(String na, String gu, long si){
		alRootFiles.add(new RootFile(na, gu, si));
		logger.debug("adding file: "+na+"\tGUID: "+gu+"\t bytes: "+si);
		size+=si;
	}
}
