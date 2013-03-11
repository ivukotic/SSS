package edu.uchicago.SSSserver;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSetsBuffer {

	final Logger logger = LoggerFactory.getLogger(DataSetsBuffer.class);

	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();
    private ArrayList<Response> responses = new ArrayList<Response>();
    
	public DataContainer getContainer(String[] sDSs){
		logger.info("creating new DataContainer.");
    	DataContainer DC=new DataContainer();
    	for (String ds : sDSs) {
    		DC.add(getDSfromName(ds));
    	}
    	return DC;
    }
    
	private Dataset getDSfromName(String name) {
		for (Dataset ds : dSets) {
			if (ds.name.equalsIgnoreCase(name)) {
				logger.info("DS: " + name + " FOUND in BUFFER.");
				return ds;
			}
		}
		logger.info("DS: " + name + " not in BUFFER. Adding it.");
		Dataset ds = new Dataset(name);
		dSets.add(ds);

		return ds;
	}

	public Response createResponse(String md5){
		logger.info("Created new response:"+md5);
		Response r=new Response();
		r.md5=md5;
		responses.add(r);
		return r;
	}
	
	public Response getResponse(String md5){
		for (Response r: responses){
			if (md5.equals(r.md5)) {
				logger.info("request: "+md5+" found.");
				return r;
				}
		}
		logger.info("request with md5: "+md5+" not found.");
		return null;
	}
	
}
