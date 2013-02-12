package edu.uchicago.SSSexecutor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

	final Logger logger = LoggerFactory.getLogger(Task.class);

	public Integer id = 0;
	public String outFile;
	public String dataset;
	public String cut;
	public String branches;
	public String tree;
	public String treesToCopy;
	public String deliverTo;
	List<String> inputFiles = new ArrayList<String>();

	public void print() {
		logger.info("taskID:       " + id.toString());
		logger.info("outFile:      " + outFile);
		logger.info("dataset:      " + dataset);
		logger.info("cut:          " + cut);
		logger.info("tree:         " + tree);
		logger.info("treesToCopy:  " + treesToCopy);
		logger.info("branches:     " + branches);
		logger.info("deliverTo:    " + deliverTo);
		logger.info("inputFiles:   ");
		for (String s : inputFiles) {
			logger.info(s);
		}
	}

	public void createFiles() {
		String fn = "SSS_" + id + ".";

		String redi = System.getenv("STORAGEPREFIX");
		if (redi == null) {
			logger.info("no STORAGEPREFIX defined. Using global redirector: root://glrd.usatlas.org/ .");
		} else {
			logger.info("using redirector: " + redi);
		}

		logger.info("Creating inputFiles file.");
		try { // input files
			FileWriter fstream = new FileWriter(fn + "inputFileList");
			BufferedWriter out = new BufferedWriter(fstream);
			for (String s : inputFiles) {
				if (redi != null)
					s=s.replaceFirst("root://glrd.usatlas.org/", redi);
				out.write(s + "\n");
			}
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		logger.info("Creating list of branches file.");
		try { // variables to keep
			FileWriter fstream = new FileWriter(fn + "branchesList");
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(branches);
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		logger.info("Creating file with filter code.");
		if (cut != null) {
			try { // cutCode
				FileWriter fstream = new FileWriter(fn + "cutCode");
				BufferedWriter out = new BufferedWriter(fstream);
				out.write(cut);
				out.close();
			} catch (Exception ex) {
				logger.error(ex.getMessage());
			}
		}

		logger.info("Creating script to execute.");
		try { // script to execute
			FileWriter fstream = new FileWriter(fn + "sh");
			BufferedWriter out = new BufferedWriter(fstream);
			String res = "#!/bin/zsh\n";
			res += "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n";
			res += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\n";
			res += "source $AtlasSetup/scripts/asetup.sh 17.6.0,noTest\n";
			res += "export X509_USER_PROXY=x509up_u20074\n";
			res += "source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalDQ2ClientSetup.sh --dq2ClientVersion current --skipConfirm\n";
			res += "export DQ2_LOCAL_SITE_ID=MWT2_UC_USERDISK\n";
			res += "python filter-and-merge-d3pd.py ";
			res += " --in=" + fn + "inputFileList";
			res += " --outDS="+dataset;
			res += " --out=" + outFile;
			res += " --tree=" + tree;
			res += " --var=" + fn + "branchesList";
			if (cut != null)
				res += " --selection=" + fn + "cutCode";
			if (treesToCopy != null)
				res += " --keep-all-trees";
			out.write(res);
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		logger.info("All files created");
	}

}
