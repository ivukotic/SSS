package edu.uchicago.SSSexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class Dq2Puter {

	final Logger logger = LoggerFactory.getLogger(Dq2Puter.class);

//	Integer 
	void put(Task task) {
		logger.info("Creating dq2-put script to execute.");

		String fn = "SSS_uploader_" + task.id + ".sh";
		try { // script to execute

			Path path = FileSystems.getDefault().getPath(".", fn);
			BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8);

			// FileWriter fstream = new FileWriter(fn);
			// BufferedWriter out = new BufferedWriter(fstream);

			String res = "#!/bin/zsh\n";
			res += "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n";
			res += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\n";
			res += "source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalDQ2ClientSetup.sh --dq2ClientVersion current --skipConfirm\n";
			res += "export DQ2_LOCAL_SITE_ID=MWT2_UC_USERDISK\n";
			res += "export X509_USER_PROXY=x509up_u20074\n";
			res += "dq2-put -C -a -f " + task.outFile;
			if (task.deliverTo != null)
				res += " -L" + fn + task.deliverTo;
			res += " " + "user.ivukotic.SSS." + task.dataset + "\n";
			out.write(res);
			out.close();

			path.toFile().setExecutable(true);

		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

//		// start it
//		Runtime rt = Runtime.getRuntime();
//		String[] comm = { "./" + fn };
//		try {
//			Process proc = rt.exec(comm);
//
//			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
//			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
//
//			String s;
//			// read any errors from the dq2-put shell script
//			while ((s = stdError.readLine()) != null) {
//				logger.error("there is an error from dq2-put command...");
//				logger.error(s);
//			}
//
//			// read the output from the dq2-put shell script
//			logger.info("start output:\n");
//			String siz = "";
//			while ((s = stdInput.readLine()) != null) {
//				logger.info(s);
//				if (s.contains("to SE: OK")) {
//					siz = s.substring(s.indexOf("size:") + 5, s.indexOf(")"));
//				}
//			}
//			if (siz.length() > 0) {
//				logger.info("dq2-put was successful. File size is: " + siz);
//				return Integer.parseInt(siz);
//			}
//
//		} catch (Exception ex) {
//			logger.error(ex.getMessage());
//		}
//
//		return 0;

	}
}