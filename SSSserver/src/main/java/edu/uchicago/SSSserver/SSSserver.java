package edu.uchicago.SSSserver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class SSSserver {

	final private static Logger logger = Logger.getLogger(SSSserver.class);
	
	SSSserver() {
		PropertyConfigurator.configure(SSSserver.class.getClassLoader().getResource("log4j.properties"));
	}

	private void start(){
		logger.info("starting up ...");
		
		try { // write process number so shell can restart it if it crashes.
			FileWriter fstream = new FileWriter("/tmp/.SSSserver.proc");
			BufferedWriter out = new BufferedWriter(fstream);
			String[] pr=ManagementFactory.getRuntimeMXBean().getName().split("@");
			out.write(pr[0]+"\n");
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		
		// this handles IO threads and their pools.
		ChannelFactory factory=new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool()
				);
		
		// this helps setup server
		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new HttpServerPipelineFactory());

		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("child.tcpNoDelay", true);
		
		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(8080));
		
		logger.info("Waiting for connections ...");

		
	}


	public static void main(String[] args) {

		SSSserver Ss=new SSSserver();
		Ss.start();

	}

}
