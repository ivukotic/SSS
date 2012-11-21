package edu.uchicago.SSSserver;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class SSSserver {


	SSSserver() {
	}

//	public void send(String res) {
//
//		DatagramChannelFactory f = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
//
//		ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
//
//		// Configure the pipeline factory.
//		b.setPipelineFactory(new ChannelPipelineFactory() {
//			public ChannelPipeline getPipeline() throws Exception {
//				return Channels.pipeline(new StringEncoder(CharsetUtil.ISO_8859_1), new StringDecoder(CharsetUtil.ISO_8859_1),
//						new SimpleChannelUpstreamHandler());
//			}
//		});
//
//		b.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024));
//
//		DatagramChannel c = (DatagramChannel) b.bind(new InetSocketAddress(0));
//
//		// Broadcast package
//		c.write(res, new InetSocketAddress(address, port));
//
//		// will close the DatagramChannel when a
//		// response is received. If the channel is not closed within 100
//		// milliseconds,
//		// print an error message and quit.
//		if (!c.getCloseFuture().awaitUninterruptibly(100)) {
//			System.err.println("Done.");
//			c.close().awaitUninterruptibly();
//		}
//
//		f.releaseExternalResources();
//	}


	public static void main(String[] args) {
		System.out.println("starting up ...");


		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new HttpServerPipelineFactory());

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(8080));
		
		
		System.out.println("Setting up dq2 environment ...");
		System.out.println("connecting to ORACLE server ...");

//		dbreader dbr = new dbreader(args[0], args[1], args[2], args[3], args[6]);
//		dbr.connect();

		// sender r = new sender("atl-prod05.slac.stanford.edu",9931);
		// SSSserver r = new SSSserver(args[4], Integer.parseInt(args[5]) );

//		while (true) {
//			System.out.println("----------------");
//			String res = dbr.sel();
//			System.out.println(res);
//			r.send(res);
//			try {
//				Thread.currentThread().sleep(60000);
//			} 
//			catch (Exception e) {
//			}
//		}
	}

}
