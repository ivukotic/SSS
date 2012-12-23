package edu.uchicago.SSSserver;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.SlicedChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.SSSserver.Dataset.RootFile;

public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

	private final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	final Logger logger = LoggerFactory.getLogger(HttpRequestHandler.class);
	private HttpRequest request;
	private boolean readingChunks;
	/** Buffer that stores the response content */
	private StringBuilder buf = new StringBuilder();
	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	public String runInspector(){
		String res="";
		for (Dataset ds: dSets){
			try {
				for (RootFile rf:ds.alRootFiles){
					Runtime rt = Runtime.getRuntime();
					String comm = "./inspector " + ds.gLFNpath+"/"+rf.name;
					logger.info("executing >" + comm + "<");
					Process pr = rt.exec(comm);
					BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

					String line = null;
					while ((line = input.readLine()) != null) {
						logger.debug(line);
						res+=line+"\n";
					}
				}
			} catch (Exception e) {
				logger.error(e.toString());
				e.printStackTrace();
			}
		}
		return res;
	}
	
	public void queryDQ2(String ds) {
		try {
			Dataset dSet = new Dataset(ds);

			Runtime rt = Runtime.getRuntime();
			String comm = "dq2-ls -f " + ds;
			logger.info("executing >" + comm + "<");
			Process pr = rt.exec(comm);

			BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

			String line = null;
			while ((line = input.readLine()) != null) {
				if (line.indexOf(".root") > 0) {
					logger.debug(line);
					String[] r = line.split("\t");
					if (r.length == 5)
						dSet.addFile(r[1], r[2], Long.parseLong(r[4]));
				}
			}

			int exitVal = pr.waitFor();
			logger.info("Exited with code " + exitVal);
			if (exitVal == 0)
				dSets.add(dSet);

			// get gLFN path 
			comm = "dq2-list-files -p " + ds;
			logger.info("executing >" + comm + "<");
			Process pr1 = rt.exec(comm);
			BufferedReader input1 = new BufferedReader(new InputStreamReader(pr1.getInputStream()));
			line=null;
			if ((line = input1.readLine()) != null){
				logger.info(line);
				line=line.substring(0,line.lastIndexOf("/"));
				logger.info(line);
			}
			exitVal = pr1.waitFor();
			logger.info("Exited with code " + exitVal);
			
			if (exitVal == 0)
				dSet.setPath(line);

		} catch (Exception e) {
			logger.error(e.toString());
			e.printStackTrace();
		}
		
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		logger.info("MessageReceived.");
		dSets.clear();
		
		if (!readingChunks) {
			HttpRequest request = this.request = (HttpRequest) e.getMessage();
			SlicedChannelBuffer scb = (SlicedChannelBuffer) request.getContent();
			logger.info("content: " + scb.toString(UTF8_CHARSET));
			
			Integer step=-1;
			String ds = scb.toString(UTF8_CHARSET);
			String[] pars = ds.split("&");
			String sStep=pars[0];
			String[] sSplit=sStep.split("=");
			if (sSplit[0].equals("step")){
				step=Integer.parseInt(sSplit[1]);
			} else return;
			logger.info("step: "+step.toString());
			
			String[] r = pars[1].split("=");
			if (step==0 && r[0].equals("inds")) {
				String[] dss = r[1].split(",");
				for (String d : dss) {
					queryDQ2(d);
				}
			}

			if (is100ContinueExpected(request)) {
				send100Continue(e);
			}

			long totsize=0;
			for (Dataset das : dSets) {
				totsize+=das.size;
			}

			buf.setLength(0);
			buf.append("size:" + String.valueOf(totsize)+"\n");
			buf.append(runInspector());
			
			if (request.isChunked()) {
				readingChunks = true;
			} else {
				writeResponse(e);
			}
		}
	}

	private void writeResponse(MessageEvent e) {
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		logger.info("returns:\n" + buf.toString());
		response.setContent(ChannelBuffers.copiedBuffer(buf, CharsetUtil.UTF_8));
		
		response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

		if (keepAlive) {
			// Add 'Content-Length' header only for a keep-alive connection.
			response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
		}

		// Encode the cookie.
		String cookieString = request.getHeader(COOKIE);
		if (cookieString != null) {
			CookieDecoder cookieDecoder = new CookieDecoder();
			Set<Cookie> cookies = cookieDecoder.decode(cookieString);
			if (!cookies.isEmpty()) {
				// Reset the cookies if necessary.
				CookieEncoder cookieEncoder = new CookieEncoder(true);
				for (Cookie cookie : cookies) {
					cookieEncoder.addCookie(cookie);
				}
				response.addHeader(SET_COOKIE, cookieEncoder.encode());
			}
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);
		
		// Close the non-keep-alive connection after the write operation is
		// done.
		if (!keepAlive) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
		
	}

	private void send100Continue(MessageEvent e) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
		e.getChannel().write(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}