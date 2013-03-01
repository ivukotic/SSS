package edu.uchicago.SSSserver;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
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

public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

	final Logger logger = LoggerFactory.getLogger(HttpRequestHandler.class);
	private HttpRequest request;
	private boolean readingChunks;
	/** Buffer that stores the response content */
	private StringBuilder buf = new StringBuilder();
	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	private DataSetsBuffer DSB;

	HttpRequestHandler(DataSetsBuffer buf) {
		logger.info("new HttpRequestHandler created...");
		DSB = buf;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		logger.info("MessageReceived.");
//		logger.info("Message: " + e.toString());

		dSets.clear();

		if (!readingChunks) {
			HttpRequest request = this.request = (HttpRequest) e.getMessage();

			if (is100ContinueExpected(request)) {
				logger.info("This was is100ContinueExpected message.");
				send100Continue(e);
			}

			logger.debug("Message headers: " + request.getHeaders().toString());

			ChannelBuffer content = request.getContent();
			if (content.readable()) {
				logger.debug("CONTENT: >" + content.toString(CharsetUtil.UTF_8) + "<");
			} else {
				logger.error("content unreadable!!!");
				return;
			}

			String mes = content.toString(CharsetUtil.UTF_8);
			String[] pars = mes.split("&");

			for (String par : pars) {
				logger.info(par);
			}
			logger.info("------------------------------------------------");

			if (pars.length < 8) {
				logger.error("Not enough parameters.");
				return;
			}

			buf.setLength(0);

			logger.info("datasets xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			String[] sSplit = pars[0].split("=");

			if (!sSplit[0].equals("inds"))
				return;
			
			logger.info("inds: " + sSplit[1]);
			String[] dss = sSplit[1].split(",");

			DataContainer DC = DSB.getContainer(dss);

			long totsize = DC.getInputSize();
			if (totsize<0) {
				buf.append("warning:at least one of the datasets does not exist, or has no root files.");
				writeResponse(e);
				return;
			}

			logger.info("sizes of aLL input DSs have been found xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			String treeDetails=DC.getTreeDetails();
			

			if (true){
				Thread.sleep(500000);
//				writeResponse(e);
				return;
			}
			
			buf.append("size:" + String.valueOf(totsize) + "\n");
			buf.append(treeDetails);
			
			logger.info("trees xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

			sSplit = pars[1].split("=");
			if (!sSplit[0].equals("mainTree"))
				return;
			String mainTree = null;
			if (sSplit.length == 2) {
				mainTree = sSplit[1];
				logger.info("mainTree: " + mainTree);
				buf.append(mainTree + "\n");
			} else {
				buf.append("noTree\n");
			}

			sSplit = pars[2].split("=");
			if (!sSplit[0].equals("treesToCopy"))
				return;
			HashSet<String> treesToCopy = new HashSet<String>();
			if (sSplit.length == 1) {
				logger.info("No trees to copy selected.");
				buf.append("NoTree\n");
			} else {
				String[] ttC = sSplit[1].split(",");
				for (String c : ttC)
					treesToCopy.add(c);
				logger.info("treesToCopy: " + sSplit[1]);
				buf.append(sSplit[1] + "\n");
			}

			logger.info("branches xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			sSplit = pars[3].split("=");
			if (!sSplit[0].equals("branchesToKeep"))
				return;
			HashSet<String> branchesToKeep = new HashSet<String>();
			if (sSplit.length == 1) {
				logger.info("No branches to keep selected.");
			} else {
				String[] byComma = sSplit[1].split(",");
				for (String s : byComma) {
					String[] byNewLine = s.split("\n");
					for (String n : byNewLine)
						branchesToKeep.add(n);
				}
				logger.info("branches to keep: " + sSplit[1]);
			}

			logger.info("cut code xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			if (!pars[4].startsWith("cutCode="))
				return;
			String cutCode = pars[4].substring(8);
			logger.info("cut code: " + cutCode);
			
			buf.append(DC.getOutputEstimate(mainTree, treesToCopy, branchesToKeep, cutCode));
			// ==================================================

			sSplit = pars[5].split("=");
			if (!sSplit[0].equals("sReq"))
				return;
			
			if (sSplit[1].equals("1")) {
				logger.info("submit request xxxxxxxxxxxxxxxxxxxxxxxxxx");

				// create input files, submit script
//				DC.createCondorInputFiles(mainTree, treesToCopy, branchesToKeep, cutCode);
				
				// execute condor_submit
				String outDS=null;
				sSplit = pars[6].split("=");
				if (!sSplit[0].equals("outDS"))
					return;

				if (sSplit.length == 1) {
					logger.error("No outDS !");
				} else {
					outDS=sSplit[1];
					logger.info("outDS: " + outDS);
				}

				String deliverTo=null;
				sSplit = pars[7].split("=");
				if (!sSplit[0].equals("deliverTo"))
					return;

				if (sSplit.length == 1) {
					logger.info("No delivery");
				} else {
					deliverTo=sSplit[1];
					logger.info("deliverTo: " + deliverTo);
				}

				DC.insertJob(outDS, mainTree, treesToCopy, branchesToKeep, cutCode, deliverTo);
				logger.debug("submitted");
				
				buf.append("\nYour job has been submitted.");
			}
			else buf.append("\nOK");
			// ==================================================

			if (request.isChunked()) {
				readingChunks = true;
			} else {
				writeResponse(e);

				logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
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
		logger.debug("eXcEpTiOn caught. client broke connection.");
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}