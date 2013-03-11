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
	private ArrayList<Dataset> dSets = new ArrayList<Dataset>();

	private DataSetsBuffer DSB;

	HttpRequestHandler(DataSetsBuffer buf) {
		logger.info("new HttpRequestHandler created...");
		DSB = buf;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		logger.info("MessageReceived.");
		// logger.info("Message: " + e.toString());

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
			
			if (pars.length > 1 && pars.length < 8) {
				logger.error("Not enough parameters.");
				return;
			}
			
			if (pars.length == 1) {
				// check if this is md5 and if it is, find the current state of
				// that datacontainer and return it.
				logger.info("got md5:"+pars[0]);
				sendBackResults(e,pars[0]);
				return;
			}

			// this is creating a new response.
			String requestMD5 = MD5(mes);
			Response resp=DSB.getResponse(requestMD5);
			if (resp==null){
				resp=DSB.createResponse(requestMD5);
				resp.parseParameters(pars);
				DataContainer DC = DSB.getContainer(resp.dss);
				resp.setDC(DC);
				resp.start();
			}else{
				logger.info("already had that md5. not parsing again. returning the result.");
			}
			
			// sending only md5 back
			sendBackMD5(e,resp); 
			// start the thread.
			
			if (true) return;
//			
//			logger.info("datasets xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//			String[] sSplit = pars[0].split("=");
//
//			if (!sSplit[0].equals("inds"))
//				return;
//
//			logger.info("inds: " + sSplit[1]);
//			String[] dss = sSplit[1].split(",");
//			
//			DataContainer DC = DSB.getContainer(dss);
//			
//			logger.info("DataContainer in place.Getting its size.");
//
//			long totsize = DC.getInputSize();
//			if (totsize < 0) {
//				resp.append("warning:at least one of the datasets does not exist, or has no root files.");
//				writeResponse(e,resp);
//				return;
//			}
//
//			logger.info("sizes of aLL input DSs have been found xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//
//			resp.append("size:" + String.valueOf(totsize) + "\n");
//			resp.append(DC.getTreeDetails());
//
//			logger.info("trees xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//
//			sSplit = pars[1].split("=");
//			if (!sSplit[0].equals("mainTree"))
//				return;
//			String mainTree = null;
//			if (sSplit.length == 2) {
//				mainTree = sSplit[1];
//				logger.info("mainTree: " + mainTree);
//				resp.append(mainTree + "\n");
//			} else {
//				resp.append("noTree\n");
//			}
//
//			sSplit = pars[2].split("=");
//			if (!sSplit[0].equals("treesToCopy"))
//				return;
//			HashSet<String> treesToCopy = new HashSet<String>();
//			if (sSplit.length == 1) {
//				logger.info("No trees to copy selected.");
//				resp.append("NoTree\n");
//			} else {
//				String[] ttC = sSplit[1].split(",");
//				for (String c : ttC)
//					treesToCopy.add(c);
//				logger.info("treesToCopy: " + sSplit[1]);
//				resp.append(sSplit[1] + "\n");
//			}
//
//			logger.info("branches xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//			sSplit = pars[3].split("=");
//			if (!sSplit[0].equals("branchesToKeep"))
//				return;
//			HashSet<String> branchesToKeep = new HashSet<String>();
//			if (sSplit.length == 1) {
//				logger.info("No branches to keep selected.");
//			} else {
//				String[] byComma = sSplit[1].split(",");
//				for (String s : byComma) {
//					String[] byNewLine = s.split("\n");
//					for (String n : byNewLine)
//						branchesToKeep.add(n);
//				}
//				logger.info("branches to keep: " + sSplit[1]);
//			}
//
//			logger.info("cut code xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//			if (!pars[4].startsWith("cutCode="))
//				return;
//			String cutCode = pars[4].substring(8);
//			logger.info("cut code: " + cutCode);
//
//			resp.append(DC.getOutputEstimate(mainTree, treesToCopy, branchesToKeep, cutCode));
//			// ==================================================
//
//			sSplit = pars[5].split("=");
//			if (!sSplit[0].equals("sReq"))
//				return;
//
//			if (sSplit[1].equals("1")) {
//				logger.info("submit request xxxxxxxxxxxxxxxxxxxxxxxxxx");
//
//				// create input files, submit script
//				// DC.createCondorInputFiles(mainTree, treesToCopy,
//				// branchesToKeep, cutCode);
//
//				// execute condor_submit
//				String outDS = null;
//				sSplit = pars[6].split("=");
//				if (!sSplit[0].equals("outDS"))
//					return;
//
//				if (sSplit.length == 1) {
//					logger.error("No outDS !");
//				} else {
//					outDS = sSplit[1];
//					logger.info("outDS: " + outDS);
//				}
//
//				String deliverTo = null;
//				sSplit = pars[7].split("=");
//				if (!sSplit[0].equals("deliverTo"))
//					return;
//
//				if (sSplit.length == 1) {
//					logger.info("No delivery");
//				} else {
//					deliverTo = sSplit[1];
//					logger.info("deliverTo: " + deliverTo);
//				}
//
//				DC.insertJob(outDS, mainTree, treesToCopy, branchesToKeep, cutCode, deliverTo);
//				logger.debug("submitted");
//
//				resp.append("\nYour job has been submitted.");
//			} else
//				resp.append("\nOK");
//			// ==================================================
//
//			if (request.isChunked()) {
//				readingChunks = true;
//			} else {
//				writeResponse(e,resp);
//
//				logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//			}
		}
	}



	private void sendBackMD5(MessageEvent e,Response r) {
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

		StringBuilder buf = new StringBuilder();
		buf.append("md5:"+r.md5);
		logger.info("returns md5: " + r.md5);

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
	
	// this one sends results back. 
	private void sendBackResults(MessageEvent e, String md5) {
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		Response result=DSB.getResponse(md5);
		if (result==null){
			return;
		}

		response.setContent(ChannelBuffers.copiedBuffer(result.getStringBuffer(), CharsetUtil.UTF_8));
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

	private String MD5(String md5) {
		try {
			java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
		}
		return null;
	}

}