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
			logger.info("result not ready/found in DSB.");
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
//		e.getCause().printStackTrace();
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