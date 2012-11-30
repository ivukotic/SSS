package edu.uchicago.SSSserver;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
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
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpRequestHandler extends SimpleChannelUpstreamHandler {
	
	private final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	final Logger logger = LoggerFactory.getLogger(HttpRequestHandler.class);
    private HttpRequest request;
    private boolean readingChunks;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();
    private ArrayList<Dataset> dSets=new ArrayList<Dataset>();
    
    public void queryDQ2(String ds){
    	try {

            Dataset dSet=new Dataset(ds);
            
            Runtime rt = Runtime.getRuntime();
            String comm="dq2-ls -f "+ds;
            logger.info("executing >"+comm+"<");
            Process pr = rt.exec(comm);

            BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

            String line=null;
            while((line=input.readLine()) != null) {
            	if (line.indexOf(".root")>0){
            		logger.debug(line);
            		String [] r=line.split("\t");
            		if (r.length==5)
            			dSet.addFile(r[1],r[2],Long.parseLong(r[4]));
                }
            }

            int exitVal = pr.waitFor();
            logger.info("Exited with code "+exitVal);
            if (exitVal==0) dSets.add(dSet);
            
        } catch(Exception e) {
            logger.error(e.toString());
            e.printStackTrace();
        }
    	
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    	logger.info("MessageReceived.");
        if (!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            SlicedChannelBuffer scb = (SlicedChannelBuffer) request.getContent();
            logger.info("content: "+ scb.toString(UTF8_CHARSET));
            
            String ds=scb.toString(UTF8_CHARSET);
            String [] r=ds.split("=");
            if (r[0].equals("inds")){
            	String [] dss=r[1].split(",");
            	for (String d : dss){
            		queryDQ2(d);
            	}
            }
            
            if (is100ContinueExpected(request)) {
                send100Continue(e);
            }

            
            buf.setLength(0);
            for (Dataset das:dSets){
            	buf.append("size:"+String.valueOf(das.size)+"\r\n");
            }

//            buf.append("VERSION: " + request.getProtocolVersion() + "\r\n");
//            buf.append("HOSTNAME: " + getHost(request, "unknown") + "\r\n");
//            buf.append("REQUEST_URI: " + request.getUri() + "\r\n\r\n");
//            for (Map.Entry<String, String> h: request.getHeaders()) {
//                buf.append("HEADER: " + h.getKey() + " = " + h.getValue() + "\r\n");
//            }
//            buf.append("\r\n");
//
//            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
//            Map<String, List<String>> params = queryStringDecoder.getParameters();
//            if (!params.isEmpty()) {
//                for (Entry<String, List<String>> p: params.entrySet()) {
//                    String key = p.getKey();
//                    List<String> vals = p.getValue();
//                    for (String val : vals) {
//                        buf.append("PARAM: " + key + " = " + val + "\r\n");
//                    }
//                }
//                buf.append("\r\n");
//            }

            if (request.isChunked()) {
                readingChunks = true;
            } else {
                ChannelBuffer content = request.getContent();
                if (content.readable()) {
//                    buf.append("CONTENT: " + content.toString(CharsetUtil.UTF_8) + "\r\n");
                }
                writeResponse(e);
            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                buf.append("END OF CONTENT\r\n");

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                if (!trailer.getHeaderNames().isEmpty()) {
                    buf.append("\r\n");
                    for (String name: trailer.getHeaderNames()) {
                        for (String value: trailer.getHeaders(name)) {
                            buf.append("TRAILING HEADER: " + name + " = " + value + "\r\n");
                        }
                    }
                    buf.append("\r\n");
                }

                writeResponse(e);
            } else {
                buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
            }
        }
    }

    private void writeResponse(MessageEvent e) {
        // Decide whether to close the connection or not.
        boolean keepAlive = isKeepAlive(request);

        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
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
            if(!cookies.isEmpty()) {
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

        // Close the non-keep-alive connection after the write operation is done.
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void send100Continue(MessageEvent e) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
        e.getChannel().write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}