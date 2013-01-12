package edu.uchicago.SSSserver;

import static org.jboss.netty.channel.Channels.*;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerPipelineFactory implements ChannelPipelineFactory {

	final static Logger logger = LoggerFactory.getLogger(HttpServerPipelineFactory.class);

    DataSetsBuffer DSB=new DataSetsBuffer();
    
	HttpServerPipelineFactory(){
    	logger.info("new HttpServerPipelineFactory created!!!!");
    }
    
	public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

    	logger.info("new ChannelPipeline created!!!!");
    	
        pipeline.addLast("decoder", new HttpRequestDecoder());
        // Uncomment the following line if you don't want to handle HttpChunks.
        //pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        // Remove the following line if you don't want automatic content compression.
//        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new HttpRequestHandler(DSB));
        return pipeline;
    }
}