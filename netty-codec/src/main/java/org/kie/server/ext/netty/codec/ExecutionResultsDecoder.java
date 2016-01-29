package org.kie.server.ext.netty.codec;

import java.util.List;

import org.kie.api.runtime.ExecutionResults;
import org.kie.server.api.marshalling.Marshaller;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class ExecutionResultsDecoder extends MessageToMessageDecoder<ByteBuf> {
    
    private Codec codec = new Codec();
    
    public ExecutionResultsDecoder(Marshaller marshaller) {
        codec.setMarshaller(marshaller);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        
        ExecutionResults results = codec.decodeExecutionResults(msg);
        out.add(results);        
    }

}
