package org.kie.server.ext.netty;

import java.util.List;
import java.util.Map;

import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.ext.netty.codec.Codec;
import org.kie.server.ext.netty.codec.ContainerExecutionResults;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class ExecutionResultsEncoder extends MessageToMessageEncoder<ContainerExecutionResults> {
    
    private Map<String, Marshaller> marshallers;
    
    private Codec codec = new Codec();
    
    public ExecutionResultsEncoder(Map<String, Marshaller> marshallers) {
        this.marshallers = marshallers;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ContainerExecutionResults msg, List<Object> out) throws Exception {
        
        String containerId = msg.getContainerId();
        codec.setMarshaller(marshallers.get(containerId));
        ByteBuf byteBuf = Unpooled.buffer();
        codec.encodeExecutionResults(msg.getExecutionResults(), byteBuf);
        out.add(byteBuf);
    }

}
