package org.kie.server.ext.netty;

import java.util.List;
import java.util.Map;

import org.kie.api.command.Command;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.ext.netty.codec.Codec;
import org.kie.server.ext.netty.codec.ContainerCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class CommandDecoder extends MessageToMessageDecoder<ByteBuf> {
    
    private Map<String, Marshaller> marshallers;
    
    private Codec codec = new Codec();
    
    public CommandDecoder(Map<String, Marshaller> marshallers) {
        this.marshallers = marshallers;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        
        String containerId = codec.decodeString(msg);
        codec.setMarshaller(marshallers.get(containerId));
        Command<?> command = codec.decodeCommand(msg);
        out.add(new ContainerCommand(containerId, command));        
    }

}
