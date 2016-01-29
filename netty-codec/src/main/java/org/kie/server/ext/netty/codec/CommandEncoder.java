package org.kie.server.ext.netty.codec;

import java.util.List;

import org.kie.server.api.marshalling.Marshaller;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class CommandEncoder extends MessageToMessageEncoder<ContainerCommand> {
    
    private Codec codec = new Codec();
    
    public CommandEncoder(Marshaller marshaller) {
        codec.setMarshaller(marshaller);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ContainerCommand msg, List<Object> out) throws Exception {
        
        ByteBuf byteBuf = Unpooled.buffer();
        codec.encodeString(msg.getContainerId(), byteBuf);
        codec.encodeCommand(msg.getCommand(), byteBuf);
        out.add(byteBuf);
    }

}
