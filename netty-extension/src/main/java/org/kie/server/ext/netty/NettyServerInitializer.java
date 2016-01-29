package org.kie.server.ext.netty;

import java.util.Map;

import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.services.api.KieServerRegistry;
import org.kie.server.services.drools.RulesExecutionService;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.EventExecutorGroup;

public class NettyServerInitializer extends ChannelInitializer<SocketChannel> {
    
    private EventExecutorGroup eventExecutorGroup;
    
    Map<String, Marshaller> marshallers;
    
    private KieServerRegistry registry;
    
    private RulesExecutionService rulesExecutionService;
    
    public NettyServerInitializer(KieServerRegistry registry, Map<String, Marshaller> marshallers, 
                                  RulesExecutionService rulesExecutionService, EventExecutorGroup eventExecutorGroup) {
        super();
        this.eventExecutorGroup = eventExecutorGroup;
        this.marshallers = marshallers;
        this.registry = registry;
        this.rulesExecutionService = rulesExecutionService;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new ExecutionResultsEncoder(marshallers));
        pipeline.addLast(new CommandDecoder(marshallers));
        pipeline.addLast(eventExecutorGroup, new ContainerCommandHandler(registry, rulesExecutionService));
    }
    
    

}
