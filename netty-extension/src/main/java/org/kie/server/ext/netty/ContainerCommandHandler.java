package org.kie.server.ext.netty;

import java.util.Arrays;

import org.drools.core.command.impl.GenericCommand;
import org.drools.core.command.runtime.BatchExecutionCommandImpl;
import org.kie.api.command.Command;
import org.kie.api.runtime.ExecutionResults;
import org.kie.server.ext.netty.codec.ContainerCommand;
import org.kie.server.ext.netty.codec.ContainerExecutionResults;
import org.kie.server.services.api.KieServerRegistry;
import org.kie.server.services.drools.RulesExecutionService;
import org.kie.server.services.impl.KieContainerInstanceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ContainerCommandHandler extends ChannelInboundHandlerAdapter {
    
    private static Logger LOG = LoggerFactory.getLogger(ContainerCommandHandler.class);
    
    private KieServerRegistry registry;
    
    private RulesExecutionService rulesExecutionService; 
    
    public ContainerCommandHandler(KieServerRegistry registry, RulesExecutionService rulesExecutionService) {
        super();
        this.registry = registry;
        this.rulesExecutionService = rulesExecutionService;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Channel inactive");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ContainerCommand cc = (ContainerCommand) msg;
        KieContainerInstanceImpl kci = registry.getContainer(cc.getContainerId());
        Command<?> command = cc.getCommand();
        if (!(command instanceof BatchExecutionCommandImpl)) {
            command = new BatchExecutionCommandImpl(Arrays.asList(new GenericCommand<?>[]{(GenericCommand<?>) command}));
        }
        ExecutionResults results = rulesExecutionService.call(kci, (BatchExecutionCommandImpl) command);
        ContainerExecutionResults cer = new ContainerExecutionResults(cc.getContainerId(), results);
        ctx.write(cer);        
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
    
    

}
