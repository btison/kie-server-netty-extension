package org.kie.server.ext.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.drools.compiler.kproject.xml.DependencyFilter;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.ext.netty.marshaller.ProtoStreamMarshaller;
import org.kie.server.services.api.KieContainerInstance;
import org.kie.server.services.api.KieServerExtension;
import org.kie.server.services.api.KieServerRegistry;
import org.kie.server.services.api.SupportedTransports;
import org.kie.server.services.drools.RulesExecutionService;
import org.kie.server.services.impl.KieServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class NettyKieServerExtension implements KieServerExtension {
    
    public static final String EXTENSION_NAME = "Drools-Netty";
    
    private static final Logger logger = LoggerFactory.getLogger(NettyKieServerExtension.class);
    private static final Boolean disabled = Boolean.parseBoolean(System.getProperty("org.kie.server.drools-netty.ext.disabled", "false"));
    private static final String NETTY_HOST = System.getProperty("org.kie.server.drools-netty.ext.host", "localhost");
    private static final int NETTY_PORT = Integer.parseInt(System.getProperty("org.kie.server.drools-netty.ext.port", "8888"));
    
    private RulesExecutionService rulesExecutionService;
    
    private EventLoopGroup group;
    
    private Map<String, Marshaller> marshallers = new HashMap<>();
    
    @Override
    public boolean isActive() {
        return disabled == false;
    }

    @Override
    public void init(KieServerImpl kieServer, KieServerRegistry registry) {
        //dependency on Drools extension
        KieServerExtension droolsExtension = registry.getServerExtension("Drools");
        if (droolsExtension == null) {
            logger.warn("No Drools extension available, quitting...");
            return;
        }
        List<Object> droolsServices = droolsExtension.getServices();
        for( Object object : droolsServices ) {
            // in case given service is null (meaning was not configured) continue with next one
            if (object == null) {
                continue;
            }
            if( RulesExecutionService.class.isAssignableFrom(object.getClass()) ) {
                rulesExecutionService = (RulesExecutionService) object;
                continue;
            } 
        }
        if (rulesExecutionService != null) {
            try {
                group = new NioEventLoopGroup();
                ServerBootstrap b = new ServerBootstrap();  
                b.group(group)               
                .channel(NioServerSocketChannel.class)     
                .localAddress(new InetSocketAddress(NETTY_HOST, NETTY_PORT))
                .childHandler(new NettyServerInitializer(registry, marshallers, rulesExecutionService, setupEventExecutorGroup()));
                ChannelFuture f = b.bind().sync();
                if (f.isSuccess()) {
                    logger.info("Netty Server initialized and listening on {}:{}", NETTY_HOST, NETTY_PORT);
                }
            } catch (Exception e) {
                logger.error("Error starting up Netty Server", e);
            }
        }
        
    }

    @Override
    public void destroy(KieServerImpl kieServer, KieServerRegistry registry) {
        group.shutdownGracefully().syncUninterruptibly();
    }

    @Override
    public void createContainer(String id, KieContainerInstance kieContainerInstance, Map<String, Object> parameters) {
        KieModuleMetaDataImpl metaData = new KieModuleMetaDataImpl(kieContainerInstance, DependencyFilter.COMPILE_FILTER);
        try {
            marshallers.put(id, initializeMarshaller(metaData.getProtoFiles(), metaData.getMarshallers()));
        } catch (Throwable e) {
            logger.error("Unexpected error when initializing marshaller for container {}", id, e);
        }
    }

    @Override
    public void disposeContainer(String id, KieContainerInstance kieContainerInstance, Map<String, Object> parameters) {
        
    }

    @Override
    public List<Object> getAppComponents(SupportedTransports type) {
        return Collections.emptyList();
    }

    @Override
    public <T> T getAppComponents(Class<T> serviceType) {
        return null;
    }

    @Override
    public String getImplementedCapability() {
        return "BRM-Netty";
    }

    @Override
    public List<Object> getServices() {
        return Collections.emptyList();
    }

    @Override
    public String getExtensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public Integer getStartOrder() {
        return 20;
    }
    
    public Marshaller getMarshaller(String containerId) {
        return marshallers.get(containerId);
    }
    
    private EventExecutorGroup setupEventExecutorGroup() {
        String useEventExecutorGroupStr = System.getProperty("org.kie.server.drools-netty.ext.executorgroup", "false");
        String eventExecutorgroupThreadsStr = System.getProperty("org.kie.server.drools-netty.ext.executorgroup.threads", "1");
        boolean useEventExecutorGroup = Boolean.valueOf(useEventExecutorGroupStr);
        if (useEventExecutorGroup) {
            int threads = Integer.valueOf(eventExecutorgroupThreadsStr);
            return new DefaultEventExecutorGroup(threads);
        } else {
            return null;
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Marshaller initializeMarshaller(Map<String, String> protoFiles, Set<Class<?>> protomarshallers) throws IOException, InstantiationException, IllegalAccessException {
        ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
        FileDescriptorSource source = new FileDescriptorSource();
        for (String name : protoFiles.keySet()) {
            source.addProtoFile(name, protoFiles.get(name));
        }
        SerializationContext ctx = marshaller.getSerializationContext();
        ctx.registerProtoFiles(source);
        for (Class<?> clazz : protomarshallers) {
            ctx.registerMarshaller((BaseMarshaller) clazz.newInstance());
        }
        return marshaller;
    }
}
