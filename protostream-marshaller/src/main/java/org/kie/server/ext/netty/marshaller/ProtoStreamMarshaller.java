package org.kie.server.ext.netty.marshaller;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.api.marshalling.MarshallingException;
import org.kie.server.api.marshalling.MarshallingFormat;

import java.io.IOException;

public class ProtoStreamMarshaller implements Marshaller {
    
    private SerializationContext ctx =  ProtobufUtil.newSerializationContext(new Configuration.Builder().build());
    
    public SerializationContext getSerializationContext() {
        return ctx;
    }

    @Override
    public String marshall(Object input) {
        return new String(marshallToBytes(input));
    }

    @Override
    public <T> T unmarshall(String input, Class<T> type) {
        return unmarshallFromBytes(input.getBytes(), type);
    }
    
    
    public byte[] marshallToBytes(Object input) {
        try {
            return ProtobufUtil.toWrappedByteArray(ctx, input);
        } catch (IOException e) {
            throw new MarshallingException("Error marshalling input", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T unmarshallFromBytes(byte[] input, Class<T> type) {
        try {
            Object o = ProtobufUtil.fromWrappedByteArray(ctx, input);
            return (T) o;
        } catch (IOException e) {
            throw new MarshallingException("Error marshalling input", e);
        }
    }

    @Override
    public void dispose() {

    }

    @Override
    public MarshallingFormat getFormat() {
        return null;
    }

    @Override
    public void setClassLoader(ClassLoader classloader) {
        //no-op
    }

    @Override
    public ClassLoader getClassLoader() {
        return null;
    }

}
