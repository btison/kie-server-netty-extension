package org.kie.server.ext.netty.codec;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.core.command.impl.GenericCommand;
import org.drools.core.command.runtime.BatchExecutionCommandImpl;
import org.drools.core.command.runtime.process.StartProcessCommand;
import org.drools.core.command.runtime.rule.FireAllRulesCommand;
import org.drools.core.command.runtime.rule.InsertObjectCommand;
import org.drools.core.runtime.impl.ExecutionResultImpl;
import org.kie.api.command.Command;
import org.kie.api.runtime.ExecutionResults;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.ext.netty.marshaller.ProtoStreamMarshaller;

import io.netty.buffer.ByteBuf;

public class Codec {
    
    private static final byte OPCODE_BATCH =  0x01;
    private static final byte OPCODE_INSERT =  0x02;
    private static final byte OPCODE_FIRE =  0x03;
    private static final byte OPCODE_START_PROCESS =  0x04;
    
    private Marshaller marshaller;

    public void encodeString(String string, ByteBuf byteBuf) {
        if (string == null) {
            byteBuf.writeInt(-1);
            return;
        }
        if (string.isEmpty()) {
            byteBuf.writeInt(0);
            return;
        }
        byte[] bytes = string.getBytes(Charset.forName("UTF-8"));
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
    
    public String decodeString(ByteBuf byteBuf) {
        int length = byteBuf.readInt();
        if (length < 0)  {
            return null;
        } else if (length == 0) {
            return "";
        } else {
            return new String(byteBuf.readBytes(length).array(),Charset.forName("UTF-8"));
        }
    }
    
    public void encodeByteArray(byte[] bytes, ByteBuf byteBuf) {
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
    
    public byte[] decodeByteArray(ByteBuf byteBuf) {
        int length = byteBuf.readInt();
        return byteBuf.readBytes(length).array();
    }

    public void encodeCommand(Command<?> command, ByteBuf byteBuf) {
        
        if (command instanceof BatchExecutionCommandImpl) {
            BatchExecutionCommandImpl batch = (BatchExecutionCommandImpl) command;
            byteBuf.writeByte(OPCODE_BATCH);
            encodeString(batch.getLookup(), byteBuf);
            byteBuf.writeByte(batch.getCommands().size());
            for (Command<?> c : batch.getCommands()) {
                encodeCommand(c, byteBuf);
            }
        } else if (command instanceof InsertObjectCommand) {
            InsertObjectCommand insert = (InsertObjectCommand) command;
            byteBuf.writeByte(OPCODE_INSERT);
            encodeString(insert.getOutIdentifier(), byteBuf);
            byteBuf.writeBoolean(insert.isReturnObject());
            byteBuf.writeBoolean(insert.isDisconnected());
            encodeString(insert.getEntryPoint(), byteBuf);
            encodeByteArray(marshall(insert.getObject()), byteBuf);            
        } else if (command instanceof FireAllRulesCommand) {
            //TODO support for AgendaFilter
            FireAllRulesCommand fire = (FireAllRulesCommand) command;
            byteBuf.writeByte(OPCODE_FIRE);
            byteBuf.writeInt(fire.getMax());
            encodeString(fire.getOutIdentifier(), byteBuf);
        } else if (command instanceof StartProcessCommand) {
            StartProcessCommand start = (StartProcessCommand) command;
            byteBuf.writeByte(OPCODE_START_PROCESS);
            encodeString(start.getProcessId(), byteBuf);
            encodeString(start.getOutIdentifier(), byteBuf);
            encodeList(start.getData(), byteBuf);
            encodeMap(start.getParameters(), byteBuf);
        }
        
    }
    
    public void encodeMap(Map<String, ?> map, ByteBuf byteBuf) {
        if (map == null) {
            byteBuf.writeInt(-1);
        }
        byteBuf.writeInt(map.size());
        for (String s : map.keySet()) {
            encodeString(s, byteBuf);
            encodeByteArray(marshall(map.get(s)), byteBuf);
        }
    }
    
    public void encodeList(List<?> list, ByteBuf byteBuf) {
        if (list == null) {
            byteBuf.writeInt(-1);
            return;
        }
        byteBuf.writeInt(list.size());
        for (Object o : list) {
            encodeByteArray(marshall(o), byteBuf);
        }
    }
    
    public Map<String, ?> decodeMap(ByteBuf byteBuf) {
        int size = byteBuf.readInt();
        if (size < 0) {
            return null;
        }
        Map<String, Object> map = new HashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            String key = decodeString(byteBuf);
            Object o = unmarshall(decodeByteArray(byteBuf));
            map.put(key, o);
        }
        return map;
    }
    
    public List<?> decodeList(ByteBuf byteBuf) {
        int size = byteBuf.readInt();
        if (size < 0) {
            return null;
        }
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Object o = unmarshall(decodeByteArray(byteBuf));
            list.add(o);
        }
        return list;
    }
    
    public byte[] marshall(Object o) {
        if (marshaller instanceof ProtoStreamMarshaller) {
            return ((ProtoStreamMarshaller)marshaller).marshallToBytes(o);
        } else {
            return marshaller.marshall(o).getBytes();
        }
    }
    
    public Object unmarshall(byte[] bytes) {
        if (marshaller instanceof ProtoStreamMarshaller) {
            return ((ProtoStreamMarshaller)marshaller).unmarshallFromBytes(bytes, Object.class);
        } else {
            return marshaller.unmarshall(new String(bytes, Charset.forName("UTF-8")), Object.class);
        }
    }
    
    public GenericCommand<?> decodeCommand(ByteBuf byteBuf) {
        byte b = byteBuf.readByte();
        if (b == OPCODE_BATCH) {
            return decodeExecuteBatchCommand(byteBuf);
        } else if (b == OPCODE_FIRE) {
            return decodeFireAllRulesCommand(byteBuf);
        } else if (b == OPCODE_INSERT) {
            return decodeInsertObjectCommand(byteBuf);
        } else if (b == OPCODE_START_PROCESS){
            return decodeStartProcessCommand(byteBuf);
        } else {
            return null;
        }
    }
    
    @SuppressWarnings("unchecked")
    private GenericCommand<?> decodeStartProcessCommand(ByteBuf byteBuf) {        
        String processId = decodeString(byteBuf);
        String outIdentifier = decodeString(byteBuf);
        List<Object> data = (List<Object>) decodeList(byteBuf);
        Map<String, Object> parameters = (Map<String, Object>) decodeMap(byteBuf);
        StartProcessCommand command = new StartProcessCommand();
        command.setData(data);
        command.setParameters(parameters);
        command.setOutIdentifier(outIdentifier);
        command.setProcessId(processId);
        return command;
    }

    private GenericCommand<?> decodeExecuteBatchCommand(ByteBuf byteBuf) {
        String lookup = decodeString(byteBuf);
        int numCommands = byteBuf.readByte();
        List<GenericCommand<?>> commands = new ArrayList<GenericCommand<?>>(numCommands);
        for (int i = 0; i < numCommands; i++) {
            commands.add(decodeCommand(byteBuf));
        }
        BatchExecutionCommandImpl command = new BatchExecutionCommandImpl(commands);
        command.setLookup(lookup);
        return command;
    }

    private GenericCommand<?> decodeFireAllRulesCommand(ByteBuf byteBuf) {
        FireAllRulesCommand command = new FireAllRulesCommand();
        command.setMax(byteBuf.readInt());
        command.setOutIdentifier(decodeString(byteBuf));
        return command;
    }

    private GenericCommand<?> decodeInsertObjectCommand(ByteBuf byteBuf) {
        String outIdentifier = decodeString(byteBuf);
        boolean returnObject = byteBuf.readBoolean();
        boolean disconnected = byteBuf.readBoolean();
        String entryPoint = decodeString(byteBuf);
        
        Object o = unmarshall(decodeByteArray(byteBuf));
        InsertObjectCommand command = new InsertObjectCommand(o, disconnected);
        command.setOutIdentifier(outIdentifier);
        command.setReturnObject(returnObject);
        command.setEntryPoint(entryPoint);
        return command;
    }
    
    public void encodeExecutionResults(ExecutionResults results, ByteBuf byteBuf) {
        byteBuf.writeInt(results.getIdentifiers().size());
        for (String key : results.getIdentifiers()) {
            encodeString(key, byteBuf);
            encodeByteArray(marshall(results.getValue(key)), byteBuf);
        }
    }
    
    public ExecutionResults decodeExecutionResults(ByteBuf byteBuf) {
        ExecutionResultImpl er = new ExecutionResultImpl();
        int size = byteBuf.readInt();
        HashMap<String, Object> results = new HashMap<>(size); 
        for (int i = 0; i < size; i++) {
            String key = decodeString(byteBuf);
            Object o = unmarshall(decodeByteArray(byteBuf));
            results.put(key, o);
        }
        er.setResults(results);
        return er;        
    }

    public void setMarshaller(Marshaller marshaller) {
        this.marshaller = marshaller;
    }

}
