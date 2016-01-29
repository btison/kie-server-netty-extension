package org.kie.server.ext.netty.codec;

import org.kie.api.command.Command;

public class ContainerCommand {
    
    private String containerId;
    
    private Command<?> command;

    public ContainerCommand(String containerId, Command<?> command) {
        this.containerId = containerId;
        this.command = command;
    }

    public String getContainerId() {
        return containerId;
    }

    public Command<?> getCommand() {
        return command;
    }

}
