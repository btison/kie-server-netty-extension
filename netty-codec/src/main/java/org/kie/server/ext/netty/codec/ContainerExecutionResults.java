package org.kie.server.ext.netty.codec;

import org.kie.api.runtime.ExecutionResults;

public class ContainerExecutionResults {
    
    private String containerId;
    
    private ExecutionResults executionResults;

    public ContainerExecutionResults(String containerId, ExecutionResults executionResults) {
        this.containerId = containerId;
        this.executionResults = executionResults;
    }

    public String getContainerId() {
        return containerId;
    }

    public ExecutionResults getExecutionResults() {
        return executionResults;
    }

}
