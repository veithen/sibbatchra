package ra;

import java.io.Serializable;

import javax.jms.Destination;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;

public class SibBatchActivationSpec implements ActivationSpec, Serializable {
    private static final long serialVersionUID = 1L;
    
    private transient ResourceAdapter resourceAdapter;
    private String busName;
    private Destination destination;
    private int maxBatchSize;
    private int batchTimeout;
    
    public ResourceAdapter getResourceAdapter() {
        return resourceAdapter;
    }

    public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
        this.resourceAdapter = resourceAdapter;
    }

    public String getBusName() {
        return busName;
    }

    public void setBusName(String busName) {
        this.busName = busName;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(int batchTimeout) {
        this.batchTimeout = batchTimeout;
    }

    public void validate() throws InvalidPropertyException {
        // TODO Auto-generated method stub
        
    }
    
    public SibBatchResourceInfo getResourceInfo() {
        // TODO: there is something wrong here: recovery can only be successful if the transaction
        //       manager connects to the right messaging engine; if there are multiple messaging
        //       engines in the bus, then it's not enough to provide the bus name. Probably we need
        //       to complete the resource info with the messaging engine UUID after we have
        //       established a connection.
        return new SibBatchResourceInfo(busName);
    }
}
