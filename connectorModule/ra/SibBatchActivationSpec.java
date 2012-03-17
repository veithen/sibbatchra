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

    public void validate() throws InvalidPropertyException {
        // TODO Auto-generated method stub
        
    }
}
