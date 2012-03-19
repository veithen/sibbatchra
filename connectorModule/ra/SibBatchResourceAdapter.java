package ra;

import java.util.HashMap;
import java.util.Map;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

// Restrictions:
//  * SIBus mediations are not executed
//  * Security is not supported (connection to messaging engine uses the server subject)
public class SibBatchResourceAdapter implements ResourceAdapter {
    private final Map<ActivationSpec,SibBatchActivation> activations = new HashMap<ActivationSpec,SibBatchActivation>();
    private BootstrapContext bootstrapContext;
    
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        this.bootstrapContext = bootstrapContext;
    }

    public BootstrapContext getBootstrapContext() {
        return bootstrapContext;
    }

    public void endpointActivation(MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) throws ResourceException {
        if (activationSpec instanceof SibBatchActivationSpec) {
            activations.put(activationSpec, new SibBatchActivation(this, messageEndpointFactory, (SibBatchActivationSpec)activationSpec));
        } else {
            throw new NotSupportedException("This resource adapter only supports activation specs of type " + SibBatchActivationSpec.class.getClass());
        }
    }

    public void endpointDeactivation(MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
        activations.get(activationSpec).deactivate();
    }

    public XAResource[] getXAResources(ActivationSpec[] activationSpec) throws ResourceException {
        // TODO Auto-generated method stub
        return null;
    }

    public void stop() {
        bootstrapContext = null;
    }
}
