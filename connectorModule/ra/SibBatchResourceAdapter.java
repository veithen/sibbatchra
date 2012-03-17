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

import com.ibm.websphere.sib.Reliability;
import com.ibm.websphere.sib.SIDestinationAddress;
import com.ibm.websphere.sib.SIDestinationAddressFactory;
import com.ibm.websphere.sib.api.jms.JmsDestination;
import com.ibm.ws.sib.api.jmsra.impl.JmsJcaActivationSpecImpl;
import com.ibm.ws.sib.security.auth.AuthUtils;
import com.ibm.ws.sib.security.auth.AuthUtilsFactory;
import com.ibm.wsspi.sib.core.ConsumerSession;
import com.ibm.wsspi.sib.core.DestinationType;
import com.ibm.wsspi.sib.core.SICoreConnection;
import com.ibm.wsspi.sib.core.SICoreConnectionFactory;
import com.ibm.wsspi.sib.core.selector.FactoryType;
import com.ibm.wsspi.sib.core.selector.SICoreConnectionFactorySelector;

// Restrictions:
//  * SIBus mediations are not executed
//  * Security is not supported (connection to messaging engine uses the server subject)
public class SibBatchResourceAdapter implements ResourceAdapter {
    private BootstrapContext bootstrapContext;
    
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        this.bootstrapContext = bootstrapContext;
    }

    public BootstrapContext getBootstrapContext() {
        return bootstrapContext;
    }

    public void endpointActivation(MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) throws ResourceException {
        if (activationSpec instanceof SibBatchActivationSpec) {
            
            new SibBatchActivation(this, (SibBatchActivationSpec)activationSpec);
            
            
        } else {
            throw new NotSupportedException("This resource adapter only supports activation specs of type " + SibBatchActivationSpec.class.getClass());
        }
    }

    public void endpointDeactivation(MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
        // TODO Auto-generated method stub
        
    }

    public XAResource[] getXAResources(ActivationSpec[] arg0) throws ResourceException {
        // TODO Auto-generated method stub
        return null;
    }

    public void stop() {
        bootstrapContext = null;
    }
}
