package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;

import com.ibm.websphere.sib.Reliability;
import com.ibm.websphere.sib.SIDestinationAddress;
import com.ibm.websphere.sib.SIDestinationAddressFactory;
import com.ibm.websphere.sib.api.jms.JmsDestination;
import com.ibm.websphere.sib.exception.SIException;
import com.ibm.wsspi.sib.core.AsynchConsumerCallback;
import com.ibm.wsspi.sib.core.ConsumerSession;
import com.ibm.wsspi.sib.core.DestinationType;
import com.ibm.wsspi.sib.core.LockedMessageEnumeration;
import com.ibm.wsspi.sib.core.SIBusMessage;
import com.ibm.wsspi.sib.core.SICoreConnection;
import com.ibm.wsspi.sib.core.SICoreConnectionFactory;
import com.ibm.wsspi.sib.core.selector.FactoryType;
import com.ibm.wsspi.sib.core.selector.SICoreConnectionFactorySelector;

public class SibBatchActivation implements AsynchConsumerCallback {
    private final SibBatchResourceAdapter resourceAdapter;
    private final MessageEndpointFactory messageEndpointFactory;
    private SICoreConnection connection;
    private ConsumerSession session;
    
    public SibBatchActivation(SibBatchResourceAdapter resourceAdapter, MessageEndpointFactory messageEndpointFactory, SibBatchActivationSpec spec) throws SIException, ResourceException {
        this.resourceAdapter = resourceAdapter;
        this.messageEndpointFactory = messageEndpointFactory;
        SICoreConnectionFactory factory = SICoreConnectionFactorySelector.getSICoreConnectionFactory(FactoryType.TRM_CONNECTION);
        Map properties = new HashMap();
        properties.put("busName", spec.getBusName());
        // TODO: other properties: targetType, targetGroup, targetSignificance, targetTransportChain, providerEndpoints
        
        // TODO: all this should be deferred so that we can retry if the connection attempt fails (and also reconnect)
        // TODO: security not supported
        connection = factory.createConnection(null, null, properties);
        // TODO: register a connection listener
        
        JmsDestination jmsDestination = (JmsDestination)spec.getDestination();
        SIDestinationAddress destination = SIDestinationAddressFactory.getInstance().createSIDestinationAddress(jmsDestination.getDestName(), jmsDestination.getBusName());
        session = connection.createConsumerSession(destination, DestinationType.QUEUE, null /* selectionCriteria */, null /* reliability */, false /* enableReadAhead */, false, Reliability.NONE /* unrecoverableReliability */, true, null /* alternateUser */);
        session.registerAsynchConsumerCallback(this, 0 /* maxActiveMessages */, 0L /* messageLockExpiry */, spec.getMaxBatchSize(), null);
        session.start(false);
    }

    public MessageEndpointFactory getEndpointFactory() {
        return messageEndpointFactory;
    }

    public SICoreConnection getConnection() {
        return connection;
    }

    public ConsumerSession getSession() {
        return session;
    }

    public void consumeMessages(LockedMessageEnumeration lockedMessages) throws Throwable {
        List<SIBusMessage> messages = new ArrayList<SIBusMessage>();
        SIBusMessage message;
        while ((message = lockedMessages.nextLocked()) != null) {
            messages.add(message);
        }
        SibBatchWork work = new SibBatchWork(this, messages);
        resourceAdapter.getBootstrapContext().getWorkManager().scheduleWork(work, Long.MAX_VALUE, null, work);
    }

    public void deactivate() {
        try {
            session.stop();
            connection.close();
        } catch (SIException ex) {
            // TODO
            ex.printStackTrace(System.out);
        }
    }
}
