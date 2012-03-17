package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;

import com.ibm.websphere.sib.Reliability;
import com.ibm.websphere.sib.SIDestinationAddress;
import com.ibm.websphere.sib.SIDestinationAddressFactory;
import com.ibm.websphere.sib.api.jms.JmsDestination;
import com.ibm.websphere.sib.exception.SIException;
import com.ibm.ws.sib.api.jms.JmsInternalsFactory;
import com.ibm.ws.sib.api.jms.JmsSharedUtils;
import com.ibm.ws.sib.security.auth.AuthUtils;
import com.ibm.ws.sib.security.auth.AuthUtilsFactory;
import com.ibm.ws.sib.security.auth.SIBSecurityException;
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
    private final JmsSharedUtils jmsSharedUtils;
    private SICoreConnection connection;
    private ConsumerSession session;
    
    public SibBatchActivation(SibBatchResourceAdapter resourceAdapter, MessageEndpointFactory messageEndpointFactory, SibBatchActivationSpec spec) throws SIException, SIBSecurityException, ResourceException {
        this.resourceAdapter = resourceAdapter;
        this.messageEndpointFactory = messageEndpointFactory;
        try {
            jmsSharedUtils = JmsInternalsFactory.getSharedUtils();
        } catch (JMSException ex) {
            throw new ResourceAdapterInternalException(ex);
        }
        AuthUtils authUtils = AuthUtilsFactory.getInstance().getAuthUtils();
        SICoreConnectionFactory factory = SICoreConnectionFactorySelector.getSICoreConnectionFactory(FactoryType.TRM_CONNECTION);
        Map properties = new HashMap();
        properties.put("busName", spec.getBusName());
        // TODO: other properties: targetType, targetGroup, targetSignificance, targetTransportChain, providerEndpoints
        
        // TODO: all this should be deferred so that we can retry if the connection attempt fails (and also reconnect)
        connection = factory.createConnection(authUtils.getServerSubject(), properties);
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

    public JmsSharedUtils getJmsSharedUtils() {
        return jmsSharedUtils;
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
}
