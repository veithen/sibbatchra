package ra;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;

import com.ibm.wsspi.sib.core.SIBusMessage;
import com.ibm.wsspi.sib.core.SIMessageHandle;
import com.ibm.wsspi.sib.core.SIXAResource;

public class SibBatchWork implements Work, WorkListener {
    private static final Method ON_MESSAGE_METHOD;
    
    static {
        try {
            ON_MESSAGE_METHOD = MessageListener.class.getMethod("onMessage", new Class[] { Message.class });
        } catch (NoSuchMethodException ex) {
            throw new NoSuchMethodError(ex.getMessage());
        }
    }
    
    private final SibBatchActivation activation;
    private final List<SIBusMessage> messages;

    public SibBatchWork(SibBatchActivation activation, List<SIBusMessage> messages) {
        this.activation = activation;
        this.messages = messages;
    }

    public void run() {
        // see SibRaDispatcher#dispatch(List, AsynchDispatchScheduler, SibRaListener)

        try {
            SIXAResource xaResource = activation.getConnection().getSIXAResource();
            
            MessageEndpoint endpoint = activation.getEndpointFactory().createEndpoint(xaResource);
            try {
                MessageListener listener = (MessageListener)endpoint;
                
                endpoint.beforeDelivery(ON_MESSAGE_METHOD);
                try {
                    List<SIMessageHandle> handles = new ArrayList<SIMessageHandle>(messages.size());
                    for (SIBusMessage message : messages) {
                        handles.add(message.getMessageHandle());
                    }
                    activation.getSession().deleteSet(handles.toArray(new SIMessageHandle[handles.size()]), xaResource);
                    
                    for (SIBusMessage message : messages) {
                        Message jmsMessage = activation.getJmsSharedUtils().inboundMessagePath(message, null, null /* passThruProps */);
                        listener.onMessage(jmsMessage);
                    }
                } finally {
                    endpoint.afterDelivery();
                }
            } finally {
                endpoint.release();
            }
        } catch (Throwable ex) {
            ex.printStackTrace(System.out);
        }
    }

    public void release() {
        // TODO Auto-generated method stub
        
    }

    public void workAccepted(WorkEvent arg0) {
        // TODO Auto-generated method stub
        
    }

    public void workCompleted(WorkEvent arg0) {
        // TODO Auto-generated method stub
        
    }

    public void workRejected(WorkEvent arg0) {
        // TODO Auto-generated method stub
        
    }

    public void workStarted(WorkEvent arg0) {
        // TODO Auto-generated method stub
        
    }
}
