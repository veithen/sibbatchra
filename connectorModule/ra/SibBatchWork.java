package ra;

import java.util.ArrayList;
import java.util.List;

import javax.jms.MessageListener;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;

import com.ibm.ws.Transaction.TransactionManagerFactory;
import com.ibm.ws.Transaction.WebSphereTransactionManager;
import com.ibm.wsspi.sib.core.SIBusMessage;
import com.ibm.wsspi.sib.core.SIJMSMessageFactory;
import com.ibm.wsspi.sib.core.SIMessageHandle;
import com.ibm.wsspi.sib.core.SIXAResource;

public class SibBatchWork implements Work, WorkListener {
    private final SibBatchActivation activation;
    private final List<SIBusMessage> messages;

    public SibBatchWork(SibBatchActivation activation, List<SIBusMessage> messages) {
        this.activation = activation;
        this.messages = messages;
    }

    public void run() {
        // see SibRaDispatcher#dispatch(List, AsynchDispatchScheduler, SibRaListener)

        try {
            WebSphereTransactionManager transactionManager = TransactionManagerFactory.getTransactionManager();
            SIXAResource xaResource = activation.getConnection().getSIXAResource();
            
            // By default, transaction demarcation is handled by the beforeDelivery and afterDelivery methods
            // of the MessageEndpoint object. However, the JCA spec forbids to deliver multiple messages between
            // a call to beforeDelivery and a call to afterDelivery. Therefore we manage the transaction
            // explicitly. In this case, the transaction will appear as an imported transaction (see the JCA
            // spec) to the MessageEndpoint.
            transactionManager.begin();
            transactionManager.enlist(xaResource, SibBatchResourceFactory.class.getName(), activation.getResourceInfo());
            
            // For an imported transaction, the XAResource provided to createEndpoint is ignored.
            MessageEndpoint endpoint = activation.getEndpointFactory().createEndpoint(null);
            try {
                MessageListener listener = (MessageListener)endpoint;
                
                List<SIMessageHandle> handles = new ArrayList<SIMessageHandle>(messages.size());
                for (SIBusMessage message : messages) {
                    handles.add(message.getMessageHandle());
                }
                activation.getSession().deleteSet(handles.toArray(new SIMessageHandle[handles.size()]), xaResource);
                
                for (SIBusMessage message : messages) {
                    listener.onMessage(SIJMSMessageFactory.getInstance().createJMSMessage(message));
                }
            } finally {
                endpoint.release();
            }
            
            transactionManager.commit();
            
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
