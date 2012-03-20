package ra;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.MessageListener;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;
import javax.transaction.Status;
import javax.transaction.SystemException;

import com.ibm.ws.Transaction.TransactionManagerFactory;
import com.ibm.ws.Transaction.WebSphereTransactionManager;
import com.ibm.wsspi.sib.core.SIBusMessage;
import com.ibm.wsspi.sib.core.SIJMSMessageFactory;
import com.ibm.wsspi.sib.core.SIMessageHandle;
import com.ibm.wsspi.sib.core.SIXAResource;

public class SibBatchWork implements Work, WorkListener {
    private static final Logger logger = Logger.getLogger(SibBatchWork.class.getName());
    
    private final SibBatchActivation activation;
    private final List<SIBusMessage> messages;
    private final int batchId;

    public SibBatchWork(SibBatchActivation activation, List<SIBusMessage> messages, int batchId) {
        this.activation = activation;
        this.messages = messages;
        this.batchId = batchId;
    }

    public void run() {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Start processing batch " + batchId + " (size=" + messages.size() + ")");
        }

        try {
            WebSphereTransactionManager transactionManager = TransactionManagerFactory.getTransactionManager();
            SIXAResource xaResource = activation.getConnection().getSIXAResource();
            
            // By default, transaction demarcation is handled by the beforeDelivery and afterDelivery methods
            // of the MessageEndpoint object. However, the JCA spec forbids to deliver multiple messages between
            // a call to beforeDelivery and a call to afterDelivery. Therefore we manage the transaction
            // explicitly. In this case, the transaction will appear as an imported transaction (see the JCA
            // spec) to the MessageEndpoint.
            transactionManager.begin();
            boolean rollback = true;
            try {
                logger.log(Level.FINE, "Enlisting SIXAResource");
                transactionManager.enlist(xaResource, SibBatchResourceFactory.class.getName(), activation.getResourceInfo());
                
                // For an imported transaction, the XAResource provided to createEndpoint is ignored.
                MessageEndpoint endpoint = activation.getEndpointFactory().createEndpoint(null);
                boolean forceRollback = false;
                try {
                    List<SIMessageHandle> handles = new ArrayList<SIMessageHandle>(messages.size());
                    for (SIBusMessage message : messages) {
                        handles.add(message.getMessageHandle());
                    }
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "Deleting messages that will be delivered to the endpoint: " + handles);
                    }
                    activation.getSession().deleteSet(handles.toArray(new SIMessageHandle[handles.size()]), xaResource);
                    
                    MessageListener listener = (MessageListener)endpoint;
                    for (SIBusMessage message : messages) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Delivering message with ID " + message.getSystemMessageId() + " to endpoint");
                        }
                        listener.onMessage(SIJMSMessageFactory.getInstance().createJMSMessage(message));
                        
                        if (transactionManager.getTransaction().getStatus() == Status.STATUS_MARKED_ROLLBACK) {
                            logger.log(Level.FINE, "Transaction has been marked for rollback; stop delivering messages to the endpoint");
                            forceRollback = true;
                            break;
                        }
                    }
                } finally {
                    endpoint.release();
                }
                
                if (!forceRollback) {
                    rollback = false;
                }
            } finally {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Processing of batch " + batchId + " completed; rollback=" + rollback);
                }
                if (rollback) {
                    try {
                        transactionManager.rollback();
                    } catch (SystemException ex) {
                        logger.log(Level.SEVERE, "Failed to rollback transaction", ex);
                    }
                } else {
                    try {
                        transactionManager.commit();
                    } catch (SystemException ex) {
                        logger.log(Level.SEVERE, "Failed to commit transaction", ex);
                    }
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace(System.out);
        }
    }

    public void release() {
        // TODO Auto-generated method stub
        
    }

    public void workAccepted(WorkEvent event) {
        // TODO Auto-generated method stub
        
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Batch " + batchId + ": work accepted");
        }
    }

    public void workRejected(WorkEvent event) {
        // TODO Auto-generated method stub
        
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Batch " + batchId + ": work rejected");
        }
    }

    public void workStarted(WorkEvent event) {
        // TODO Auto-generated method stub
        
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Batch " + batchId + ": work started");
        }
    }
    
    public void workCompleted(WorkEvent event) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Batch " + batchId + ": work completed");
        }
        activation.batchCompleted(batchId);
    }
}
