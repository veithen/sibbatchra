package ra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkException;

import com.ibm.websphere.sib.Reliability;
import com.ibm.websphere.sib.SIDestinationAddress;
import com.ibm.websphere.sib.SIDestinationAddressFactory;
import com.ibm.websphere.sib.api.jms.JmsDestination;
import com.ibm.websphere.sib.exception.SIException;
import com.ibm.ws.sib.mfp.JsMessage;
import com.ibm.wsspi.sib.core.AsynchConsumerCallback;
import com.ibm.wsspi.sib.core.ConsumerSession;
import com.ibm.wsspi.sib.core.DestinationType;
import com.ibm.wsspi.sib.core.LockedMessageEnumeration;
import com.ibm.wsspi.sib.core.SIBusMessage;
import com.ibm.wsspi.sib.core.SICoreConnection;
import com.ibm.wsspi.sib.core.SIMessageHandle;

public class SibBatchActivation implements AsynchConsumerCallback {
    private static final Logger logger = Logger.getLogger(SibBatchActivation.class.getName());
    
    private final SibBatchResourceAdapter resourceAdapter;
    private final MessageEndpointFactory messageEndpointFactory;
    private final SibBatchActivationSpec spec;
    private final Timer timer;
    private SibBatchResourceInfo resourceInfo;
    private SICoreConnection connection;
    private ConsumerSession session;
    
    private final Object concurrencyLock = new Object();
    private int concurrency;
    
    private final Object batchLock = new Object();
    private int batchId;
    private List<SIBusMessage> batch;
    private long batchStartTime = -1;
    private TimerTask batchTimeoutTask;
    
    public SibBatchActivation(SibBatchResourceAdapter resourceAdapter, MessageEndpointFactory messageEndpointFactory, SibBatchActivationSpec spec) throws ResourceException {
        this.resourceAdapter = resourceAdapter;
        this.messageEndpointFactory = messageEndpointFactory;
        this.spec = spec;
        this.timer = resourceAdapter.getBootstrapContext().createTimer();
        try {
            // TODO: all this should be deferred so that we can retry if the connection attempt fails (and also reconnect)
            resourceInfo = spec.getResourceInfo();
            connection = resourceInfo.createConnection();
            // TODO: register a connection listener
            
            JmsDestination jmsDestination = (JmsDestination)spec.getDestination();
            SIDestinationAddress destination = SIDestinationAddressFactory.getInstance().createSIDestinationAddress(jmsDestination.getDestName(), jmsDestination.getBusName());
            session = connection.createConsumerSession(destination, DestinationType.QUEUE, null /* selectionCriteria */, null /* reliability */, false /* enableReadAhead */, false, Reliability.NONE /* unrecoverableReliability */, true, null /* alternateUser */);
            session.registerAsynchConsumerCallback(this, 0 /* maxActiveMessages */, 0L /* messageLockExpiry */, spec.getMaxBatchSize(), null);
            session.start(false);
        } catch (SIException ex) {
            throw new ResourceException(ex);
        }
    }

    public MessageEndpointFactory getEndpointFactory() {
        return messageEndpointFactory;
    }

    public SibBatchResourceInfo getResourceInfo() {
        return resourceInfo;
    }

    public SICoreConnection getConnection() {
        return connection;
    }

    public ConsumerSession getSession() {
        return session;
    }

    public void consumeMessages(LockedMessageEnumeration lockedMessages) throws Throwable {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Got " + lockedMessages.getRemainingMessageCount() + " messages from session");
        }
        boolean maxConcurrencyReached = false;
        List<SIMessageHandle> undeliveredMessages = new ArrayList<SIMessageHandle>();
        synchronized (batchLock) {
            int maxBatchSize = spec.getMaxBatchSize();
            SIBusMessage message;
            while ((message = lockedMessages.nextLocked()) != null) {
                if (maxConcurrencyReached) {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "Skipping message " + message.getSystemMessageId() + " because maximum concurrency has been reached");
                    }
                    undeliveredMessages.add(message.getMessageHandle());
                } else if (((JsMessage)message).getRedeliveredCount() > 0) {
                    if (batch != null) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Closing batch " + batchId + " because we got a redelivered message");
                        }
                        if (!scheduleBatch()) {
                            maxConcurrencyReached = true;
                        }
                    }
                    if (maxConcurrencyReached) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Skipping message " + message.getSystemMessageId() + " because maximum concurrency has been reached");
                        }
                        undeliveredMessages.add(message.getMessageHandle());
                    } else {
                        batchId++;
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Creating batch with ID " + batchId + " containing a single message " + message.getSystemMessageId() + " (redeliveredCount=" + ((JsMessage)message).getRedeliveredCount() + ")");
                        }
                        batch = Collections.singletonList(message);
                        if (!scheduleBatch()) {
                            maxConcurrencyReached = true;
                        }
                    }
                } else {
                    if (batch == null) {
                        batchId++;
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Starting new batch with ID " + batchId);
                        }
                        batch = new ArrayList<SIBusMessage>(maxBatchSize);
                        batchStartTime = System.currentTimeMillis();
                    }
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "Adding message " + message.getSystemMessageId() + " to batch " + batchId);
                    }
                    batch.add(message);
                    if (batch.size() == maxBatchSize) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Match bax size (" + maxBatchSize + ") reached for batch " + batchId);
                        }
                        if (!scheduleBatch()) {
                            maxConcurrencyReached = true;
                        }
                    }
                }
            }
            if (batch != null && batchTimeoutTask == null) {
                long time = batchStartTime + spec.getBatchTimeout();
                if (time > System.currentTimeMillis()) {
                    batchTimeoutTask = new TimerTask() {
                        @Override
                        public void run() {
                            synchronized (batchLock) {
                                if (batchTimeoutTask == this) {
                                    batchTimeoutTask = null;
                                    if (logger.isLoggable(Level.FINE)) {
                                        logger.log(Level.FINE, "Batch " + batchId + " timed out");
                                    }
                                    try {
                                        scheduleBatch();
                                    } catch (WorkException ex) {
                                        // TODO Auto-generated catch block
                                        ex.printStackTrace();
                                    }
                                }
                            }
                        }
                    };
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "Scheduling batch timeout task " + batchTimeoutTask + " for batch " + batchId + "; time=" + time);
                    }
                    timer.schedule(batchTimeoutTask, new Date(time));
                } else {
                    logger.log(Level.FINE, "Batch " + batchId + " timed out; schedule immediately");
                    if (!scheduleBatch()) {
                        maxConcurrencyReached = true;
                    }
                }
            }
        }
        if (maxConcurrencyReached) {
            logger.log(Level.FINE, "Stopping session because maximum concurrency has been reached");
            session.stop();
        }
        if (!undeliveredMessages.isEmpty()) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Unlocking undelivered messages: " + undeliveredMessages);
            }
            // Passing false as second argument prevents the messaging engine from incrementing the delivery count
            session.unlockSet(undeliveredMessages.toArray(new SIMessageHandle[undeliveredMessages.size()]), false);
        }
    }
    
    private boolean scheduleBatch() throws WorkException {
        if (batchTimeoutTask != null) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Cancelling batch timeout task " + batchTimeoutTask + " for batch " + batchId);
            }
            batchTimeoutTask.cancel();
            batchTimeoutTask = null;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Scheduling batch " + batchId + " for processing");
        }
        SibBatchWork work = new SibBatchWork(this, batch, batchId);
        resourceAdapter.getBootstrapContext().getWorkManager().scheduleWork(work, Long.MAX_VALUE, null, work);
        batch = null;
        batchStartTime = -1;
        synchronized (concurrencyLock) {
            concurrency++;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Batch " + batchId + " submitted to WorkManager; new concurrency: " + concurrency);
            }
            return concurrency < spec.getMaxConcurrency();
        }
    }

    public void batchCompleted(int batchId) {
        boolean startSession;
        synchronized (concurrencyLock) {
            startSession = concurrency == spec.getMaxConcurrency();
            concurrency--;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Received notification that batch " + batchId + " completed; new concurrency: " + concurrency);
            }
        }
        if (startSession) {
            logger.log(Level.FINE, "Restarting session");
            try {
                session.start(false);
            } catch (SIException ex) {
                // TODO: we should probably drop the connection and schedule an attempt to reconnect
                logger.log(Level.SEVERE, "Unable to restart session", ex);
            }
        }
    }
    
    public void deactivate() {
        timer.cancel();
        
        // Note: there may still be pending messages at this point, but they will be unlocked
        //       automatically when the connection is closed; no further action is required
        //       for these messages
        
        try {
            session.stop();
            connection.close();
        } catch (SIException ex) {
            // TODO
            ex.printStackTrace(System.out);
        }
    }
}
