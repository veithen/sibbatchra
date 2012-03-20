package ra;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.ibm.websphere.sib.exception.SIException;
import com.ibm.wsspi.sib.core.SICoreConnection;

/**
 * {@link XAResource} wrapper that keeps a reference to the corresponding {@link SICoreConnection}.
 * It is only used during recovery.
 */
public class SibBatchRecoveryXAResource implements XAResource {
    private static final Logger logger = Logger.getLogger(SibBatchRecoveryXAResource.class.getName());
    
    private XAResource parent;
    private SICoreConnection connection;
    
    public SibBatchRecoveryXAResource(SICoreConnection connection) throws SIException {
        parent = connection.getSIXAResource();
        this.connection = connection;
    }

    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "commit " + xid);
        }
        parent.commit(xid, onePhase);
    }

    public void end(Xid xid, int flags) throws XAException {
        parent.end(xid, flags);
    }

    public void forget(Xid xid) throws XAException {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "forget " + xid);
        }
        parent.forget(xid);
    }

    public int getTransactionTimeout() throws XAException {
        return parent.getTransactionTimeout();
    }

    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (xaResource instanceof SibBatchRecoveryXAResource) {
            return parent.isSameRM(((SibBatchRecoveryXAResource)xaResource).parent);
        } else {
            return parent.isSameRM(xaResource);
        }
    }

    public int prepare(Xid xid) throws XAException {
        return parent.prepare(xid);
    }

    public Xid[] recover(int flag) throws XAException {
        Xid[] result = parent.recover(flag);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "recover returned " + (result == null ? null : Arrays.asList(result)));
        }
        return result;
    }

    public void rollback(Xid xid) throws XAException {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "rollback " + xid);
        }
        parent.rollback(xid);
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        return parent.setTransactionTimeout(seconds);
    }

    public void start(Xid xid, int flags) throws XAException {
        parent.start(xid, flags);
    }
    
    public void destroy() throws SIException {
        connection.close();
    }
}
