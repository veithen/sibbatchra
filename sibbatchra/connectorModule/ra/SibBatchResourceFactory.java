package ra;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAResource;

import com.ibm.websphere.sib.exception.SIException;
import com.ibm.ws.Transaction.DestroyXAResourceException;
import com.ibm.ws.Transaction.XAResourceFactory;
import com.ibm.ws.Transaction.XAResourceInfo;
import com.ibm.ws.Transaction.XAResourceNotAvailableException;

public class SibBatchResourceFactory implements XAResourceFactory {
    private static final Logger logger = Logger.getLogger(SibBatchResourceFactory.class.getName());
    
    public SibBatchResourceFactory() {
        logger.log(Level.FINE, "Factory instance created");
    }
    
    public XAResource getXAResource(XAResourceInfo resourceInfo) throws XAResourceNotAvailableException {
        if (resourceInfo instanceof SibBatchResourceInfo) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Recreating XAResource from " + resourceInfo);
            }
            try {
                return new SibBatchRecoveryXAResource(((SibBatchResourceInfo)resourceInfo).createConnection());
            } catch (SIException ex) {
                throw new XAResourceNotAvailableException(ex);
            }
        } else {
            throw new IllegalArgumentException("Expected a XAResourceInfo instance of type " + SibBatchResourceInfo.class.getName());
        }
    }

    public void destroyXAResource(XAResource resource) throws DestroyXAResourceException {
        logger.log(Level.FINE, "Destroying XAResource");
        try {
            ((SibBatchRecoveryXAResource)resource).destroy();
        } catch (SIException ex) {
            throw new DestroyXAResourceException(ex);
        }
    }
}
