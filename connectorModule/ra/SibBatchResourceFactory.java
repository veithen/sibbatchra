package ra;

import javax.transaction.xa.XAResource;

import com.ibm.websphere.sib.exception.SIException;
import com.ibm.ws.Transaction.DestroyXAResourceException;
import com.ibm.ws.Transaction.XAResourceFactory;
import com.ibm.ws.Transaction.XAResourceInfo;
import com.ibm.ws.Transaction.XAResourceNotAvailableException;

public class SibBatchResourceFactory implements XAResourceFactory {
    public XAResource getXAResource(XAResourceInfo resourceInfo) throws XAResourceNotAvailableException {
        try {
            return ((SibBatchResourceInfo)resourceInfo).createConnection().getSIXAResource();
        } catch (SIException ex) {
            throw new XAResourceNotAvailableException(ex);
        }
    }

    public void destroyXAResource(XAResource resource) throws DestroyXAResourceException {
        // TODO: we need to close the connection here; to make this possible, we need to create an XAResource wrapper around the resource returned by getSIXResource
    }
}
