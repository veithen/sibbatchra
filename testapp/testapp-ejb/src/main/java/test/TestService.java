package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.annotation.Resource;
import javax.ejb.EJBException;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.sql.DataSource;

@WebService
@Stateless
public class TestService {
    @Resource
    private QueueConnectionFactory qcf;
    
    @Resource
    private Queue queue;
    
    @Resource
    private DataSource ds;
    
    @WebMethod
    @WebResult(name="uuid")
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public String[] put(@WebParam(name="count") int count, @WebParam(name="duration") int duration) {
        try {
            Connection c = ds.getConnection();
            try {
                QueueConnection qc = qcf.createQueueConnection();
                try {
                    QueueSession session = qc.createQueueSession(false, 0);
                    try {
                        QueueSender sender = session.createSender(queue);
                        try {
                            PreparedStatement p = c.prepareStatement("insert into message (uuid) values (?)");
                            try {
                                List<String> results = new ArrayList<String>(count);
                                for (int i=0; i<count; i++) {
                                    UUID uuid = UUID.randomUUID();
                                    p.setString(1, uuid.toString());
                                    p.addBatch();
                                    sender.send(session.createObjectMessage(new TestMessage(uuid, duration)));
                                    results.add(uuid.toString());
                                }
                                p.executeBatch();
                                return results.toArray(new String[count]);
                            } finally {
                                p.close();
                            }
                        } finally {
                            sender.close();
                        }
                    } finally {
                        session.close();
                    }
                } finally {
                    qc.close();
                }
            } finally {
                c.close();
            }
        } catch (SQLException ex) {
            throw new EJBException(ex);
        } catch (JMSException ex) {
            throw new EJBException(ex);
        }
    }
}
