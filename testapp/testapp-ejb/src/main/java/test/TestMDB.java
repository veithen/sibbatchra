package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJBException;
import javax.ejb.MessageDriven;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.sql.DataSource;

@MessageDriven(
		activationConfig = { @ActivationConfigProperty(
				propertyName = "destinationType", propertyValue = "javax.jms.Queue"
		) })
public class TestMDB implements MessageListener {
    @Resource
    private DataSource ds;
    
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void onMessage(Message message) {
        try {
            TestMessage testMessage = (TestMessage)((ObjectMessage)message).getObject();
            Connection c = ds.getConnection();
            try {
                PreparedStatement s = c.prepareStatement("delete from message where uuid=?");
                try {
                    s.setString(1, testMessage.getUuid().toString());
                    if (s.executeUpdate() != 1) {
                        throw new EJBException("Message with UUID " + testMessage.getUuid() + " not found");
                    }
                } finally {
                    s.close();
                }
            } finally {
                c.close();
            }
            try {
                Thread.sleep(testMessage.getDuration());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        } catch (SQLException ex) {
            throw new EJBException(ex);
        } catch (JMSException ex) {
            throw new EJBException(ex);
        }
    }
}
