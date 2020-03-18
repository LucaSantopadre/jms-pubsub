package com.spacelaunch.sub;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * Subscriber
 */
public class Subscriber {

    public static void main(String[] args) throws Exception {
        thread(new Sub(), false);
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }


    public static class Sub implements Runnable, ExceptionListener {
        public void run() {
        	
        		try {

                    // Create a ConnectionFactory
                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);

                    // Create a Connection
                    Connection connection = connectionFactory.createConnection();
                    connection.setClientID("id_1");
                    connection.start();

                    connection.setExceptionListener(this);
                    

                    // Create a Session
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                    // Create the destination (Topic or Queue)
                    Topic topic = session.createTopic("test.topic1");

                    // Create a MessageConsumer from the Session to the Topic or Queue
                    TopicSubscriber sub = session.createDurableSubscriber(topic, "sub1");

                    
                    int i = 0;
                    while(i < 100) {
                    	// Wait for a message
                        Message message = sub.receive(1000);
                        
                        if (message instanceof TextMessage) {
                            TextMessage textMessage = (TextMessage) message;
                            String text = textMessage.getText();
                            System.out.println("Message "+ i + " - expired: " + message.getJMSExpiration() + " - TEXT:" + text);
                        } else {
                            System.out.println("Other "+ i + ": " + message);
                        }
                        i++;
                    }
                    

                    sub.close();
                    session.close();
                    connection.close();
                } catch (Exception e) {
                    System.out.println("Caught: " + e);
                    e.printStackTrace();
                }
            }
        	
            

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}