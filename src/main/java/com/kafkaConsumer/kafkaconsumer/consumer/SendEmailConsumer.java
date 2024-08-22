package com.kafkaConsumer.kafkaconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaConsumer.kafkaconsumer.dtos.KafkaMessage;
import com.kafkaConsumer.kafkaconsumer.utility.EmailUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import java.util.Properties;

@Service
public class SendEmailConsumer {


    private ObjectMapper objectMapper;
    private EmailUtil emailUtil;

    public SendEmailConsumer(ObjectMapper objectMapper, EmailUtil emailUtil) {
        this.objectMapper = objectMapper;
        this.emailUtil = emailUtil;
    }

    @KafkaListener(topics = "userSignUp" , groupId = "Grocery_Store")
    public void sendEmail(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

        Session session = session();

        emailUtil.sendEmail(
                session,
                kafkaMessage.getTo(),
                kafkaMessage.getSubject(),
                kafkaMessage.getBody()
        );
    }



    @KafkaListener(topics = "newAddress" , groupId = "Grocery_Store")
    public void sendEmailForAddress(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

        Session session = session();

        emailUtil.sendEmail(
                session,
                kafkaMessage.getTo(),
                kafkaMessage.getSubject(),
                kafkaMessage.getBody()
        );
    }

    @KafkaListener(topics = "productAddedToCart" , groupId = "Grocery_Store")
    public void sendEmailForCart(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

        Session session = session();

        emailUtil.sendEmail(
                session,
                kafkaMessage.getTo(),
                kafkaMessage.getSubject(),
                kafkaMessage.getBody()
        );
    }

    @KafkaListener(topics = "order" , groupId = "Grocery_Store")
    public void sendEmailForOrder(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

        Session session = session();

        emailUtil.sendEmail(
                session,
                kafkaMessage.getTo(),
                kafkaMessage.getSubject(),
                kafkaMessage.getBody()
        );
    }

    @KafkaListener(topics = "shopRegistration" , groupId = "Grocery_Store")
    public void sendEmailForShopRegistration(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

        Session session = session();

        emailUtil.sendEmail(
                session,
                kafkaMessage.getTo(),
                kafkaMessage.getSubject(),
                kafkaMessage.getBody()
        );
    }




    public Session session() {
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.gmail.com"); //SMTP Host
        props.put("mail.smtp.port", "587"); //TLS Port
        props.put("mail.smtp.auth", "true"); //enable authentication
        props.put("mail.smtp.starttls.enable", "true"); //enable STARTTLS

        //create Authenticator object to pass in Session.getInstance argument
        Authenticator auth = new Authenticator() {
            //override the getPasswordAuthentication method
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("grocerystore4169@gmail.com", "");
            }
        };
        Session session = Session.getInstance(props, auth);

        return session;
    }
}
