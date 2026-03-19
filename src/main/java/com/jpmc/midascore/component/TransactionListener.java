package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class TransactionListener {

    private final List<Transaction> receivedTransactions = new ArrayList<>();

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
        System.out.println("Received #" + receivedTransactions.size()
                + " | Amount: " + transaction.getAmount());
    }

    public List<Transaction> getReceivedTransactions() {
        return receivedTransactions;
    }
}