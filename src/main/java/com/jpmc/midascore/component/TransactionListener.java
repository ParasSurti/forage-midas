package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final TransactionValidator validator;
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public TransactionListener(TransactionValidator validator,
                               UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository) {
        this.validator = validator;
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(Transaction transaction) {
        if (!validator.isValid(transaction)) {
            return;
        }

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        transactionRecordRepository.save(
                new TransactionRecord(sender, recipient, transaction.getAmount())
        );

        userRepository.findAll().forEach(user -> {
            if (user.getName().equalsIgnoreCase("waldorf")) {
                System.out.println("WALDORF BALANCE: " + user.getBalance());
            }
        });
    }
}