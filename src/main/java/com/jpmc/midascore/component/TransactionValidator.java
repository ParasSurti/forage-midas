package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class TransactionValidator {

    private final UserRepository userRepository;

    public TransactionValidator(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public boolean isValid(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) return false;

        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) return false;

        if (sender.getBalance() < transaction.getAmount()) return false;

        return true;
    }
}