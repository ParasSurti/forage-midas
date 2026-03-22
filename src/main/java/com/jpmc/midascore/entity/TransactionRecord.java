package com.jpmc.midascore.entity;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "transaction_record")
public class TransactionRecord {

    @Id
    @GeneratedValue
    private long id;

    @ManyToOne
    private UserRecord sender;

    @ManyToOne
    private UserRecord recipient;

    private float amount;

    protected TransactionRecord() {}

    public TransactionRecord(UserRecord sender,
                             UserRecord recipient,
                             float amount) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
    }
}