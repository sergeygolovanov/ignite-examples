package com.examples.ignite.domain;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

public class Address implements Binarylizable {

    private String street;

    private int zip;

    public Address() {
    }

    public Address(String street, int zip) {
        this.street = street;
        this.zip = zip;
    }

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        System.out.println("----------------------------------------------");
        writer.writeString("street", street);
        writer.writeInt("zip", zip);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        street = reader.readString("street");
        zip = reader.readInt("zip");
    }

    @Override
    public String toString() {
        return "Address{" +
                "street='" + street + '\'' +
                ", zip=" + zip +
                '}';
    }

}
