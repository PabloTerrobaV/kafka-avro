/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.example.kafka.PaymentMethod;
@org.apache.avro.specific.AvroGenerated
public enum PaymentMethod implements org.apache.avro.generic.GenericEnumSymbol<PaymentMethod> {
  CREDIT_CARD, PAYPAL, CASH, OTHER  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"PaymentMethod\",\"namespace\":\"com.example.kafka.PaymentMethod\",\"symbols\":[\"CREDIT_CARD\",\"PAYPAL\",\"CASH\",\"OTHER\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
}
