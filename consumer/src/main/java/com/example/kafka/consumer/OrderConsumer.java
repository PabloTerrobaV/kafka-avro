package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.example.kafka.Order;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
@ComponentScan(basePackages = "com.example.kafka.controllers") // Incluye el paquete del controlador
public class OrderConsumer {

    public static void main(String[] args) {
        SpringApplication.run(OrderConsumer.class, args);
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        // props.put("schema.registry.url", "http://schema-registry:8081"); // Para utilizar las imñagenes de Docker
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.reader", true); // Habilita la deserialización a objetos Avro específicos
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void listen(Order order) {
        System.out.println("📥 Received order:");
        System.out.println(order);
        System.out.println("-------------------------------------");
    }
}



/*
package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.example.kafka.Order;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
public class OrderConsumer {

    public static void main(String[] args) {
        SpringApplication.run(OrderConsumer.class, args);
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.reader", true); // Habilita la deserialización a objetos Avro específicos
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void listen(Order order) {
        System.out.println("📥 Received order:");
        System.out.println(order);
        System.out.println("-------------------------------------");
    }
}
 */


/*
package com.example.kafka.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.kafka.Order; // Importa la clase generada a partir del esquema Avro

import org.apache.avro.Schema;
import org.apache.avro.JsonProperties;

import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class OrderConsumer {
    // Configuración de constantes para Kafka y Schema Registry
    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    // private static final String BOOTSTRAP_SERVERS = "http://kafka:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    // private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final String GROUP_ID = "order-consumer-group";

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);

        // Configuración de propiedades para el consumidor Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Comienza a leer desde el inicio del topic si no hay offset guardado
        props.put("specific.avro.reader", true); // Habilita la deserialización a objetos Avro específicos

        // Creación del consumidor Kafka
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        // Suscripción al topic 'orders'
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("🔄 Listening for new orders...");

        // Bucle infinito para consumir mensajes
        while (true) {
            // Sondeo de registros cada 100 ms
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Order> record : records) {
                Order order = record.value();
                System.out.println("📥 Received order:");

                // Iteración sobre los campos del esquema para imprimir los valores
                Schema schema = order.getSchema();
                for (Schema.Field field : schema.getFields()) {
                    Object value = order.get(field.name());
                    String displayValue = formatValue(value, field);
                    System.out.printf("%s: %s%n", field.name(), displayValue);
                }

                System.out.println("-------------------------------------");
            }
        }
    }

    // Método para formatear los valores para su visualización
    private static String formatValue(Object value, Schema.Field field) {
        if (value == null || value instanceof JsonProperties.Null) {
            // Maneja valores nulos o JsonProperties.Null (usado por Avro para representar null)
            return field.defaultVal() != null ? "default: " + field.defaultVal() : "<null>";
        }

        if (field.schema().getType() == Schema.Type.ENUM) {
            // Para ENUMs, simplemente devuelve el nombre del valor enum
            return value.toString();
        }

        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean) {
            // Para tipos simples, convierte directamente a String
            return value.toString();
        }

        // Para otros tipos complejos, muestra un placeholder
        return "<complex type>";
    }
}
 */


/*
public class OrderConsumer {
    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "order-consumer-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.reader", true);  // 👈 Esto es clave

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("🔄 Listening for new orders...");

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Order> record : records) {
                Order order = record.value();

                System.out.printf("📥 Received order: ID=%s, Customer=%s, Email=%s, Price=%.2f, Product=%s, Gift=%b, Currency=%s, Payment Method=%s, Order Status=%s%n",
                        order.getId(),
                        order.getCustomerName(),
                        order.getEmail(),
                        order.getTotalPrice(),
                        // order.getIsPaid(),
                        order.getProduct() != null ? order.getProduct() : "N/A", // Evitar mostrar "null"
                        // order.getQuantity() != null ? order.getQuantity().toString() : "N/A",
                        // order.getDiscount() != null ? order.getDiscount() : 0.00f,
                        // order.getPriority(),
                        order.getIsGift(),
                        order.getCurrency() != null ? order.getCurrency() : "N/A",
                        order.getPaymentMethod() != null ? order.getPaymentMethod().name() : "N/A",
                        order.getOrderStatus()
                );
            }
        }
    }
}
*/
