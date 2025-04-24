package com.example.kafka.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.context.annotation.ComponentScan;

import com.example.kafka.Order; // Clases generadas por Avro
import com.example.kafka.PaymentMethod.PaymentMethod;
import com.example.kafka.OrderStatus.OrderStatus;

import org.apache.avro.Schema;
import org.apache.avro.JsonProperties;

import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;

import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
@ComponentScan(basePackages = "com.example.kafka")
public class OrderProducer {

    // Configuración de constantes para Kafka y Schema Registry
    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    // private static final String BOOTSTRAP_SERVERS = "http://host.docker.internal:9092";
    // private static final String BOOTSTRAP_SERVERS = "http://kafka:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    // private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081"; // Para utilizar las imágenes de Docker

    public static void main(String[] args) {
        SpringApplication.run(OrderProducer.class, args);

        // Configuración de propiedades para el productor Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Asegura que el mensaje se ha replicado completamente antes de confirmar

        // Creación del productor Kafka
        Producer<String, Order> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        // Verificación de conexión con Kafka
        System.out.println("⚠ Probando conexión con Kafka...");
        try {
            // Intenta obtener información sobre las particiones del topic
            producer.partitionsFor(TOPIC).forEach(p ->
                    System.out.println("✅ Nodo disponible: " + p.leader().host()));
        } catch (Exception e) {
            System.err.println("❌ NO SE PUDO CONECTAR A KAFKA: " + e.getMessage());
            System.exit(1); // Termina el programa si no se puede conectar a Kafka
        }

        // Bucle principal para crear órdenes
        while (true) {
            System.out.print("\n¿Crear nueva orden? (Enter/Sí | 'exit'): ");
            String command = scanner.nextLine();
            if ("exit".equalsIgnoreCase(command)) break; // Sale del bucle si el usuario escribe 'exit'

            Map<String, Object> fieldValues = new HashMap<>();

            // Iteración sobre los campos del esquema Avro de Order
            for (Schema.Field field : Order.getClassSchema().getFields()) {
                Schema fieldSchema = getNonNullSchema(field.schema());
                boolean isRequired = !isNullable(field.schema()) && field.defaultVal() == null;

                String defaultText = getHumanReadableDefault(field);
                System.out.printf("%n%s (%s)%s: ", field.name(), fieldSchema.getType().getName().toLowerCase(), defaultText);

                // Bucle para obtener un valor válido para cada campo
                Object value = null;
                while (value == null) {
                    String input = scanner.nextLine().trim();
                    try {
                        if (input.isEmpty()) {
                            // Manejo de entrada vacía
                            if (isNullable(field.schema())) {
                                value = null; // Permite null para campos opcionales
                                break;
                            } else if (field.defaultVal() != null) {
                                value = handleDefault(field); // Usa el valor por defecto si está disponible
                                break;
                            } else if (isRequired) {
                                throw new IllegalArgumentException("Campo obligatorio");
                            }
                        } else {
                            // Convierte la entrada del usuario al tipo de dato correcto
                            value = convertInput(input, field.schema());
                        }
                    } catch (Exception e) {
                        System.out.printf("✖ Error: %s. Intenta nuevamente: ", e.getMessage());
                    }
                }
                fieldValues.put(field.name(), value);
            }

            // Construcción del objeto Order con los valores recopilados
            Order order = buildOrder(fieldValues);

            // Creación del registro para enviar a Kafka
            ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, (String) fieldValues.get("id"), order);

            // Envío del mensaje a Kafka
            try {
                // Envía el mensaje y espera la confirmación (operación bloqueante)
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("%n✅ Orden enviada!%nPartición: %d | Offset: %d%n%s%n",
                        metadata.partition(),
                        metadata.offset(),
                        "=".repeat(50));
            } catch (Exception e) {
                System.err.printf("%n❌ Error enviando orden:%n%s%n", e.getMessage());
                if (e.getCause() != null) {
                    System.err.println("Causa raíz: " + e.getCause().getMessage());
                }
            }
        }

        // Cierre de recursos
        scanner.close();
        producer.close(); // Importante cerrar el productor para liberar recursos
        System.out.println("\n🚪 Producer cerrado");
    }

    // Método para construir el objeto Order a partir de los valores recopilados
    private static Order buildOrder(Map<String, Object> fieldValues) {
        // Crea una nueva instancia de Order usando los valores del Map
        // Cada getter corresponde a un campo en el esquema Avro
        return new Order(
                (String) fieldValues.get("id"),
                (String) fieldValues.get("customer_name"),
                (String) fieldValues.get("nationality"),
                (String) fieldValues.get("email"),
                (Float) fieldValues.get("total_price"),
                (String) fieldValues.get("product"),
                (Integer) fieldValues.get("quantity"),
                (Integer) fieldValues.get("discount"),
                // (Boolean) fieldValues.get("is_gift"),
                (String) fieldValues.get("currency"),
                (PaymentMethod) fieldValues.get("payment_method"),
                (OrderStatus) fieldValues.get("order_status")
        );
    }

    // Método para convertir la entrada del usuario al tipo de dato correcto según el esquema
    private static Object convertInput(String input, Schema schema) throws Exception {
        // Si el schema es de tipo UNION, obtiene el schema no nulo
        Schema targetSchema = schema.getType() == Schema.Type.UNION ? getNonNullSchema(schema) : schema;

        switch (targetSchema.getType()) {
            case STRING: return input;
            case INT: return Integer.parseInt(input);
            case FLOAT: return Float.parseFloat(input);
            case BOOLEAN: return parseBoolean(input);
            case ENUM: return validateEnum(input, targetSchema);
            case UNION: return handleUnionType(input, schema);
            default: throw new IllegalArgumentException("Tipo no soportado: " + targetSchema.getType());
        }
    }

    // Método para validar y convertir valores de tipo ENUM
    private static Enum<?> validateEnum(String input, Schema enumSchema) throws Exception {
        List<String> validValues = enumSchema.getEnumSymbols();
        String upperInput = input.toUpperCase();

        // Verifica si el valor ingresado es válido para el ENUM
        if (!validValues.contains(upperInput)) {
            throw new IllegalArgumentException("Valores permitidos: " + validValues);
        }

        // Convierte el string a la instancia ENUM correspondiente
        return Enum.valueOf((Class<? extends Enum>) getEnumClass(enumSchema), upperInput);
    }

    // Método para manejar valores por defecto
    private static Object handleDefault(Schema.Field field) {
        // Si el valor por defecto es null, retorna null explícitamente
        if (field.defaultVal() instanceof JsonProperties.Null) {
            return null;
        }
        // Retorna el valor por defecto definido en el esquema
        return field.defaultVal();
    }

    // Método para manejar tipos UNION (generalmente para campos opcionales)
    private static Object handleUnionType(String input, Schema unionSchema) throws Exception {
        // Filtra los tipos no nulos del schema UNION
        List<Schema> nonNullTypes = unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .collect(Collectors.toList());

        // Verifica que solo haya un tipo no nulo (no soporta uniones complejas)
        if (nonNullTypes.size() != 1) {
            throw new IllegalArgumentException("Unión compleja no soportada");
        }

        // Convierte el input al tipo no nulo encontrado
        return convertInput(input, nonNullTypes.get(0));
    }

    // Método para obtener una representación legible de los valores por defecto
    private static String getHumanReadableDefault(Schema.Field field) {
        if (field.defaultVal() == null) {
            return isNullable(field.schema()) ? " [opcional]" : " [requerido]";
        }

        if (field.defaultVal() instanceof JsonProperties.Null) {
            return " [default: null]";
        }

        return " [default: " + field.defaultVal() + "]";
    }

    // Método para determinar si un campo es nullable (puede ser null)
    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION &&
                schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    // Método para obtener el esquema no nulo de un tipo UNION
    private static Schema getNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            // Busca el primer tipo no nulo en la unión
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unión sin tipos válidos"));
        }
        return schema;
    }

    // Método para parsear valores booleanos de manera más flexible
    private static Boolean parseBoolean(String input) {
        if ("sí".equalsIgnoreCase(input) || "si".equalsIgnoreCase(input)) return true;
        if ("no".equalsIgnoreCase(input)) return false;
        return Boolean.parseBoolean(input);
    }

    // Método para obtener la clase Enum a partir del esquema
    @SuppressWarnings("unchecked")
    private static Class<? extends Enum<?>> getEnumClass(Schema schema) {
        try {
            // Intenta cargar la clase Enum basada en el nombre completo del schema
            return (Class<? extends Enum<?>>) Class.forName(schema.getFullName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Clase Enum no encontrada: " + schema.getFullName(), e);
        }
    }
}


/*
public class OrderProducer {
    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Producer<String, Order> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter order ID (or 'exit' to quit): ");
            String id = scanner.nextLine();
            if ("exit".equalsIgnoreCase(id)) {
                break;
            }

            System.out.print("Enter customer name: ");
            String customerName = scanner.nextLine();

            System.out.print("Enter user email: ");
            String email = scanner.nextLine();

            System.out.print("Enter total price: ");
            String priceInput = scanner.nextLine(); // Leer como String
            float totalPrice = Float.parseFloat(priceInput);

            // System.out.print("Is paid? (true/false): ");
            // boolean isPaid = Boolean.parseBoolean(scanner.nextLine());

            System.out.print("Enter product name (or press Enter to skip): ");
            String product = scanner.nextLine();
            if (product.isEmpty()) {
                product = null; // Si el usuario no introduce un valor, se establece como null
            }

            System.out.print("Is gift? (true/false, default=false): ");
            boolean isGift = Boolean.parseBoolean(scanner.nextLine());

            // System.out.print("Enter quantity (or press Enter to skip): ");
            // String quantityInput = scanner.nextLine();
            // Integer quantity = quantityInput.isEmpty() ? null : Integer.parseInt(quantityInput);

            // System.out.print("Enter discount value (or press Enter to skip): ");
            // String discountInput = scanner.nextLine();
            // Float discount = discountInput.isEmpty() ? null : Float.parseFloat(discountInput);

            // System.out.print("Enter priority (default=1): ");
            // int priority = Integer.parseInt(scanner.nextLine());

            System.out.print("Enter currency (default: USD): ");
            String currency = scanner.nextLine();

            // Mostrar las opciones de PaymentMethod al usuario
            System.out.println("Select payment method (CREDIT_CARD, PAYPAL, CASH, OTHER) or press Enter to skip:");
            String paymentMethodInput = scanner.nextLine();

            /*
            // Validar el valor ingresado (opcional)
            if (paymentMethod.isEmpty()) {
                paymentMethod = null; // Permitir valor nulo si el usuario presiona Enter
            } else {
                paymentMethod = paymentMethod.toUpperCase();
                if (!paymentMethod.equals("CREDIT_CARD") && !paymentMethod.equals("PAYPAL")
                        && !paymentMethod.equals("CASH") && !paymentMethod.equals("OTHER")) {
                    System.out.println("❌ Invalid payment method. Using 'OTHER' by default.");
                    paymentMethod = "OTHER";
                }
            }
            */

            /*
            // Manejar valor nulo o convertir a PaymentMethod
            // Debemos indicar el directorio completo para la utilización de las clases autogeneradas para los campos de tipo enumeración.
            com.example.kafka.PaymentMethod.PaymentMethod paymentMethod = paymentMethodInput.isEmpty() ? null : com.example.kafka.PaymentMethod.PaymentMethod.valueOf(paymentMethodInput.toUpperCase());

            // Debemos indicar el directorio completo para la utilización de las clases autogeneradas para los campos de tipo enumeración.
            System.out.print("Select order status (PENDING, SHIPPED, DELIVERED, CANCELLED): ");
            com.example.kafka.OrderStatus.OrderStatus orderStatus = com.example.kafka.OrderStatus.OrderStatus.valueOf(scanner.nextLine().toUpperCase());

            // Crear el objeto Order (basado en el esquema Avro)
            // Order order = new Order(id, customerName, totalPrice, product, quantity);

            Order order = new Order(id, customerName, email, totalPrice, product, isGift, currency, paymentMethod, orderStatus);  // , quantity);

            ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, id, order);

            // Enviar mensaje
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("✅ Order sent: ID=%s, Customer=%s, Email=%s, Price=%.2f, Product=%s, Currency=%s, PaymentMethod=%s, OrderStatus=%s%n",
                            order.getId(), order.getCustomerName(), order.getEmail(), order.getTotalPrice(), order.getProduct(),
                            order.getCurrency(), paymentMethod.name(), order.getOrderStatus());// order.getPaymentMethod());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        scanner.close();
        producer.close();
    }
}
*/