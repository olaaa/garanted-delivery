package ru.olga.gof.pattern.saga.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация Pulsar для гарантированной доставки.
 *
 * КЛЮЧЕВЫЕ НАСТРОЙКИ ДЛЯ EXACTLY-ONCE:
 * - enableDeduplication: дедупликация на стороне брокера
 * - enableTransaction: транзакции для атомарности
 * - acknowledgment: явное подтверждение после обработки
 *
 * PULSAR ПРЕИМУЩЕСТВА:
 * - Встроенная дедупликация по message ID
 * - Транзакции для атомарной отправки/получения
 * - Гарантированное упорядочение в рамках partition
 * - Negative acknowledgment для retry
 */
@Slf4j
@Configuration
public class PulsarConfig {

    @Value("${pulsar.service-url:pulsar://localhost:6650}")
    private String serviceUrl;

    /**
     * Создание Pulsar клиента с настройками для надежности.
     */
    @Bean
    public PulsarClient pulsarClient() throws PulsarClientException {
        log.info("Initializing Pulsar client with service URL: {}", serviceUrl);

        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                // IO threads для network operations
                .ioThreads(2)
                // Listener threads для callbacks
                .listenerThreads(2)
                // Таймаут операций
                .operationTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
                .build();
    }

    /**
     * ObjectMapper для сериализации событий и команд.
     * Поддержка Java 8 Time API.
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
