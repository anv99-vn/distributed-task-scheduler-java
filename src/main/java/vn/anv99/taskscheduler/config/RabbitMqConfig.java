package vn.anv99.taskscheduler.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    @Bean
    TopicExchange taskExchange(SchedulerProperties properties) {
        return new TopicExchange(properties.exchangeName(), true, false);
    }

    @Bean
    FanoutExchange deadLetterExchange(SchedulerProperties properties) {
        return new FanoutExchange(properties.deadLetterExchangeName(), true, false);
    }

    @Bean
    Queue workerQueue(SchedulerProperties properties) {
        return QueueBuilder.durable(properties.workerQueueName())
                .deadLetterExchange(properties.deadLetterExchangeName())
                .ttl(86_400_000)
                .build();
    }

    @Bean
    Queue deadLetterQueue(SchedulerProperties properties) {
        return QueueBuilder.durable(properties.deadLetterQueueName()).build();
    }

    @Bean
    Binding workerBinding(Queue workerQueue, TopicExchange taskExchange, SchedulerProperties properties) {
        return BindingBuilder.bind(workerQueue).to(taskExchange).with(properties.workerRoutingKey());
    }

    @Bean
    Binding deadLetterBinding(Queue deadLetterQueue, FanoutExchange deadLetterExchange) {
        return BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange);
    }

    @Bean
    MessageConverter jacksonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter
    ) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setPrefetchCount(10);
        return factory;
    }
}
