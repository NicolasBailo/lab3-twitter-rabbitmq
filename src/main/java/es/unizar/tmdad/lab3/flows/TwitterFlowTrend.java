package es.unizar.tmdad.lab3.flows;

import java.util.*;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.social.twitter.api.Tweet;

@Configuration
@Profile("fanout")
public class TwitterFlowTrend extends TwitterFlowCommon {

    final static String TWITTER_FANOUT_A_QUEUE_NAME = "twitter_trends";

    @Autowired
    FanoutExchange fanOutExchange;

    @Autowired
    RabbitTemplate rabbitTemplate;

    // Configuración RabbitMQ
    @Bean
    Queue aTwitterTrendsQueue() {
        return new Queue(TWITTER_FANOUT_A_QUEUE_NAME, false);
    }

    @Bean
    Binding twitterFanoutBinding() {
        return BindingBuilder.bind(aTwitterTrendsQueue())
                .to(fanOutExchange);
    }

    @Override
    @Bean
    public DirectChannel requestChannelRabbitMQ() { return MessageChannels.direct().get(); }

    @Bean
    public DirectChannel requestChannelTwitter() { return MessageChannels.direct().get(); }

    @Bean
    public IntegrationFlow sendTrendsToRabbitMQ() {
        return IntegrationFlows
                .from(requestChannelTwitter())
                .filter(flow -> flow instanceof Tweet)
                .aggregate(aggregationSpec())
                .transform(getTrends())
                .handle("streamSendingService", "sendTrends").get();
    }

    private GenericTransformer<List<Tweet>, List<Map.Entry<String, Integer>>> getTrends() {
        return tweetFlow -> {
            Map<String, Integer> hashtags = new HashMap<String, Integer>();

            tweetFlow.stream().forEach(tweet -> {
                tweet.getEntities().getHashTags().forEach(hashT -> {
                    hashtags.put(hashT.getText(), hashtags.getOrDefault(hashT.getText(), 0) + 1);
                });
            });

            List<Map.Entry<String, Integer>> trendList = new ArrayList<>(hashtags.entrySet());
            trendList.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));
            return trendList.subList(0, 10);
        };
    }

    private Consumer<AggregatorSpec> aggregationSpec() {
        return a -> a.correlationStrategy(m -> 1)
                .releaseStrategy(g -> g.size() == 1000)
                .expireGroupsUponCompletion(true);
    }

    @Bean
    public AmqpInboundChannelAdapter amqpInbound() {
        SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer(
                rabbitTemplate.getConnectionFactory());
        smlc.addQueues(aTwitterTrendsQueue());
        return Amqp.inboundAdapter(smlc)
                .outputChannel(requestChannelRabbitMQ()).get();
    }
}