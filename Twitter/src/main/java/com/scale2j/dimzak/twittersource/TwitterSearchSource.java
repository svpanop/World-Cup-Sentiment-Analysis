package com.scale2j.dimzak.twittersource;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A flume source for consuming tweets using Twitter Search API
 *
 * @author Dimitris Zakas
 */
public class TwitterSearchSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger =
            LoggerFactory.getLogger(TwitterSearchSource.class);

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private String[] keywords;

    private Twitter twitter;

    private Query query;

    /**
     * Acts as a Constructor for the sink. Uses flume context
     * to load properties from flume's configuration file
     */
    @Override
    public void configure(Context context) {
        consumerKey = context.getString("consumerKey");
        consumerSecret = context.getString("consumerSecret");
        accessToken = context.getString("accessToken");
        accessTokenSecret = context.getString("accessTokenSecret");

        // Default value = 0
        String keywordString = context.getString("keywords", "");
        if (keywordString.trim().length() == 0) {
            keywords = new String[0];
        } else {
            keywords = keywordString.split(",");
            for (int i = 0; i < keywords.length; i++) {
                keywords[i] = keywords[i].trim();
            }
        }

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        query = new Query("hadoop");
        // TODO construct query

        twitter = new TwitterFactory(cb.build()).getInstance();
    }

    /**
     * Start
     */
    @Override
    public void start() {
        // The channel is the piece of Flume that sits between the Source and Sink,
        // and is used to process events.
        final ChannelProcessor channel = getChannelProcessor();

        final Map<String, String> headers = new HashMap<String, String>();

        logger.debug("Setting up Twitter sample stream using consumer key {} and" +
                " access token {}", new String[]{consumerKey, accessToken});

        // TODO maybe break per keyword type(geo, keyword)
        if (keywords.length == 0) {
            logger.debug("Starting up Twitter sampling...");
            twitter.search();
        } else {
            logger.debug("Starting up Twitter sampling...");
            twitter.search();
        }

        try {
            QueryResult result;
            result = twitter.search(query);
            List<Status> tweets = result.getTweets();

            for (Status tweet : tweets) {
                logger.info("USER: " + tweet.getUser().getScreenName() + "\nTEXT: " + tweet.getText() + "\nAT: " + tweet.getCreatedAt() + "\n");

                headers.put("timestamp", String.valueOf(tweet.getCreatedAt().getTime()));
                Event event = EventBuilder.withBody(
                        DataObjectFactory.getRawJSON(tweet).getBytes(), headers);

                channel.processEvent(event);
            }
            logger.info("Got " + tweets.size() + " tweets, kudos!");


        } catch (TwitterException te) {
            te.printStackTrace();
            logger.error("Failed to search tweets: " + te.getMessage());

        }

        // TODO it hangs ;(
        //super.start();
    }

    /**
     * Stop
     */
    @Override
    public void stop() {
        logger.info("Shutting down Twitter...");
        twitter.shutdown();
        super.stop();
    }
}
