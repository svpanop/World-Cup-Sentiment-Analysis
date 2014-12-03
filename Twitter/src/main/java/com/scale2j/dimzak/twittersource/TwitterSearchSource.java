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

import java.util.ArrayList;
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

        query = new Query("obama");
        query.setCount(100);
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


        int wantedTweets = 15000;
        long lastSearchID = Long.MAX_VALUE;
        int remainingTweets = wantedTweets;

        int counter = 0;

        //ArrayList<Status> tweets = new ArrayList<Status>();
        while (remainingTweets > 0) {
            remainingTweets = wantedTweets - counter;
            if (remainingTweets > 100) {
                query.count(100);
            } else {
                query.count(remainingTweets);
            }
            logger.info("querying for: " + query.getCount() + " results");


            try {
                logger.info("Searching Twitter...");
                QueryResult result = twitter.search(query);
                logger.info("GOT " + result.getTweets().size());
                counter = counter + result.getTweets().size();
                logger.info("Twitter counter: " + counter);
                List<Status> tweets = result.getTweets();
                Status s = tweets.get(tweets.size() - 1);
                lastSearchID = s.getId();
                query.setMaxId(lastSearchID);
                remainingTweets = wantedTweets - counter;
                logger.info("remaining: " + remainingTweets);

            } catch (TwitterException te) {
                logger.info("Couldn't connect: " + te);
            } catch (ArrayIndexOutOfBoundsException aioob) {
                logger.info("Couldn't retrieve more tweets...");
                logger.info("Retrieved " + counter + " tweets so far");
                break;
            }

        }

        logger.info("SIZE " + counter);

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

    /**
     * Create events based on a List<{@link twitter4j.Status}>
     */
    private void createEventsFromTweets(List<Status> tweets) {

        final ChannelProcessor channel = getChannelProcessor();

        Map<String, String> headers = new HashMap<String, String>();

        for (Status tweet : tweets) {
            logger.info("USER: " + tweet.getUser().getScreenName() + "\nTEXT: " + tweet.getText() + "\nAT: " + tweet.getCreatedAt() + "\n");

            headers.put("timestamp", String.valueOf(tweet.getCreatedAt().getTime()));
            Event event = EventBuilder.withBody(
                    DataObjectFactory.getRawJSON(tweet).getBytes(), headers);

            channel.processEvent(event);

        }
        logger.info("Got " + tweets.size() + " tweets, kudos!");

    }
}
