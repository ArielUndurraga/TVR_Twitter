package cl.citiaps.twitter.streaming;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.JSONObject;
import twitter4j.json.DataObjectFactory;

import com.mongodb.*;
import com.mongodb.util.JSON;


public class TwitterStreaming {

	private final TwitterStream twitterStream;
	private Set<String> keywords;

	private TwitterStreaming() {
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.keywords = new HashSet<>();
		loadKeywords();
	}

	private void loadKeywords() {
		try {
			ClassLoader classLoader = getClass().getClassLoader();
			keywords.addAll(IOUtils.readLines(classLoader.getResourceAsStream("words.dat"), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void init() throws IOException{
		Mongo m = new Mongo("127.0.0.1");
		DB db = m.getDB("test");
		final DBCollection twet = db.getCollection("tweet");

		StatusListener listener = new StatusListener() {

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}

			@Override
			public void onStatus(Status status) {
				//System.out.println(status.getId());
				//System.out.println(status.getText());
				JSONObject comentario = new JSONObject();
				JSONObject content = new JSONObject();
				String comment = "{\"id\" : \""+status.getId()+"\","
						+ " \"text\" : \""+status.getText()+"\","
								+ " \"location\" : \""+status.getGeoLocation()+"\","
										+ " \"date\" : \""+status.getCreatedAt()+"\"}";
				//String tweet = DataObjectFactory.getRawJSON(status);
				DBObject doc = (DBObject)JSON.parse(comment);
				System.out.println(comment);
				twet.insert(doc);

			}
		};

		FilterQuery fq = new FilterQuery();

		fq.track(keywords.toArray(new String[0]));

		this.twitterStream.addListener(listener);
		this.twitterStream.filter(fq);
	}
	
	public static void main(String[] args) {
		try {
			new TwitterStreaming().init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
