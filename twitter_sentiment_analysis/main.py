from settings import API_KEY, API_SECRET_KEY, TOKEN, TOKEN_SECRET
import tweepy

auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(TOKEN, TOKEN_SECRET)
api = tweepy.API(auth)

from settings import non_retweeted_tweets


def tweets_search(query, language, count):
    data = []
    tweets = []
    max_id = None
    while len(data) < count:
        tweet_batch = api.search(q=query, lang=language, count=100, tweet_mode="extended", max_id=max_id)
        try:
            max_id = tweet_batch[-1].id # max_id for the next query is the id of current query's last tweet
            for tweet in tweet_batch:
                tweets.append(tweet)
        except IndexError:
            print('index error')
        # Keep only non-retweeted (newly created) tweets
        retweeted_tweets = non_retweeted_tweets(tweets)

        for tweet in retweeted_tweets:
            text = tweet.full_text
            data.append(text)
    # We print the last id of last query in case we want to pursue querying later
    print('last max_id: ' + str(max_id))

    return data


def search(keywords, language, count):
    data =tweets_search(keywords, language, count)

    from settings import remove_url, remove_tags

    for i in range(len(data)):
        data[i] = remove_url(data[i])

    for i in range(len(data)):
        data[i] = remove_tags(data[i])

    from nltk.sentiment.vader import SentimentIntensityAnalyzer

    vader = SentimentIntensityAnalyzer()

    scores = []
    for text in data:
        scores.append(vader.polarity_scores(text))

    import pandas as pd

    score_df = pd.DataFrame(scores)

    df = pd.DataFrame(columns=['score', 'type'])
    for i in ['neg','pos']:
        df_temp = pd.DataFrame(score_df[i].copy())
        df_temp.loc[:, 'type'] = i
        df_temp = df_temp.rename(columns={i: 'score'})
        df = pd.concat([df, df_temp], axis=0, sort=True)

    mean = score_df.mean()

    return df, mean