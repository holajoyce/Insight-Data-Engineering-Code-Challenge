# example of program that calculates the 
# number of tweets cleaned

import json
import sys

infname = "../tweet_input/tweets.txt"
outfname = "../tweet_output/ft1.txt"

# clean a single tweet if needed
def extract_hashtags(tweet):
    cleaned = False
    newtweet = ""
    for letter in tweet:
        if( ord(letter)>=0 and  ord(letter)<128):
            newtweet += letter
        else:
            cleaned=True
    return (1 if cleaned else 0,newtweet if cleaned else tweet)
    


def read_file_and_clean(infname,outfname):
    times_cleaned =0
    
    target = open(outfname,'w')
    target.truncate()
    
    with open(infname, 'r+') as f:
        for tweet_json in f:
            tweet = json.loads(tweet_json)
            if 'text' in tweet and 'created_at' in tweet:
                tweet_txt = tweet['text']
                (cleaned,new_tweet_text) = extract_hashtags(tweet_txt)
#                 tweet['text'] = new_tweet_text
#                 line = json.dumps(tweet)
                target.write(new_tweet_text+" ("+tweet['created_at']+")\n")
                times_cleaned += cleaned
    
    target.write("\n"+str(times_cleaned)+" tweets contained unicode.\n")
    target.close()

def main():
    print("processing")
    read_file_and_clean(sys.argv[1],sys.argv[2])
    print("done")

if __name__ == "__main__":
    main()