# example of program that calculates the 
# number of tweets cleaned

import json
import sys
import re
import time

infname = "../tweet_input/tweets.txt"
outfname = "../tweet_output/ft1.txt"

# clean a single tweet if needed
def remove_non_ascii(tweet):
    has_removed_unicode = False
    newtweet = ""
    for letter in tweet:
        if( ord(letter)>=0 and  ord(letter)<128):
            newtweet += letter
        else:
            has_removed_unicode=True
    
    # return count of 1 if there's a unicode, and also the unicode free text
    return (1 if has_removed_unicode else 0,newtweet if has_removed_unicode else tweet)

def further_clean_text(tweet):    
    tweet = re.sub(r"(\n|\t)"," ", tweet)
    tweet = tweet.replace("\/","/").replace("\\\\",'\\').replace('\"','"').replace("\'","'")
    return tweet

def read_file_and_clean(infname,outfname):
    times_cleaned =0
    target = open(outfname,'w')
    target.truncate()
    with open(infname, 'r+') as f:
        for tweet_json in f:
            tweet = json.loads(tweet_json)
            if 'text' in tweet and 'created_at' in tweet:
                tweet_txt = tweet['text']
                (cleaned,new_tweet_text) = remove_non_ascii(tweet_txt)
                target.write(further_clean_text(new_tweet_text)+" ("+tweet['created_at']+")\n")
                times_cleaned += cleaned
    target.write("\n"+str(times_cleaned)+" tweets contained unicode.\n")
    target.close()

def main():
    print("processing")
    start = time.time()
    read_file_and_clean(sys.argv[1],sys.argv[2])
    end = time.time()
    elapsed = end - start
    print("done in "+str(elapsed)+"s")

if __name__ == "__main__":
    main()
