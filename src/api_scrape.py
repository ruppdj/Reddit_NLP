import praw, yaml
import time, calendar
from decimal import Decimal
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key, Attr


def parse_store_comment(sub, submission, comment, comment_table):
    '''
    Takes a comment object and adds it to the DB

    INPUT:
        sub - STR - the subreddit
        comment - PRAW Comment object -
        comment_table - DynamoDB table connection object
    '''

    # Name can be deleted so if that is the case replace with unknown
    try:
        full_name_c = comment.author_fullname
    except: 
        full_name_c = '*unknown'

    com = {
        'comment_id': comment.name,
        'post_id': submission.name,
        'comment_parent': comment.parent_id,
        'votes_up':comment.ups,
        'votes_down':comment.downs,
        'gilded':comment.gilded,
        'score':Decimal(str(comment.score)),
        'post_date':Decimal(str(comment.created)),
        'post_user':full_name_c,
        'awards':comment.all_awardings,   
        'body':comment.body,
        'comment_depth':comment.depth,
        'subreddit':sub
        }
        
    comment_table.put_item(Item = com)


def parse_store_post(sub, submission, post_table):
    '''
    Takes a post object and adds it to the DB

    INPUT:
        sub - STR - the subreddit
        submission - PRAW Submission object -
        post - DynamoDB table connection object
    '''
    try:
        full_name = submission.author_fullname
    except:
        full_name = '*unknown'
    
    post = {
        'post_id':submission.name,
        'votes_up':submission.ups,
        'votes_down':submission.downs,
        'gilded':submission.gilded,
        'votes_ratio':Decimal(str(submission.upvote_ratio)),
        'score':Decimal(str(submission.score)),
        'post_date':Decimal(str(submission.created)),
        'post_user':full_name,
        'awards':submission.all_awardings,   
        'body':submission.selftext,
        'title':submission.title,
        'url':submission.url,
        'subreddit':sub
    }

    post_table.put_item(Item = post)


def scrape_sub(sub, reddit_connection, post_table, comment_table):
    '''
    Scrapes a given subreddit of all posts and comments that are over 4 hours old storing them into a DynamoDB table

    INPUT
        sub - STR - Subreddit to scrape
        reddit_connection - PRAW connection object
        post_table - DynamoDB table connection object
        comment_table - DynamoDB table connection object
    '''
    
    # Can limit the amount of data to scrape.  Used mostly in testing.
    post_limit = 1000

    # Keep log of scraping for later use
    with open('scrape_progress.log', 'a+') as log:
        log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        log.write('\n SUBREDDIT: {} \n'.format(sub))
    

    for submission in reddit_connection.subreddit(sub).new(limit=post_limit):
        
        time.sleep(1)
        # Dont want the newest ones as I want comments to be there.
        if (calendar.timegm(time.gmtime()) - 14400) < submission.created_utc:
            continue

        # has this record been scraped already (this will need to be updated to just stop when I am finished)
        # Not sure this is the fastest way but was unable to find better
        # Switch from continue to break after initial scrape so only getting new data and stopping when get to scraped stuff
        if 'Item' in post_table.get_item(Key={'post_id': submission.name}).keys():
            break

        try:
            parse_store_post(sub, submission, post_table)
        
        except Exception as e:
                with open('scrape_post_error.log', 'a+') as log:
                    log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
                    log.write("\n type error: " + str(e))
                    log.write('\n Title: {} \n Name: {} \n'.format(submission.title,submission.name)
                    log.write('--------------------------- \n')
                continue
        
        
        # Get all comment data
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            try:
                parse_store_comment(sub, submission, comment, comment_table)
                
            except Exception as e:
                with open('scrape_comment_error.log', 'a+') as log:
                    log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
                    log.write("\n type error: " + str(e))
                    log.write('\n Title: {} \n Comment Name: {} \n'.submission.title, comment.name)
                    log.write('--------------------------- \n')

    with open('scrape_progress.log', 'a+') as log:
        log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        log.write('\n-------------------END----------------\n')



if __name__ == "__main__":

    dynamodb = boto3.resource('dynamodb')
    post_table = dynamodb.Table('posts')
    comment_table = dynamodb.Table('comments')

    home = str(Path.home())
    conf = yaml.safe_load(open(home+'/keys/api_keys.yml'))
    clientid = conf['reddit']['client_id']
    clientsecret = conf['reddit']['client_secret']


    reddit = praw.Reddit(client_id=clientid,
                        client_secret=clientsecret,
                     user_agent='DamitDan01')


    subs = [ 'confession', 'AmItheAsshole', 'tifu', 'pettyrevenge', 'ProRevenge', 'AskReddit']

    for sub in subs:
        scrape_sub(sub, reddit, post_table, comment_table)