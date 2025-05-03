import csv
import praw
import datetime
import json

reddit = praw.Reddit(
    client_id="rUNOl6AdYl-TzPdQUhb7xw",
    client_secret="4qv0uThCVUVTUmf3QLVbB0FoQTHWQQ",
    user_agent="script:reddit_scraper:v1.0 (by u/TahaRazzaq)",
    username="TahaRazzaq",
    password="taha123lums"
)
print(f"Authenticated as: {reddit.user.me()}")

### Part 1. Read Operation (Extract)
def fetch_reddit_data(keyword, limit=100):
    """
    Fetches Reddit posts containing a specific keyword from a given subreddit.
    """
    posts = []
    subreddit = reddit.subreddit(keyword)
    
    for post in subreddit.search('remote work', limit=limit):
        posts.append({
            "title": post.title,
            "text": post.selftext,
            "author": str(post.author),
            "date": datetime.date.fromtimestamp(post.created),
            "upvotes": post.score,  
            "subreddit": post.subreddit.display_name
        })


    raw_file = open('raw/reddit_posts.json', "w")
    json.dump(posts, raw_file, indent = 4, default=str)
    return posts

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    print(data, len(data))
    with open(filename, 'w+', newline='',encoding="utf-8") as csvfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(data)):
            writer.writerow(data[i])
        return None

if __name__ == "__main__":
    print("True")
    posts = fetch_reddit_data('remotework', 200)
    if posts:
        save_to_csv(posts, "./datasets/reddit_posts.csv")
        print("Reddit data saved to reddit_posts.csv")
        
