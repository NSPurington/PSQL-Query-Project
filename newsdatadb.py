#!/usr/bin/python2.7.x

# "Database code" for the DB news.

import psycopg2

dbname = "news"

def connect(database_name):
    """Connect to the PostgreSQL database.  Returns a database connection."""
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        c = db.cursor()
        return db, c
    except psycopg2.Error as e:
        print "Unable to connect to database"
        # THEN perhaps exit the program
        sys.exit(1) # The easier method
        # OR perhaps throw an error
        raise e
        # If you choose to raise an exception,
        # It will need to be caught by the whoever called this function

def top_articles():
  """Return the three most popular articles in the database, sorted by number of ip address hits."""
  db, c = connect(dbname)
  c.execute ("select articles.title, count(log.path) from articles, log where log.path like '%'||articles.slug group by articles.title order by count desc limit 3")
  rows = c.fetchall()
  for t in rows:
    print t[0], "-", t[1], 'views'
  return " "
  db.close()
  
def top_authors():
  """Return the most popular authors in the database, sorted by number of ip address hits."""
  db, c = connect(dbname)
  c.execute ("select name, count from author_count, authors where author_count.author = authors.id group by authors.name, author_count.count order by count desc")
  rows = c.fetchall()
  for t in rows:
    print t[0], "-", t[1], 'views'
  return " "
  db.close()   
             
def error_ratio():
  """Return how many days had more than %1 of total ID address hits as errors."""
  db, c = connect(dbname)
  c.execute ("select * from rounded_percentages where percent > 1")
  rows = c.fetchall()
  for t in rows:
    print t[0], "-", t[2], "%", 'errors'
  return " "
  db.close()   


""" Function calls """

if __name__ == '__main__':
    # code goes here

  print "What are the most popular three articles of all time?"
  print ""
  print top_articles()

  print "Who are the most popular article authors of all time?"
  print ""
  print top_authors()

  print "On which days did more than 1% of requests lead to errors?"
  print ""
  print error_ratio()
