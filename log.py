import psycopg2
import psycopg2

def question1():
	conn = psycopg2.connect("dbname=news")
	print ("connected")
	cursor = conn.cursor()
	cursor.execute ("select articles.title, count (substring(path from 10 for 45))\
        as views from articles join log on articles.slug = substring (path from 10 for 45)\
        group by articles.title order by views desc limit 3;")
	results = cursor.fetchall()
	print (results)
	conn.close()
	return question1

def question2():
	conn = psycopg2.connect("dbname=news")
	cursor = conn.cursor()
	cursor.execute ("select authors.name, count (substring \
	(path from 10 for 45)) as views from articles join log on \
	articles.slug = substring (path from 10 for 45) left join authors\
	on authors.id=articles.author group by articles.author, \
	authors.name order by views desc limit 3;")
	results = cursor.fetchall()
	print (results)
	conn.close()
	return question2

def creatview1 ():
	conn = psycopg2.connect("dbname=news")
	cursor = conn.cursor()
	cursor.execute ("create view t_errors as select date(time), count(log.status)\
	 as errors from log where log.status = '404 NOT FOUND'\
	 group by date(time) order by errors desc;")
	conn.close()
	return creatview1

def createview2 ():
	conn = psycopg2.connect("dbname=news")
	cursor = conn.cursor()
	cursor.execute ("create view t_requests as select date(time), count(log.status)\
	as requests from log group by date(time) order by requests desc;")
	conn.close()
	return createview2

def question3 ():
	conn = psycopg2.connect("dbname=news")
	cursor = conn.cursor()
	cursor.execute ("select t_requests.date,\
	(cast(t_errors.errors as float)) / (cast(t_requests.requests as float)) * 100\
	as percentage from t_requests join t_errors on t_errors.date = t_requests.date\
	group by t_requests.date, t_errors.errors, t_requests.requests order by percentage desc;")
	results = cursor.fetchall()
	print (results)
	conn.close()
	return question3	





