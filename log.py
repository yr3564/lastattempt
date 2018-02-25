import psycopg2


def question1():
    conn = psycopg2.connect("dbname=news")
    print("")
    print("What are the most popular three articles of all time?")
    print("")
    print("")
    cursor = conn.cursor()
    cursor.execute("select articles.title, \
    count (substring(path from 10 for 45))\
    as views from articles join log on articles.slug = substring\
    (path from 10 for 45)\
    group by articles.title order by views desc limit 3;")
    results = cursor.fetchall()
    for result in results:
        print result[0], ' --    ',   result[1], '    Views'
        conn.close()
    return question1


def question2():
    conn = psycopg2.connect("dbname=news")
    print("")
    print("")
    print("Who are the most popular article \
        authors of all time?")
    print("")
    print("")
    cursor = conn.cursor()
    cursor.execute("select authors.name, count (substring \
    (path from 10 for 45)) as views from articles join log on \
    articles.slug = substring (path from 10 for 45) \
    left join authors\
    on authors.id=articles.author group by articles.author, \
    authors.name order by views desc limit 3;")
    results = cursor.fetchall()
    for result in results:
        print result[0], ' --    ', result[1], '    Views'
        conn.close()
    return question2


def creatview1():
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    cursor.execute("create view t_errors as \
        select date (time), count (log.status)\
    as errors from log where log.status = '404 NOT FOUND'\
    group by date (time) order by errors desc;")
    conn.commit()
    conn.close()
    return creatview1


def createview2():
    conn = psycopg2.connect("dbname=news")

    cursor = conn.cursor()
    cursor.execute("create view t_requests as \
        select date(time), count(log.status)\
    as requests from log group by date(time) \
    order by requests desc;")
    conn.commit()
    conn.close()
    return createview2

def question3():
    conn = psycopg2.connect("dbname=news")


    print("")
    print("")
    print("On which days did more than '1%' \
        of requests lead to errors?")
    print("")
    print("")
    cursor = conn.cursor()
    cursor.execute("select t_requests.date,\
    (cast(t_errors.errors as float)) / \
    (cast(t_requests.requests as float)) * 100\
    as percentage from t_requests join \
    t_errors on t_errors.date = t_requests.date\
    group by t_requests.date, t_errors.errors,\
    t_requests.requests order by percentage desc limit 1;")
    results = cursor.fetchall()
    for result in results:
        print result[0], ' --  ', round(result[1], 2), ' %'
        print("")
        conn.close()
    return question3

question1()


question2()


creatview1()


createview2()


question3()
