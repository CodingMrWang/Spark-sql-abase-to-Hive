# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf
import redis


from datetime import datetime,timedelta
from pyspark.sql.functions import udf
import sys


conf = SparkConf()
# conf.set("spark.executor.instances", "100")
conf.set("spark.driver.memory", "6g")
conf.set("spark.executor.memory", "20g")
conf.set("spark.python.worker.memory", "3g")
conf.set("spark.dynamicAllocation.enabled", "true")
conf.set("spark.dynamicAllocation.minExecutors", "10")
conf.set("spark.dynamicAllocation.initialExecutors", "100")
conf.set("spark.dynamicAllocation.maxExecutors", "150")
conf.set("spark.dynamicAllocation.executorIdleTimeout", "360s")
conf.set("spark.yarn.queue", "root.growth.stats")
conf.set("spark.app.name", "jiangchun_get_springdb")


spark = (SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel('ERROR')


def execute(p_date):
    datetime_object = datetime.strptime(p_date, '%Y-%m-%d')
     yesterday_object = datetime_object - timedelta(days=1)
    p_date_1_day_ago =  yesterday_object.strftime('%Y-%m-%d')

    video_link_sql = '''
    select
    *
    FROM
    (
      select
        link_id,
        item_id,
        meta_group_id,
        CASE WHEN fetch_time > "2010-01-01" THEN fetch_time ELSE from_unixtime(cast(fetch_time as bigint)) END as fetch_time
      from crawl_video_meta_link_dict
      WHERE date="{data}"
    )
    WHERE substr(fetch_time,0,10)="{p_date}"
    '''.format(date=date,p_date=p_date)

    video_link=spark.sql(video_link_sql)

    def get_link_value(iterator):
        from pyutil.springdb import SpringDBClient
        from pyspark.sql import Row
        SpringDBClient.set_zone("online")
        db = SpringDBClient('springdb_crawl', 'crawl_link')

        for row in iterator:
            json_str= ''
            try:
                json_str = db.get('link_item/'+row.link_id)

            except redis.RedisError as e:
                print ('redis failed')

            yield Row(link_id=row.link_id,item_id=row.item_id,meta_group_id=row.meta_group_id,fetch_time=row.fetch_time,info=json_str)


    a = video_link.rdd
    a.repartition(100)
    a = a.mapPartitions(get_link_value)
    a = a.toDF()

    a.createOrReplaceTempView('video_info')


    insert_sql='''
    INSERT OVERWRITE TABLE da.m_video_info_detail PARTITION(p_date="{p_date}")
    SELECT
      link_id,
      item_id,
      meta_group_id,
      fetch_time,
      info,
      get_json_object(info,'$.extra.author_pid') AS author_pid,
      get_json_object(info,'$.extra.album_type') AS album_type,
      get_json_object(info,'$.extra.created_time') AS created_time,
      get_json_object(info,'$.extra.video_copyright') AS video_copyright,
      get_json_object(info,'$.extra.duration') AS duration,
      get_json_object(info,'$.extra.favorite_count') AS favorite_count,
      get_json_object(info,'$.extra.bury_count') AS bury_count,
      get_json_object(info,'$.extra.area') AS area,
      get_json_object(info,'$.extra.play_url') AS play_url,
      get_json_object(info,'$.extra.tags') AS tags,
      get_json_object(info,'$.extra.comment_count') AS comment_count,
      get_json_object(info,'$.extra.video_category') AS video_category,
      get_json_object(info,'$.extra.play_status') AS play_status,
      get_json_object(info,'$.extra.video_type') AS video_type,
      get_json_object(info,'$.extra.play_count') AS play_count,
      get_json_object(info,'$.extra.video_cover_info') AS video_cover_info,
      get_json_object(info,'$.extra.director') AS director,
      get_json_object(info,'$.extra.release_date') AS release_date,
      get_json_object(info,'$.extra.author_name') AS author,
      get_json_object(info,'$.extra.digg_count') AS digg_count,
      get_json_object(info,'$.url') AS url,
      get_json_object(info,'$.title') AS title,
      get_json_object(info,'$.source') AS source
    FROM
      video_info
    UNION ALL
    select
        link_id,
        item_id,
        meta_group_id,
        fetch_time,
        info,
        author_pid,
        album_type,
        created_time,
        video_copyright,
        duration,
        favorite_count,
        bury_count,
        area,
        play_url,
        tags,
        comment_count,
        video_category,
        play_status,
        video_type,
        play_count,
        video_cover_info,
        director,
        release_date,
        author,
        digg_count,
        url,
        title,
        source
    from
    da.m_video_info_detail where p_date="{yesterday}"
    '''.format(p_date=p_date,yesterday=p_date_1_day_ago)

    spark.sql(insert_sql)


if __name__ == '__main__':
    if len(sys.argv) < 2:
     sys.stderr.write('Usage: python ' + sys.argv[0] + "p_date\n" +
     "if any other question, ask jiangchun.\n\n")
     sys.exit(1)

    p_date = sys.argv[1]
    execute(p_date)
