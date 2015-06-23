;;deprecated
create table if not exists news
(
ID serial primary key,
user_id integer,
news_id integer,
visit_time time without time zone,
visit_timestamp timestamp without time zone,
post_time time without time zone,
post_timestamp timestamp without time zone,
news_title varchar(100),
news_content text
);

;;总的数据表
create table if not exists news
(
  ID serial primary key,
  user_id integer,
  news_id integer,
  visit_timestamp timestamp without time zone,
  post_timestamp timestamp without time zone,
  news_title varchar(100),
  news_content text
);

;;存储点击时间间隔一天以上的user_id news_id对
create table if not exists oneday
(
  ID serial primary key,
  user_id integer,
  news_id integer
);

;;存储每一条新闻记录浏览时间和发布时间之间的时间间隔以天为单位
create table if not exists interval
(
  ID serial primary key,
  user_id integer,
  news_id integer,
  interval integer
);

;;为发布时间为NULL的新闻存储最早的浏览日期作为其发布日期的近似日期
create table if not exists oldestvisit
(
  ID serial primary key,
  news_id integer,
  visit_timestamp timestamp without time zone
);

;;为存在浏览时间在发布时间之前的噪声记录的新闻做处理，将这个新闻的发布时间也处理为最早的浏览时间
create table if not exists oldestvisit2
(
  ID serial primary key,
  news_id integer,
  visit_timestamp timestamp without time zone
);

;;存储训练用的正例，也就是所有用户倒数点击的第二条新闻记录
create table if not exists trainpositive
(
  ID serial primary key,
  user_id integer,
  news_id integer,
  visit_timestamp timestamp without time zone,
  post_timestamp timestamp without time zone,
  news_title varchar(100),
  news_content text
);

;;存储训练用的负例，也就是10000*6183个user-news组合减去已有的116225条记录，剩下的所有还没有发生过浏览行为的user-news组合
create table if not exists trainnegative
(
  ID serial primary key,
  user_id integer,
  news_id integer,
  post_timestamp timestamp without time zone,
  news_title varchar(100),
  news_content text
);

;;存储新闻内容中包含的城市名称，以name1-name2-name3这样的形式存在psql中，其中存的是城市的编号而不是真实的中文，映射关系可以在city.clj中查看
create table if not exists newscity
(
  ID serial primary key,
  news_id integer,
  cities varchar(100)
);
