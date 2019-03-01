# 数据仓库课程设计文档

##### 1552761 韩乐桐  1652677 吴桐欣

------




[TOC]

## 1. 项目目的

​	利用从amazon获取的电影详细信息数据，针对电影及其周边信息，参考数据使用场景，建立多种存储模型，设计其存储架构并进行系统性能比对。



## 2. 存储模型设计与选择

### 2.1 星型模型

#### 选择理由

##### 星型模型

​	星型模型由一个事实表和一组维度表组成。每个维度表都有一个主键，所有这些维的主键组合成事实表的主键。强调的是对维度进行预处理，将多个维度集合到一个事实表，形成一个宽表。大宽表包含了维度表的主键和一些度量信息，而维度表则是事实表里面维度的具体信息，使用时候一般通过join来组合数据。

##### 雪花模型

​	雪花模型是对星型模型的扩展。它对星型模型的维表进一步层次化，原有的各维表可能被扩展为小的事实表，形成一些局部的 "层次 " 区域，这些被分解的表都连接到主维度表而不是事实表。雪花模型更加符合数据库范式，减少数据冗余，但是在分析数据的时候，需要join的表比较多，可能导致低性能，此外后期维护也比较复杂。

##### 设计过程

​	数据仓库一般都都使用星型模型或者雪花模型。比对了星星模型和雪花模型的特点后，由于我们的数据量较少、不太复杂，我们认为雪花模型中对维度的细分对我们不适用，甚至可能join太多而导致低性能，因此我们选择用星型模型作为我们数据库设计的基础。

​	我们将电影作为事实表，分别设计时间、演员、导演、工作室、类型、演员-导演合作、演员-演员合作共7个维度表。

​	但是，电影与其他维度是多对多的关系，如果要在维度表上进行一些有用的查询，势必导致电影数据的冗余。我们仅将电影名称和电影id加入维度表中。为了数据处理更方便也更灵活，我们采用pre-join的方式来存储冗余的电影名称和电影id，而不是将一串电影名称拼接起来作为维度表中的一个字段。

​	经过更改后的"星型模型"的优势如下：

- 在不同维度上的查询分别在不同的表中进行，减轻单表，尤其是movie表的压力
- 对于跨维度的一些复杂查询，在只涉及电影名称、不涉及电影详情的情况下，不需要join电影表，只需要将两个维度表join起来

#### 存储模型

![image-20181230225843867](/Users/mustafa/Library/Application Support/typora-user-images/image-20181230225843867.png)

##### 索引

- 所有的id字段
- 所有的名称name字段
- 时间的字段：年、月、日
- 可能作为查询条件的字段：评分、热度



### 2.2 3NF模型

#### 选择理由

​	在考虑对我们设计的"星型模型"进行查询实验时，我们意识到缺少一个对照组。尽管我们已经有了一个基于图数据库的存储模型，但因为与一般的关系型数据库差别较大，难以体现存储模型对查询效率的优化。因此我们增加了一个符合第三范式的模型，与"星型模型"使用相同的数据库，以控制变量地比较二者的性能。

#### 存储模型

![3nf-mysql](/Users/mustafa/Desktop/a8 数据仓库_朱宏明/期末项目/3nf-mysql.png)

##### 索引

- 所有的id字段
- 所有的名称name字段
- 时间的字段：年、月、日
- 可能作为查询条件的字段：评分、热度



### 2.3 图数据库

#### 选择理由

​	在传统的关系型数据库中，通常需要一个关系表来存储实体之间的关系，而图数据库可以很高效的查询关联数据，同时在数据的表现上也更加直观和自然。在我们的项目中，电影与多个实体（时间、演员、导演、工作室……）有联系，而其他实体之间也会有联系，如导演与演员的合作关系。因此，设计一个图数据库或许能很好地进行一些关系方面的查询。

#### 存储模型

​	值得一提的是，在图数据库中，我们把时间的"年""月""日"拆成三类节点，从而提高在时间查询上的效率。

##### 节点设计

| 节点标签:LABEL | 属性                                        | 主键  |
| -------------- | ------------------------------------------- | ----- |
| movie          | id, name, review, star, ver_count, duration | id    |
| actor          | id, name                                    | id    |
| director       | id, name                                    | id    |
| genre          | id, name                                    | id    |
| studio         | id, name                                    | id    |
| year           | id, value                                   | value |
| month          | id, value                                   | value |
| day            | id, value                                   | value |
| day_of_week    | id, value                                   | value |

##### 边设计

| 边类型:TYPE       | 起始节点 | 终止节点    | 属性                             |
| ----------------- | -------- | ----------- | -------------------------------- |
| cooperate_with    | actor    | actor       | list[movie_id], list[movie_name] |
| work_with         | director | actor       | list[movie_id], list[movie_name] |
| moive_genre       | movie    | genre       |                                  |
| movie_studio      | movie    | studio      |                                  |
| movie_actor       | movie    | actor       |                                  |
| movie_director    | movie    | director    |                                  |
| movie_year        | movie    | year        |                                  |
| movie_month       | movie    | month       |                                  |
| movie_day         | movie    | day         |                                  |
| movie_day_of_week | movie    | day_of_week |                                  |

##### 索引

- 所有节点的主要属性：name、value
- 可能作为查询条件的属性：评分、热度



## 3. 查询场景设计

​	为了全面地比较不同存储模型的查询性能，我们设计了由简单到复杂的大致3类查询：

- 简单查询：单个维度上不涉及电影细节的查询（含以下2种）
  - 限定维度的属性（如：某个演员参演过哪些电影）
  - 进行聚合查询并排序（如：参演最多电影的前100位演员）
- 组合查询：单个维度结合电影细节的查询（含以下2种）
  - 限度维度的属性查询电影细节（如：某个导演导过的所有电影）
  - 依据电影细节进行聚合查询并排序（如：依据电影平均热度对月份排序）
- 复杂查询：两个维度结合电影细节的查询

​	我们用下表记录查询涉及的维度，保证查询的全面性：

| 分类      | 电影 | 演员 | 导演 | 时间 | 类型 | 工作室 | 演员-演员 | 导演-演员 |
| --------- | ---- | ---- | ---- | ---- | ---- | ------ | --------- | --------- |
| 电影      | √    | √    | √    | √    | √    | √      | √         | √         |
| 演员      |      | √    |      |      | √    |        |           |           |
| 导演      |      |      | √    |      |      |        |           |           |
| 时间      |      |      |      | √    | √    |        |           |           |
| 类型      |      |      |      |      | √    |        |           |           |
| 工作室    |      |      |      |      |      | √      |           |           |
| 演员-演员 |      |      |      |      |      |        | √         |           |
| 导演-演员 |      |      |      |      |      |        |           | √         |



## 4. 查询效率对比结果

### 4.1 简单查询

#### 4.1.1 电影查询

**查询内容**

- 电影热度随评分的变化关系

**查询语句**

- 星型mysql

```mysql
select star, avg(review) from movie group by star order by star desc;
```

- 3NFmysql

```sql
select star, avg(review) from movie group by star order by star desc;
```

- neo4j

```CQL
MATCH (m:movie) RETURN m.star,avg(m.review) order by m.star DESC
```

- mongoDB

```python
pipeline = [
        {"$unwind": "$star"},
        {"$group": {"_id": "$star", "avg_review": {"$avg": "$Comments"}}},
        {"$sort": SON([("_id", -1), ("avg_review", -1)])}
]
col.create_index('star')
result = col.aggregate(pipeline)
# pprint.pprint(list(result))
```

**查询结果**

![image-20181228195330653](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228195330653.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 9.500030  | 0.09500030 |
| 3NFmysql  | 100  | 10.687963 | 0.10687963 |
| neo4j     | 100  | 9.219226  | 0.09219226 |
| mongoDB   | 100  | 16.420340 | 0.16420340 |



#### 4.1.2 时间查询（1）

**查询内容**

- 星期几电影数量最多

**查询语句**

- 星型mysql

```mysql
select day_of_week, count(1) c from time group by day_of_week order by c desc
```

- 3NFmysql

```sql
select day_of_week, count(1) c from movie group by day_of_week order by c desc;
```

- neo4j

```cql
MATCH (w:day_of_week)-[r:movie_day_of_week]-() RETURN w,count(r) order by count(r) DESC
```

- mongoDB

```python
pipeline = [
        {"$unwind": "$releaseTime.day_of_week"},
        {"$group": {"_id": "$releaseTime.day_of_week", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1)])}
]
col.create_index('releaseTime.day_of_week')
result = col.aggregate(pipeline)
# pprint.pprint(list(result))
```

**查询结果**

![image-20181224144115201](/Users/mustafa/Library/Application Support/typora-user-images/image-20181224144115201.png)

**查询时间**

| 数据库    | 次数 | 时间        | 平均/次     |
| :-------- | ---- | ----------- | ----------- |
| 星型mysql | 1000 | 3.991120    | 0.003991120 |
| 3NFmysql  | 1000 | 19.691369   | 0.019691369 |
| neo4j     | 1000 | 3.563363    | 0.003563363 |
| mongoDB   | 1000 | 2:21.225708 | 0.141225708 |



#### 4.1.3 时间查询（2）

**查询内容**

- 1999年有哪些电影

**查询语句**

- 星型mysql

```mysql
select year, movie_name from time where year=1999;
```

- 3NFmysql

```sql
select year, name from movie where year=1999;
```

- neo4j

```CQL
MATCH (y:year{value:'1999'})-[:movie_year]-(m:movie) RETURN y.value,m.name
```

- mongoDB

```python
col.create_index('releaseTime')
results = col.find({"releaseTime.year": 1999})
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228220423349](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228220423349.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 1.452964  | 0.01452964 |
| 3NFmysql  | 100  | 1.893186  | 0.01893186 |
| neo4j     | 100  | 2.380420  | 0.02380420 |
| mongoDB   | 100  | 11.876764 | 0.11876764 |



#### 4.1.4 导演查询（1）

**查询内容**

- 根据名字查询导过的电影

**查询语句**

- 星型mysql

```mysql
select name, movie_name from director where name='Jim Edward';
```

- 3NFmysql

```sql
select s_director.name, movie.name from s_movie_director,s_director,movie where s_director.id=director_id and movie.id=movie_id and s_director.name='Jim Edward';
```

- neo4j

```CQL
MATCH (d:director{name:'Jim Edward'})-[:movie_director]-(m:movie) RETURN m.name
```

- mongoDB

```python
col.create_index('directors')
results = col.find({'directors': 'Jim Edward'})
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228203120166](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228203120166.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次     |
| :-------- | ---- | -------- | ----------- |
| 星型mysql | 1000 | 0.336963 | 0.000336963 |
| 3NFmysql  | 1000 | 0.518493 | 0.000518493 |
| neo4j     | 1000 | 1.990857 | 0.001990857 |
| mongoDB   | 1000 | 1.003830 | 0.001003830 |



#### 4.1.5 导演查询（2）

**查询内容**

- 找出导演电影最多的（前100）

**查询语句**

- 星型mysql

```mysql
select name, count(1) c from director group by name order by c desc limit 100;
```

- 3NFmysql

```sql
select s_director.name,count(1) from s_movie_director,s_director where s_director.id=director_id group by s_director.name order by count(1) desc limit 100;
```

- neo4j

```CQL
MATCH (d:director)-[r:movie_director]-() RETURN d.name,count(r) order by count(r) DESC limit 100
```

**查询结果**

![image-20181224154223086](/Users/mustafa/Library/Application Support/typora-user-images/image-20181224154223086.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 3.367825  | 0.03367825 |
| 3NFmysql  | 100  | 17.607243 | 0.17607243 |
| neo4j     | 100  | 11.085453 | 0.11085453 |



#### 4.1.6 演员查询（1）

**查询内容**

- 根据演员名查询演过的电影

**查询语句**

- 星型mysql

```mysql
select name, movie_name from actor where name='Wendy Braun';
```

- 3NFmysql

```sql
select s_actor.name, movie.name from s_movie_actor,s_actor,movie where s_actor.id=actor_id and movie.id=movie_id and s_actor.name='Wendy Braun';
```

- neo4j

```CQL
MATCH (a:actor{name:'Wendy Braun'})-[:movie_actor]-(m:movie) RETURN m.name
```

- mongoDB

```python
col.create_index('actors')
results = col.find({'actors': 'Wendy Braun'})
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228202946738](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228202946738.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次     |
| :-------- | ---- | -------- | ----------- |
| 星型mysql | 1000 | 0.253710 | 0.000253710 |
| 3NFmysql  | 1000 | 0.454516 | 0.000454516 |
| neo4j     | 1000 | 1.674080 | 0.001674080 |
| mongoDB   | 1000 | 0.777349 | 0.000777349 |



#### 4.1.7 演员查询（2）

**查询内容**

- 找出参演最多电影的演员（前100）

**查询语句**

- 星型mysql

```mysql
select name, count(1) c from actor group by name order by c desc limit 100;
```

- 3NFmysql

```sql
select s_actor.name, count(1) from s_movie_actor,s_actor where s_actor.id=actor_id group by s_actor.name order by count(1) desc limit 100;
```

- neo4j

```CQL
MATCH (a:actor)-[r:movie_actor]-() RETURN a.name,count(r) order by count(r) DESC limit 100
```

**查询结果**

![image-20181228203239310](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228203239310.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 10.138048 | 0.10138048 |
| 3NFmysql  | 100  | 59.216854 | 0.59216854 |
| neo4j     | 100  | 44.736898 | 0.44736898 |



#### 4.1.8 类型查询（1）

**查询内容**

- 某个类型的全部电影

**查询语句**

- 星型mysql

```mysql
select name, movie_name from genre where name='Action';
```

- 3NFmysql

```sql
select s_genre.name, movie.name from s_movie_genre,s_genre,movie where s_genre.id=genre_id and movie.id=movie_id and s_genre.name='Action';
```

- neo4j

```CQL
MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie) RETURN m.name
```

- mongoDB

```python
col.create_index('genres')
results = col.find({'genres': 'Action'})
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228203530750](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228203530750.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 1.427083  | 0.01427083 |
| 3NFmysql  | 100  | 1.893451  | 0.01893451 |
| neo4j     | 100  | 2.128092  | 0.02128092 |
| mongoDB   | 100  | 32.001234 | 0.32001234 |



#### 4.1.9 类型查询（2）

**查询内容**

- 各个类型的电影数量

**查询语句**

- 星型mysql

```mysql
select name, count(*) c from genre group by name order by c desc;
```

- 3NFmysql

```sql
select s_genre.name, count(1) from s_movie_genre,s_genre where s_genre.id=genre_id group by s_genre.name order by count(1) desc;
```

- neo4j

```CQL
MATCH (g:genre)-[r:movie_genre]-() RETURN g.name,count(r) order by count(r) DESC
```

**查询结果**

![image-20181224160859948](/Users/mustafa/Library/Application Support/typora-user-images/image-20181224160859948.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次     |
| :-------- | ---- | --------- | ----------- |
| 星型mysql | 1000 | 5.280648  | 0.005280648 |
| 3NFmysql  | 1000 | 10.142987 | 0.010142987 |
| neo4j     | 1000 | 13.842696 | 0.013842696 |



#### 4.1.10 工作室查询（1）

**查询内容**

- 某个工作室的全部电影

**查询语句**

- 星型mysql

```mysql
select name, movie_name from studio where name='CreateSpace';
```

- 3NFmysql

```sql
select s_studio.name, movie.name from s_movie_studio,s_studio,movie where s_studio.id=studio_id and movie.id=movie_id and s_studio.name='CreateSpace';
```

- neo4j

```CQL
MATCH (s:studio{name:'CreateSpace'})-[:movie_studio]-(m:movie) RETURN m.name
```

- mongoDB

```python
col.create_index('studio')
results = col.find({'studio': 'CreateSpace'})
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228204005782](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228204005782.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次    |
| :-------- | ---- | -------- | ---------- |
| 星型mysql | 100  | 0.729129 | 0.00729129 |
| 3NFmysql  | 100  | 0.914735 | 0.00914735 |
| neo4j     | 100  | 0.970038 | 0.00970038 |
| mongoDB   | 100  | 1.375395 | 0.01375395 |



#### 4.1.11 工作室查询（2）

**查询内容**

- 电影数量多的工作室（前100）

**查询语句**

- 星型mysql

```mysql
select name, count(1) c from studio group by name order by c desc limit 100;
```

- 3NFmysql

```sql
select s_studio.name, count(1) from s_movie_studio,s_studio where s_studio.id=studio_id group by s_studio.name order by count(1) desc limit 100;
```

- neo4j

```CQL
MATCH (s:studio)-[r:movie_studio]-() RETURN s.name,count(r) order by count(r) DESC limit 100
```

**查询结果**

![image-20181224161437706](/Users/mustafa/Library/Application Support/typora-user-images/image-20181224161437706.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次    |
| :-------- | ---- | -------- | ---------- |
| 星型mysql | 100  | 2.727416 | 0.02727416 |
| 3NFmysql  | 100  | 5.910072 | 0.05910072 |
| neo4j     | 100  | 5.566594 | 0.05566594 |



#### 4.1.12 演员合作查询

**查询内容**

- 共同参演次数最多的演员（前100）

**查询语句**

- 星型mysql

```mysql
select actor_name1, actor_name2, count(1) c from cooperate_with group by actor_name1, actor_name2 order by c desc limit 100;
```

- 3NFmysql

```sql
select a.name,b.name,count(1) c from s_movie_actor ma,s_movie_actor mb,s_actor a,s_actor b where ma.movie_id=mb.movie_id and ma.actor_id<>mb.actor_id and ma.actor_id=a.id and mb.actor_id=b.id group by a.name,b.name order by c desc limit 100;
```

- neo4j

```CQL
MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor) RETURN a.name,b.name,count(m) order by count(m) DESC limit 100
```

- neo4j（优化）

```CQL
MATCH (a)-[r:cooperate_with]->(b) RETURN a.name,b.name,length(r.movie_id) order by length(r.movie_id) DESC limit 100
```

**查询结果**

![image-20181224181733776](/Users/mustafa/Library/Application%20Support/typora-user-images/image-20181224181733776.png)

**查询时间**

| 数据库            | 次数 | 时间      | 平均/次   |
| :---------------- | ---- | --------- | --------- |
| 星型mysql         | 10   | 5.976000  | 0.5976000 |
| 3NFmysql          | 10   | 30.664697 | 3.0664697 |
| neo4j             | 10   | 28.954396 | 2.8954396 |
| neo4j（优化查询） | 10   | 11.691526 | 1.1691526 |



#### 4.1.13 演员导演合作查询

**查询内容**

- 合作过次数最多的演员和导演（前100）

**查询语句**

- 星型mysql

```mysql
select actor_name, director_name, count(1) c from work_with group by actor_name, director_name order by c desc limit 100;
```

- 3NFmysql

```sql
select d.name,a.name,count(1) c from s_movie_director md,s_movie_actor ma,s_director d,s_actor a where md.movie_id=ma.movie_id and ma.actor_id=a.id and md.director_id=d.id group by d.name,a.name order by c desc limit 100;
```

- neo4j

```CQL
MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_director]-(d:director) RETURN a.name,d.name,count(m) order by count(m) DESC limit 100
```

- neo4j（优化）

```CQL
MATCH (a:actor)-[r:work_with]-(d:director) RETURN a.name,d.name,length(r.movie_id) order by length(r.movie_id) DESC limit 100
```

**查询结果**

![image-20181224184222901](/Users/mustafa/Library/Application%20Support/typora-user-images/image-20181224184222901.png)

**查询时间**

| 数据库            | 次数 | 时间      | 平均/次   |
| :---------------- | ---- | --------- | --------- |
| 星型mysql         | 10   | 4.211463  | 0.4211463 |
| 3NFmysql          | 10   | 12.452312 | 1.2452312 |
| neo4j             | 10   | 8.756141  | 0.8756141 |
| neo4j（优化查询） | 10   | 6.270429  | 0.6270429 |



### 4.2 组合查询

#### 4.2.1 组合查询（1）

**查询内容**

- 某个演员和某个导演合作过的电影及其评分、热度

**查询语句**

- 星型mysql

```sql
with movies as (select movie_id, movie_name from work_with where actor_name='Mel Blanc' and director_name='Friz Freleng') select movie.name, movie.star, movie.review from movie, movies where movie.id=movies.movie_id order by movie.star desc, movie.review desc;
```

- 3NFmysql

```sql
select m.name,m.star,m.review from s_actor a,s_director d,s_movie_actor ma,s_movie_director md,movie m where a.name='Mel Blanc' and d.name='Friz Freleng' and a.id=ma.actor_id and d.id=md.director_id and ma.movie_id=m.id and md.movie_id=m.id order by m.star desc,m.review desc;
```

- neo4j

```CQL
MATCH (d:director{name:'Friz Freleng'})-[:movie_director]-(m:movie)-[:movie_actor]-(a:actor{name:'Mel Blanc'}) RETURN m.name,m.star,m.review order by m.star DESC, m.review DESC
```

- mongoDB

```python
col.create_index('directors')
col.create_index('actors')
col.create_index('star')
results = col.find({'actors': 'Mel Blanc', 'directors': 'Friz Freleng'}).sort([('star', -1)])
for result in results:
    # print(result)
    pass
```

**查询结果**

![image-20181228210456158](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228210456158.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次     |
| :-------- | ---- | -------- | ----------- |
| 星型mysql | 1000 | 1.172879 | 0.001172879 |
| 3NFmysql  | 1000 | 1.548074 | 0.001548074 |
| neo4j     | 1000 | 3.779676 | 0.003779676 |
| mongoDB   | 1000 | 3.309400 | 0.003309400 |



#### 4.2.2 组合查询（2）

**查询内容**

- 某两个演员合作过的电影及其评分、热度

**查询语句**

- 星型mysql

```mysql
with movies as (select movie_id, movie_name from cooperate_with where actor_name1='Moe Howard' and actor_name2='Curly Howard') select movie_name, star, review from movies, movie where movies.movie_id=movie.id order by star desc, review desc;
```

- 3NFmysql

```sql
select m.name,m.star,m.review from s_actor a,s_actor d,s_movie_actor ma,s_movie_actor md,movie m where a.name='Moe Howard' and d.name='Curly Howard' and a.id=ma.actor_id and d.id=md.actor_id and ma.movie_id=m.id and md.movie_id=m.id order by m.star desc,m.review desc;
```

- neo4j

```CQL
MATCH (a:actor{name:'Moe Howard'})-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor{name:'Curly Howard'}) RETURN a.name,b.name,m.name,m.star order by m.star DESC
```

**查询结果**

![image-20181230115852162](/Users/mustafa/Library/Application Support/typora-user-images/image-20181230115852162.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次     |
| :-------- | ---- | -------- | ----------- |
| 星型mysql | 1000 | 1.385576 | 0.001385576 |
| 3NFmysql  | 1000 | 1.969998 | 0.001969998 |
| neo4j     | 1000 | 4.024001 | 0.004024001 |



#### 4.2.3 组合查询（3）

**查询内容**

- 按照所有导演导过电影的平均分(1)、平均热度(2)排序（前100）

**查询语句**

- 星型mysql

```mysql
select director.name, avg(star) avg_star, avg(review) avg_review from movie, director where movie.id=director.movie_id group by director.name order by avg_star desc, avg_review desc limit 100;
```

- 3NFmysql

```sql
select d.name,avg(m.star) avg_star,avg(m.review) avg_review from s_director d,s_movie_director md,movie m where md.director_id=d.id and md.movie_id=m.id group by d.name order by avg_star desc, avg_review desc limit 100;
```

- neo4j

```CQL
MATCH (m:movie)-[:movie_director]-(d:director) RETURN d.name,avg(m.star),avg(m.review) order by avg(m.star) desc, avg(m.review) desc limit 100
```

**查询结果**

![image-20181228212252655](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228212252655.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 16.468601 | 0.16468601 |
| 3NFmysql  | 100  | 23.924533 | 0.23924533 |
| neo4j     | 100  | 37.777395 | 0.37777395 |



#### 4.2.4 组合查询（4）

**查询内容**

- 某演员参演的电影详情（按评分排序）

**查询语句**

- 星型mysql

```mysql
select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, actor where actor.name='Morgan Freeman' and actor.movie_id=movie.id order by star desc;
```

- 3NFmysql

```sql
select m.name,m.rated,m.star,m.review,m.version_count,m.duration from movie m,s_movie_actor ma,s_actor a where a.name='Morgan Freeman' and ma.actor_id=a.id and ma.movie_id=m.id order by m.star desc;
```

- neo4j

```CQL
MATCH (:actor{name:'Morgan Freeman'})-[:movie_actor]-(m:movie) RETURN m order by m.star desc
```

- mongoDB

```python
results = col.find({'actors': 'Morgan Freeman'}).sort([('star', -1)])
col.create_index('actors')
col.create_index('star')
for result in results:
    # print(result['movieName'], result['actors'], result['star'])
    pass
```

**查询结果**

![image-20181228212827267](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228212827267.png)

**查询时间**

| 数据库    | 次数 | 时间        | 平均/次     |
| :-------- | ---- | ----------- | ----------- |
| 星型mysql | 1000 | 2.638816    | 0.002638816 |
| 3NFmysql  | 1000 | 3.766023    | 0.003766023 |
| neo4j     | 1000 | 15.748181   | 0.015748181 |
| mongoDB   | 1000 | 1:03.839539 | 0.063839539 |



#### 4.2.5 组合查询（5）

**查询内容**

- 哪个月份的电影热度最高

**查询语句**

- 星型mysql

```mysql
select month, avg(review) avg_review from movie, time where movie.id=time.movie_id group by month order by avg_review desc;
```

- 3NFmysql

```sql
select month,avg(review) avg_review from movie group by month order by avg_review desc;
```

- neo4j

```CQL
MATCH (m:movie)-[:movie_month]-(mo:month) RETURN mo.value,avg(m.review) order by avg(m.review) desc
```

- mongoDB

```python
pipeline = [
        {"$unwind": "$releaseTime.month"},
        {"$group": {"_id": "$releaseTime.month", "avg_review": {"$avg": "$Comments"}}},
        {"$sort": SON([("avg_review", -1)])}
]
col.create_index('releaseTime.month')
result = col.aggregate(pipeline)
# pprint.pprint(list(result))
```

**查询结果**

![image-20181228213129655](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228213129655.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 1.960383  | 0.01960383 |
| 3NFmysql  | 100  | 8.834271  | 0.08834271 |
| neo4j     | 100  | 2.321689  | 0.02321689 |
| mongoDB   | 100  | 15.010125 | 0.15010125 |



#### 4.2.6 组合查询（6）

**查询内容**

- 按照不同类型电影的平均分(1)、平均热度(2)排序

**查询语句**

- 星型mysql

```mysql
select genre.name type, avg(movie.star) avg_star, avg(movie.review) avg_review from movie, genre where genre.movie_id=movie.id group by genre.name order by avg_star desc, avg_review desc;
```

- 3NFmysql

```sql
select g.name,avg(m.star) avg_star,avg(m.review) avg_review from s_genre g,s_movie_genre mg,movie m where mg.genre_id=g.id and mg.movie_id=m.id group by g.name order by avg_star desc, avg_review desc;
```

- neo4j

```CQL
MATCH (m:movie)-[:movie_genre]-(g:genre) RETURN g.name,avg(m.star),avg(m.review) order by avg(m.star) desc,avg(m.review) desc
```

**查询结果**

![image-20181228213756027](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228213756027.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次    |
| :-------- | ---- | -------- | ---------- |
| 星型mysql | 100  | 2.553228 | 0.02553228 |
| 3NFmysql  | 100  | 3.057668 | 0.03057668 |
| neo4j     | 100  | 4.728441 | 0.04728441 |



#### 4.2.7 组合查询（7）

**查询内容**

- 哪个工作室的电影平均热度最高（前100）

**查询语句**

- 星型mysql

```mysql
select studio.name studio, avg(movie.review) avg_review from movie, studio where studio.movie_id = movie.id group by studio.name order by avg_review desc limit 100;
```

- 3NFmysql

```sql
select s.name,avg(m.review) avg_review from s_studio s,s_movie_studio ms,movie m where ms.studio_id=s.id and ms.movie_id=m.id group by s.name order by avg_review desc limit 100;
```

- neo4j

```CQL
MATCH (m:movie)-[:movie_studio]-(s:studio) RETURN s.name,avg(m.review) order by avg(m.review) desc limit 100
```

**查询结果**

![image-20181230133021678](/Users/mustafa/Library/Application Support/typora-user-images/image-20181230133021678.png)

**查询时间**

| 数据库    | 次数 | 时间      | 平均/次    |
| :-------- | ---- | --------- | ---------- |
| 星型mysql | 100  | 11.935224 | 0.11935224 |
| 3NFmysql  | 100  | 15.573076 | 0.15573076 |
| neo4j     | 100  | 21.730590 | 0.21730590 |



### 4.3 复杂查询

#### 4.3.1 复杂查询（1）

**查询内容**

- 某类型电影的上映时间

**查询语句**

- 星型mysql

```mysql
with movies as (select movie.id movie_id, movie.name movie_name, movie.review review, genre.name genre from movie, genre where movie.id=genre.movie_id and genre.name='Action' order by review desc limit 100) select movies.movie_name, movies.review, movies.genre, time.year, time.month, time.day from movies, time where time.movie_id=movies.movie_id;
```

- 3NFmysql

```sql
select m.name, g.name, m.review, m.year, m.month, m.day from movie m,s_genre g,s_movie_genre mg where g.name='Action' and mg.movie_id=m.id and mg.genre_id=g.id order by m.review desc limit 100; 
```

- neo4j

```CQL
MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie)-[:movie_year]-(y:year) RETURN m.name,m.review,y.value order by m.review DESC limit 100
```

- mongoDB

```python
col.create_index('genres')
results = col.find({'genres': 'Action'}).sort([('Comments', -1)])
for result in results:
    # print(result['movieName'], result['genres'], result['Comments'], result['releaseTime'])
    pass
```

**查询结果**

![image-20181225123754954](/Users/mustafa/Library/Application Support/typora-user-images/image-20181225123754954.png)

**查询时间**

| 数据库    | 次数 | 时间        | 平均/次     |
| :-------- | ---- | ----------- | ----------- |
| 星型mysql | 1000 | 4.425852    | 0.004425852 |
| 3NFmysql  | 1000 | 4.907729    | 0.004907729 |
| neo4j     | 1000 | 8.899081    | 0.008899081 |
| mongoDB   | 1000 | 9:14.495048 | 0.554495048 |



#### 4.3.2 复杂查询（2）

**查询内容**

- 某个演员演过各个类型电影的平均分

**查询语句**

- 星型mysql

```mysql
with movies as (select movie.id movie_id, movie.name movie_name, movie.star star from movie, actor where actor.name='Jackie Chan' and actor.movie_id=movie.id) select genre.name genre, avg(movies.star) avg_star from movies, genre where movies.movie_id=genre.movie_id group by genre.name order by avg_star desc;
```

- 3NFmysql

```sql
with movies as (select m.id m_id,m.star m_star from s_actor a,s_movie_actor ma,movie m where a.name='Jackie Chan' and ma.actor_id=a.id and ma.movie_id=m.id) select g.name,avg(movies.m_star) avg_star from movies,s_genre g,s_movie_genre mg where movies.m_id=mg.movie_id and mg.genre_id=g.id group by g.name order by avg_star desc;
```

- neo4j

```CQL
MATCH (a:actor{name:'Jackie Chan'})-[:movie_actor]-(m:movie)-[:movie_genre]-(g:genre) RETURN g.name,avg(m.star) order by avg(m.star) desc
```

**查询结果**

![image-20181228215112059](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228215112059.png)

**查询时间**

| 数据库    | 次数 | 时间     | 平均/次     |
| :-------- | ---- | -------- | ----------- |
| 星型mysql | 1000 | 0.843748 | 0.000843748 |
| 3NFmysql  | 1000 | 1.109290 | 0.001109290 |
| neo4j     | 1000 | 3.260573 | 0.003260573 |



#### 4.3.3 复杂查询（3）

**查询内容**

- 某工作室拍过的某个类型的电影详情

**查询语句**

- 星型mysql

```mysql
with movies as (select studio.movie_id from studio, genre where studio.name='20th Century Fox' and genre.name='Action' and studio.movie_id=genre.movie_id) select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, movies where movie.id=movies.movie_id order by movie.star desc;
```

- 3NFmysql

```sql
with movies as (select m.id,m.name,m.rated,m.star,m.review,m.version_count,m.duration from s_studio s,s_movie_studio ms,movie m where s.name='20th Century Fox' and ms.studio_id=s.id and ms.movie_id=m.id) select movies.name,movies.rated,movies.star,movies.review,movies.version_count,movies.duration from movies,s_genre g,s_movie_genre mg where g.name='Action' and mg.genre_id=g.id and movies.id=mg.movie_id order by movies.star desc;
```

- neo4j

```CQL
MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie)-[:movie_studio]-(s:studio{name:'20th Century Fox'}) RETURN m order by m.star desc
```

- mongoDB

```python
col.create_index('studio')
col.create_index('genres')
col.create_index('star')
results = col.find({'studio': '20th Century Fox'}, {'genres': 'Action'}).sort([('star', -1)])
for result in results:
    # print(result['movieName'], result['genres'], result['Comments'], result['releaseTime'])
    pass
```

**查询结果**

![image-20181228215900777](/Users/mustafa/Library/Application Support/typora-user-images/image-20181228215900777.png)

**查询时间**

| 数据库    | 次数 | 时间        | 平均/次     |
| :-------- | ---- | ----------- | ----------- |
| 星型mysql | 1000 | 4.726963    | 0.004726963 |
| 3NFmysql  | 1000 | 4.750355    | 0.004750355 |
| neo4j     | 1000 | 7.785154    | 0.007785154 |
| mongoDB   | 1000 | 3:45.256056 | 0.225256056 |



## 5. 分析与总结

### 5.1 各种查询折线图

#### 5.1.1 简单查询折线图

![简单查询效率折线图](/Users/mustafa/Desktop/简单查询效率折线图.png)

#### 5.1.2 组合查询折线图

![组合查询效率折线图](/Users/mustafa/Desktop/组合查询效率折线图.png)

#### 5.1.3 复杂查询折线图

![复杂查询效率折线图](/Users/mustafa/Desktop/复杂查询效率折线图.png)



### 5.2 neo4j查询优化

##### - 自身查询优化

| 查询优化                                                   | 平均提高的效率百分比 |
| ---------------------------------------------------------- | -------------------- |
| 将全部已知查询条件的节点作为查询的出发节点                 | [78.3%]()            |
| 将节点数较少的一端作为出发节点                             | [18.75%]()           |
| 当进行count查询时，不需要返回对端节点，只需要count边数即可 | [11.2%]()            |

##### - 优点

- **<u>时间节点</u>**出发的查询，速度较快（大约为星型模型的[90%]()、3NF模型的[26.3%]()）（因为在图数据库中，年、月、日分别单独存储为一类节点，直接通过label查询，效率极高）
- 根据边的查询，如合作次数最多的演员和导演，速度与3NF模型相比提高大约[45.1%]()
- 适合复杂查询（涉及到很多关系），可以很高效地查询关联数据，因为表连接时间开销大，涉及大量的IO操作及内存消耗，而图数据库会对关联查询一般进行针对性的优化，防止局部数据的查询引发全部数据的读取。

##### - 缺点

- 查询速度不稳定，自身差距高达[15%]()



### 5.3 mysql查询优化

##### - 增加索引

- 在两种mysql模型中，都分别针对不同类的查询涉及字段建立了索引，与未建索引相比，效率大约提高[85%]()
- 在 where 及 order by 涉及的列上建立索引，避免了全表扫描

##### - 优化查询语句

- 使用整型的movie_id而不用varchar类型的movie_name进行匹配，速度更快（速度提升[30%]()左右）
- 使用with语句，先筛选数据数量较少的两个表，再匹配结果（三个表join查询时间超过1min）
- 用到group by的查询语句效率会降低
- 3NF模型join数据量较大的表效率低



### 5.4 星型模型3NF模型查询效率对比

##### - 效率对比表

| 查询条件                       | 查询类型                         | 各个查询提高的效率百分比               | 平均值      |
| ------------------------------ | -------------------------------- | -------------------------------------- | ----------- |
| 单表查询，表结构类似           | 包含聚合、group by               | 11%                                    | [11%]()     |
| 单表查询，3NF数据量是星型的4倍 | 包含group by                     | 79.8%                                  | [79.8%]()   |
| 单表查询，3NF数据量是星型的4倍 | 指定查询条件                     | 23.3%                                  | [23.3%]()   |
| 星型单表，3NF双表查询          | 指定查询条件                     | 35.1%，44.2%，24.7%，20.3%             | [31.075%]() |
| 星型单表，3NF双表查询          | 包含聚合、group by               | 80.9%，82.9%，48%，53.9%，81.6%，66.2% | [68.917%]() |
| 星型双表，3NF多表查询          | 指定查询条件                     | 24.3%，29.7%，30.2%                    | [28.07%]()  |
| 星型双表，3NF多表查询          | 包含聚合、group by               | 31.2%，77.9%，16.5%，23.4%             | [37.25%]()  |
| 多表查询                       | 指定查询条件，包含聚合、group by | 9.9%，24%，0.5%                        | [11.47%]()  |

##### - 分析与总结

​	从上表中数据可以看出，当查询条件相似（同为单表或同为多表查询）时，优化的星型模型与传统的3NF模型的查询效率并没有太大差距，由于硬件等外部因素（两种模型的查询操作分别在不同机器上进行）的影响而产生了[11%]()左右的查询效率差距。

​	根据3NF的设计理念，多对多关系全部映射为联系集，而在我们优化设计的星型模型中，由于pre-join而存储了部分冗余数据，空间换时间，减少了在此模型中join表的次数；因此，在进行相同效果查询时，星型模型要查询的表往往比3NF模型要少一些，从而大大提高了查询效率。

​	除了查询条件的差异之外，当在相同查询条件下进行查询时，我们的查询类型可以大致分为两类：指定查询条件和包含聚合的查询。根据上表分析可以看出，当指定查询条件（如，查询某个演员参演的全部影片）时，星型模型平均提高的查询效率约为[28%]()；而当进行包含聚合的查询（如，求不同类型电影的平均分）时，星型模型能提高高达[70%]()左右的效率，足以见得星型模型的优势。

​	当然，也有星型模型与3NF模型速度相近的情况（查询条件相似）。这是因为3NF模型只需要在某些较小的实体表（行数少）中匹配指定字符串，找到后用整型的id在较大的关系表（相差几个数量级）中进行匹配；而星型模型需要直接使用字符串在较大的表（行数多）中匹配，所以在一定程度上降低了查询效率（整型数据的匹配要比字符串类型的匹配快一些）；若消除此影响（如查询10.2中，3NF模型actor表125409rows，关系表288792rows，实体表和关系表的数据量没有相差太多）则又能体现出星型模型的优势。
