## 3 tutorial

### 3.1 connecting to and disconnecting from the server

`mysql -h host -p port -u root -p`

### 3.2 entering queries

`select version(), current_date, now(),user()`

### 3.3 creating and using a database

```
show databases;

```



#### 3.3.1 creating and selecting a database

```
create database menagerie;
use menagerie;
mysql -h host -u user -p menagerie
```



#### 3.3.2 creating a table

```
show tables;
create table pet(name varchar(20), owner varchar(20), species varchar(20), sex char(1), birth date, death date);
desc pet;
```



#### 3.3.3 loading data

date : YYYY-MM-DD

#### 3.3.4 retrieving information from a table

`select * from pet`

```sql
select * from pet where name = 'bowser'
select * from pet where birth >= '1998-1-1'
select * from pet where species = 'dog' and sex = 'f';
select * from pet where species = 'snake' or species = 'bird'
select * from pet where (species = 'cat' and sex = 'm') or (species = 'dog' and sex = 'f')
select name, birth from pet;
select owner from pet;
select distinct owner from pet;
select name, species, birth from pet where species = 'dog' or species = 'cat'
select name, birth from pet order by birth
select name, birth from pet order by birth desc;
select name, species, birth from pet order by species, birth desc
select name, birth, curdate(), timestampdiff(year, birth, curdate()) as age from pet;
select name, birth, curdate(), timestampdiff(year, birth, curdate()) as age from pet order by name;
select name, birth, curdate(), timestampdiff(year, birth, curdate()) as age from pet order  by age
select name, birth, death, timestampdiff(year, birth, death) as age from pet where death is not null order by age;
select name, birth, month(birth) from pet;
select name, birth from pet where month(birth) = 5;
select name, birth from pet where month(birth) = month(date_add(curdate(), interval 1 month));
select name, birth form pet where month(birth) = mod(month(curdate()), 12)+1;
select '2018-10-31' + interval 1 day;
select '2018-10-31' + interval 1 day;
select '2018-10-32' + interval 1 day;
show warnings;
select 1 is null, 1 is not null;
select 1 = null, 1 <> null, 1 < null , 1 > null;
select 0 is null, 0 is not null, ''is null, '' is not null;
select * from pet where name like 'b%';
select * from pet where name like '%fy';
select * from pet where name like '%w%'
select * from pet where name like '_____'; //five characters
select * from pet where regexp like(name, '^b'); // start with 'b'
select * from pet where regexp like (name, 'fy$'); // end with 'fy'
select * from pet where regexp like( name, 'w'); //containing a w
select * from pet where regexp like(name, '^.....$'); // exactly five characters
select * from pet where regexp like (name, '^.{5}$') // equals to the last one


```

count rows

```sql
select count(*) from pet;
select owner, count(*) from pet group by owner;
select species, count(*) from pet group by species;
select sex, count(*) from pet group by sex
select species, sex, count(*) from pet group by species, sex;
select species, sex, count(*) from pet where species = 'dog' or species = 'cat' group by species, sex;
select species, sex, count(*) from pet where sex is not null group by species, sex;
select owner, count(*) from pet; // error
select species, sex group by sex; // error
```

more than one table

```sql
create table event(name varchar(20), date date, type varchar(20), remark varchar(255));

select pet.name, timestampdiff(year, birth, date) as age, remark from  pet inner join event on pet.name = event.name where event.type = 'litter';
select p1.name, p1.sex, p2.name, p2.sex, p1.species from pet as p1 inner join pet as p2 on p1.species = p2.species and p1.sex = 'f' and p1.death is null and p2.sex = 'm' and p2.death is null



```



### 3.4 getting information about databases and tables

```
select database();
show tables;
desc pet;
```



### 3.5 using mysql in batch model

```shell
mysql -h host -u user -p < batch-file
mysql -h host -u user -p < batch-file > mysql.out
mysql -h host -u user -p < batch-file | more

```



### 3.6 examples and common queries



#### 3.6.1  某列最大值

`select max(article) as article from shop`

#### 3.6.2 某列最大值的那一行

`select article, dealer, price from shop where price  = (select max(price ) from shop)`

`select s1.article, s1.dealer, s1.price from shop s1 left join shop s2 on s1.price < s2.price where s2.article is null`

`select article, dealer, price from shop order by price desc limit 1`;

#### 3.6.3 分组, 组内最大

以图书分组, 找出每本书最贵价格

`select article, max(price) as price from shop group by article order by article;`

#### 3.6.4 

`select article, dealer, price from shop s1 where price = ( select max(s2.price) from shop s2 where s1.article = s2.article) order by article`



```sql
select article, dealer, price from shop s1 where price = ( select max(s2.price) from shop s2 where s1.article = s2.article) order by article

select s1.article, dealer, s1.price from shop s1 join
(select article, max(price) as price from shop group by article) 
as s2 on 
s1.article = s2.article and s1.price = s2.price 
order by article

select s1.article, s1.dealer, s1.price
from shop s1
left join shop s2 on s1.article = s2.article and s1.price < s2.price where s2.article is null order by s1.article
```

left join 工作在: 当s1.price 是组内最大值, 没有一个s2.price更大, 所以相应的s2.article 是null

#### 3.6.5 foreign keys

```sql
create table shirt(
	id smallint unsigned not null auto_increment,
	style enum('t-shirt', 'polo','dress') not null,
    color enum('red', 'blue','orange','white','black') not null
    owner smallint unsigned not null references person(id),
    primary key(id)
);
```



#### 3.6.6

`select s.* from person p inner join shirt s on s.owner = p.id where p.name like 'lilliana%' and s.color <> 'white'; `

#### 3.6.7 searching on two keys

`select field1_index, field2_index from test_table where field1_index = '1' or field2_index = '1'`

`select field1_index, field2_index from test_table where field1_index = '1' union `

`select field1_index, field2_index from test_table where field2_index = '1'`

#### 3.6.8

`select year, month, bit_count(bit_or(1 << day)) as days from t1 group by year, month`

#### 3.6.9 using auto_increment

```sql
CREATE TABLE animals (
id MEDIUMINT NOT NULL AUTO_INCREMENT,
name CHAR(30) NOT NULL,
PRIMARY KEY (id)
);

INSERT INTO animals (name) VALUES
('dog'),('cat'),('penguin'),
('lax'),('whale'),('ostrich');
SELECT * FROM animals;

iNSERT INTO animals (id,name) VALUES(0,'groundhog');
INSERT INTO animals (id,name) VALUES(NULL,'squirrel');

INSERT INTO animals (id,name) VALUES(100,'rabbit');
INSERT INTO animals (id,name) VALUES(NULL,'mouse');
SELECT * FROM animals;
```

`last_insert_id()`

如果超过了可以表示的范围, 就会插入失败

tinyint 范围是127, tinyint unsigned 是255

对于一次插入多条记录, 那么last_insert_id() 返回的是第一个插入的记录的数字



