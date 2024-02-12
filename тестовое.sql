

-- загружаем данные из csv в исходном качестве
create table test_csv
(
	SYMBOL varchar(4),
	SYSTEM char,
	type char,
	MOMENT varchar,
	ID varchar,
	action char,
	PRICE varchar,
	VOLUME varchar,
	ID_DEAL varchar,
	PRICE_DEAL varchar
); 

COPY test_csv(SYMBOL,SYSTEM,TYPE,MOMENT,ID,ACTION,PRICE,VOLUME,ID_DEAL,PRICE_DEAL)
FROM '/Users/natalyaboltinova/20181229_fut_ord.csv'
DELIMITER ','
CSV HEADER;

drop table order_history

--создаем структуру базы данных
--сначала таблицы измерений
create table symbols
(
	id int generated always as identity primary key,
	symbol char(4)
);

create table actions
(
	id char(1) primary key,
	name varchar(11)
);

select * from actions

create table orders
(
	id bigint primary key
);

create table moments
(
	id int generated always as identity primary key,
	moment timestamp unique
);

create table types 
(
	id char(1) primary key,
	name char(7)
);

--последней создаем таблицу фактов
create table fact_table 
(
	order_id bigint references orders(id),
	symbol_id smallint references symbols(id),
	moment_id int references moments(id),
	action_id char(1) references actions(id),
	type_id char(1) references types(id),
	price numeric,
	volume int
)

--загружаем данные в таблицы измерений
insert into moments(moment)
(select distinct (substring(moment, 1, 4)||'-'||substring(moment, 5, 2)||'-'||substring(moment, 7, 2)
 ||' '||substring(moment, 9, 2)||':'||substring(moment, 11, 2)||':'||substring(moment, 13,2)||'.'||substr(moment,15))::timestamp from test_csv)

insert into actions
values ('0', 'Снятие'), ('1','Выставление'), ('2', 'Сделка')

insert into orders 
(select distinct id::bigint from test_csv)
 
insert into symbols(symbol)
(select distinct symbol from test_csv)

insert into types
values ('B', 'Покупка'), ('S', 'Продажа')

--последней - в таблицу фактов
insert into fact_table
select t.id, s.id, m.id, t.action, t.type, t.price, t.volume 
from
(select id::bigint, symbol, action, type, price::numeric, volume::int, 
(substring(moment, 1, 4)||'-'||substring(moment, 5, 2)||'-'||substring(moment, 7, 2)
 ||' '||substring(moment, 9, 2)||':'||substring(moment, 11, 2)||':'||substring(moment, 13,2)||'.'||substr(moment,15))::timestamp as moment
from test_csv) t
left join moments m using(moment)
left join symbols s using(symbol)

-- Создаем обобщенное табличное выражение (temp_order_history) с заданными инструментом и моментом, на который заявка должна быть активна
with temp_order_history as 
(select order_id id, price, type_id type, volume, action_id action
from FACT_TABLE ft
join moments m on m.id = ft.moment_id
join symbols s on s.id = ft.symbol_id
where m.moment < '2018-12-28 18:50:33.983' and s.symbol = 'GZM9'
)
-- В этом запросе мы отбираем активные заявки из temp_order_history
select type, id, price, remainder
	from
	(--10) Выведем общую информацию о заявке, где reminder - невыполненый объем заявки
	select toh.type, o.id, toh.price, o.remainder, 
	case --11) Проранжируем операции покупки и продажи по убыванию и возрастанию соответсвенно
		when type = 'B' then dense_rank() over (partition by type order by price desc)
		when type = 'S' then dense_rank() over (partition by type order by price)
	end as rank
from
(--2) Выводим id таких заявок и объем каждой из них
select id, volume as remainder
from temp_order_history 
where id in
(--1) Отбираем точно активные заявки - те, по которым было только событие выставление (1)
select id
from temp_order_history
group by id
having min(action) = max(action)
)
union --3) Соединим со второй частью активных заявок
select id, volume - sum_of_deals as remainder --8) Считаем разницу объема выставления (1) и общего объема сделок (2)
from
(
--5) Берем строки с такими id, только там, где action = 2, группируем их по id и суммируем объемы(volume).
-- Получаем общий объем совершенных сделок(2) по каждому такому id
select id, sum(volume) as sum_of_deals
from temp_order_history
where id in
(
--4) Находим id заявок, где нет событий снятие (0) и есть сделка (2), то есть потенциально активные.
select id
from temp_order_history
group by id 
having min(action) = '1' and max(action) = '2'
)
and action = '2'
group by id
)
join --7) И объединяем с 5) по id. Получаем таблицу с тремя полями - id, общий объем сделок (2) и объем выставления (1)
(--6) Выводим id и объемы всех выставлений (1)
select id, volume 
from temp_order_history
where action = '1'
)
using (id)
where volume - sum_of_deals <> 0 -- 9) Если не ноль - заявка активная
) as o
inner join temp_order_history toh 
using(id)
)
where rank = 1 --12) Тогда ранк 1 показывает и макисмальную цену покупки, и минимальную цену продажи



select max(id)
from actions 

select order_id
from fact_table
group by order_id 
having min(action_id) = '2'










