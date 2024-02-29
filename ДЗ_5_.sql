--=============== МОДУЛЬ 5. РАБОТА С POSTGRESQL =======================================
--= ПОМНИТЕ, ЧТО НЕОБХОДИМО УСТАНОВИТЬ ВЕРНОЕ СОЕДИНЕНИЕ И ВЫБРАТЬ СХЕМУ PUBLIC===========
SET search_path TO public;

--======== ОСНОВНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Сделайте запрос к таблице payment и с помощью оконных функций добавьте вычисляемые колонки согласно условиям:
--Пронумеруйте все платежи от 1 до N по дате
--Пронумеруйте платежи для каждого покупателя, сортировка платежей должна быть по дате
--Посчитайте нарастающим итогом сумму всех платежей для каждого покупателя, сортировка должна 
--быть сперва по дате платежа, а затем по сумме платежа от наименьшей к большей
--Пронумеруйте платежи для каждого покупателя по стоимости платежа от наибольших к меньшим 
--так, чтобы платежи с одинаковым значением имели одинаковое значение номера.
--Можно составить на каждый пункт отдельный SQL-запрос, а можно объединить все колонки в одном запросе.

select customer_id, payment_id, payment_date, 
row_number() over (order by payment_date) as column_1, 
row_number() over (partition by customer_id order by payment_date) as column_2,
sum(amount) over (partition by customer_id order by payment_date, amount desc) as column_3,
dense_rank() over (partition by customer_id order by amount desc) as column_4
from payment
order by customer_id, column_4


--ЗАДАНИЕ №2
--С помощью оконной функции выведите для каждого покупателя стоимость платежа и стоимость 
--платежа из предыдущей строки со значением по умолчанию 0.0 с сортировкой по дате.
select customer_id, payment_id, payment_date, amount, 
lag(amount,1,0) over (partition by customer_id order by payment_date desc) as last_amount
from payment
order by customer_id, payment_date




--ЗАДАНИЕ №3
--С помощью оконной функции определите, на сколько каждый следующий платеж покупателя больше или меньше текущего.
select customer_id, payment_id, payment_date, amount, 
amount - lead(amount) over (partition by customer_id order by payment_date desc) as difference
from payment




--ЗАДАНИЕ №4
--С помощью оконной функции для каждого покупателя выведите данные о его последней оплате аренды.
select s.customer_id, s.payment_id, s.payment_date, s.amount
from 
(select customer_id, payment_id, payment_date, amount, row_number() over (partition by customer_id order by payment_date desc)
from payment) as s
where s.row_number=1




--======== ДОПОЛНИТЕЛЬНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--С помощью оконной функции выведите для каждого сотрудника сумму продаж за август 2005 года 
--с нарастающим итогом по каждому сотруднику и по каждой дате продажи (без учёта времени) 
--с сортировкой по дате.

select staff_id, payment_date, sum(amount) over(partition by staff_id, payment_date order by payment_date) as "sum(amount)", sum(amount) over(partition by staff_id order by staff_id, payment_date)
from 
(select staff_id, payment_date::date, sum(amount) as amount
from payment
where date_trunc('month', payment_date)='2005-08-01 00:00:00.000'
group by staff_id, payment_date::date) as s



--ЗАДАНИЕ №2
--20 августа 2005 года в магазинах проходила акция: покупатель каждого сотого платежа получал
--дополнительную скидку на следующую аренду. С помощью оконной функции выведите всех покупателей,
--которые в день проведения акции получили скидку

select customer_id, payment_date, row_number as payment_number
from 
(select customer_id, payment_date, row_number() over (order by payment_date)
from payment
where payment_date::date='2005-08-20 00:00:00.000') as s 
where row_number % 100 = 0



--ЗАДАНИЕ №3
--Для каждой страны определите и выведите одним SQL-запросом покупателей, которые попадают под условия:
-- 1. покупатель, арендовавший наибольшее количество фильмов
-- 2. покупатель, арендовавший фильмов на самую большую сумму
-- 3. покупатель, который последним арендовал фильм

select country, a.first_name||' '||a.last_name as "Покупатель (max шт)", b.first_name||' '||b.last_name as "Покупатель (max сумма)", c.first_name||' '||c.last_name as "Покупатель (последний)"
from
country
left join (select distinct country, first_name, last_name, max(aa.count) over(partition by country)
from (select customer_id, count(rental_id)
	  from payment
	  group by customer_id)	as aa
join customer using (customer_id)
join address using (address_id)
join city using (city_id)
join country using (country_id)) as a using (country)
left join (select distinct country, first_name, last_name, max(amount) over(partition by country)
from payment
join customer using (customer_id)
join address using (address_id)
join city using (city_id)
join country using (country_id)) as b using (country)
left join (select distinct country, first_name, last_name, max(payment_date) over(partition by country)
from payment
join customer using (customer_id)
join address using (address_id)
join city using (city_id)
join country using (country_id)) as c using (country)
order by country

