--Шаг 2. Изучение данных нового источника
--2.1
--external_source.craft_products_orders представляет общую информацию по 
--craftsman, product, orders;
--Для соединения craft_products_orders с customers, есть поле customer_id
SELECT *
  FROM external_source.craft_products_orders;
 
SELECT public.get_all_attributes('external_source.craft_products_orders');

WITH cus_t AS (
SELECT product_name, product_description,
	-- Нумерация дубликата
	ROW_NUMBER() OVER (PARTITION BY product_name ORDER BY product_description ASC) AS duplicatenum,
	-- Количество дубликатов 
	COUNT(product_name) OVER (PARTITION BY product_name) AS duplicatecnt
 FROM external_source.craft_products_orders)
SELECT *
  FROM cus_t
 ORDER BY ;
  
SELECT product_name, product_description, count(product_name)
  FROM external_source.craft_products_orders
 GROUP BY product_name, product_description
 ORDER BY 3 DESC;

-- HAVING count(product_name), 3;

SELECT product_id, product_name, count(product_name), count(1) OVER()
  FROM external_source.craft_products_orders
 GROUP BY product_id, product_name;
 
SELECT count(1)
  FROM external_source.craft_products_orders;
  
--2.2
--external_source.customers состоит из данных только по customers

SELECT *
  FROM external_source.customers;
 
  
SELECT *, count(c.customer_id) OVER()
  FROM external_source.customers c;
 
WITH cus_t AS (
SELECT customer_name, customer_address,
	-- Number each duplicate
	ROW_NUMBER() OVER (PARTITION BY customer_name ORDER BY customer_address ASC) AS duplicatenum,
	-- Count duplicates
	COUNT(customer_name) OVER (PARTITION BY customer_name) AS duplicatecnt
 FROM external_source.customers)
SELECT *
  FROM cus_t
 ORDER BY 3 DESC;
 --WHERE duplicatecnt > 1;

SELECT customer_id, 
  FROM external_source.customers;
 
--
--Шаг 3. Напишите скрипт переноса данных из источника в хранилище
--Необходимо добавить к старому скрипту обновления и добавления данных добавить новые данные
BEGIN;
 
	DROP TABLE IF EXISTS tmp_sources;

    --обогащения данных старого источника, новыми
	CREATE TEMP TABLE tmp_sources AS
	--old code
	SELECT order_id , -- идентификатор заказа;
	order_created_date , -- дата создания заказа;
	order_completion_date , -- дата выполнения заказа;
	order_status , -- статус заказа;
	craftsman_id , -- идентификатор мастера;
	craftsman_name , -- имя мастера;
	craftsman_address , -- адрес мастера;
	craftsman_birthday , -- день рождения мастера;
	craftsman_email , -- электронная почта мастера;
	product_id , -- идентификатор товара;
	product_name , -- наименование товара;
	product_description , -- описание товара;
	product_type , -- тип товара;
	product_price , -- цена товара;
	customer_id , -- идентификатор заказчика;
	customer_name , -- имя заказчика;
	customer_address , -- адрес заказчика;
	customer_birthday , -- день рождения заказчика;
	customer_email -- электронная почта заказчика.
	  FROM source1.craft_market_wide
	UNION 
	SELECT order_id , -- идентификатор заказа;
	order_created_date , -- дата создания заказа;
	order_completion_date , -- дата выполнения заказа;
	order_status , -- статус заказа;
	cmoc.craftsman_id , -- идентификатор мастера;
	craftsman_name , -- имя мастера;
	craftsman_address , -- адрес мастера;
	craftsman_birthday , -- день рождения мастера;
	craftsman_email , -- электронная почта мастера;
	cmoc.product_id , -- идентификатор товара;
	product_name , -- наименование товара;
	product_description , -- описание товара;
	product_type , -- тип товара;
	product_price , -- цена товара;
	customer_id , -- идентификатор заказчика;
	customer_name , -- имя заказчика;
	customer_address , -- адрес заказчика;
	customer_birthday , -- день рождения заказчика;
	customer_email -- электронная почта заказчика.
	  FROM source2.craft_market_masters_products cmmp
	  	   LEFT JOIN source2.craft_market_orders_customers cmoc ON cmmp.craftsman_id  = cmoc.craftsman_id 
	  	          											   AND cmmp.product_id  = cmoc.product_id
	UNION 
	SELECT order_id , -- идентификатор заказа;
	order_created_date , -- дата создания заказа;
	order_completion_date , -- дата выполнения заказа;
	order_status , -- статус заказа;
	craf.craftsman_id , -- идентификатор мастера;
	craftsman_name , -- имя мастера;
	craftsman_address , -- адрес мастера;
	craftsman_birthday , -- день рождения мастера;
	craftsman_email , -- электронная почта мастера;
	product_id , -- идентификатор товара;
	product_name , -- наименование товара;
	product_description , -- описание товара;
	product_type , -- тип товара;
	product_price , -- цена товара;
	cmc.customer_id , -- идентификатор заказчика;
	customer_name , -- имя заказчика;
	customer_address , -- адрес заказчика;
	customer_birthday , -- день рождения заказчика;
	customer_email -- электронная почта заказчика.
	  FROM source3.craft_market_orders f
	       LEFT JOIN source3.craft_market_craftsmans craf ON f.craftsman_id = craf.craftsman_id 
		   LEFT JOIN source3.craft_market_customers cmc ON f.customer_id = cmc.customer_id
	--new code
	UNION
	SELECT order_id , -- идентификатор заказа;
	order_created_date , -- дата создания заказа;
	order_completion_date , -- дата выполнения заказа;
	order_status , -- статус заказа;
	craftsman_id , -- идентификатор мастера;
	craftsman_name , -- имя мастера;
	craftsman_address , -- адрес мастера;
	craftsman_birthday , -- день рождения мастера;
	craftsman_email , -- электронная почта мастера;
	product_id , -- идентификатор товара;
	product_name , -- наименование товара;
	product_description , -- описание товара;
	product_type , -- тип товара;
	product_price , -- цена товара;
	c.customer_id , -- идентификатор заказчика;
	c.customer_name , -- имя заказчика;
	c.customer_address , -- адрес заказчика;
	c.customer_birthday , -- день рождения заказчика;
	c.customer_email -- электронная почта заказчика.
	  FROM external_source.craft_products_orders cro
	       LEFT JOIN external_source.customers c ON cro.customer_id = c.customer_id;
	      
	--тестовый замер количества строк перед обновлением
	SELECT count(craftsman_id) cnt_craftsman_no_update FROM dwh.d_craftsman;

	--обновление существующих записей и добавление новых в dwh.d_products 
	MERGE INTO dwh.d_craftsman d
	USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_sources) t
	   ON  d.craftsman_name = t.craftsman_name 
	  AND d.craftsman_email = t.craftsman_email
	WHEN MATCHED THEN
	  UPDATE SET craftsman_address = t.craftsman_address, 
	             craftsman_birthday = t.craftsman_birthday, 
	             load_dttm = current_timestamp
	WHEN NOT MATCHED THEN
	  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
	  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);
	 
	--обновление существующих записей и добавление новых в dwh.d_products
	MERGE INTO dwh.d_product d
	USING (SELECT DISTINCT product_name, product_description, product_type, product_price from tmp_sources) t
	   ON d.product_name = t.product_name 
	   AND d.product_description = t.product_description 
	   AND d.product_price = t.product_price
	WHEN MATCHED THEN
	  UPDATE SET product_type = t.product_type, load_dttm = current_timestamp
	WHEN NOT MATCHED THEN
	  INSERT (product_name, product_description, product_type, product_price, load_dttm)
	  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);
	 
	--обновление существующих записей и добавление новых в dwh.d_customer
	MERGE INTO dwh.d_customer d
	USING tmp_sources t
	   ON d.customer_name = t.customer_name 
	   AND d.customer_email = t.customer_email
	WHEN MATCHED THEN
	  UPDATE SET customer_address= t.customer_address, 
	customer_birthday= t.customer_birthday, load_dttm = current_timestamp
	WHEN NOT MATCHED THEN
	  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);
	
	 --тестовый замер количества строк перед обновлением
	SELECT count(craftsman_id) cnt_craftsman_update FROM dwh.d_craftsman;


	-- проверка данных
--	SELECT * FROM dwh.d_craftsman WHERE craftsman_name ILIKE 'Selena Wannop';
--
--	SELECT *, count(*) OVER()
--	  FROM external_source.craft_products_orders cpo
--	       LEFT JOIN dwh.d_craftsman c ON cpo.craftsman_id = c.craftsman_id;
--
--	SELECT *, count(1) OVER() FROM dwh.d_product; --WHERE customer_id IS NULL;

	SELECT count(order_id) cnt_order_no_update FROM dwh.f_order;


	--обогащение данных фактов с новым источником
	DROP TABLE IF EXISTS tmp_sources_fact;
	CREATE TEMP TABLE tmp_sources_fact AS 
	SELECT
		src.product_id, -- идентификатор товара
		src.craftsman_id, -- идентификатор мастера
		src.customer_id, -- идентификатор заказчика
		order_created_date, -- дата создания заказа
		order_completion_date, -- дата выполнения заказа
		order_status, -- статус выполнения заказа (created, in progress, delivery, done)
		dp.load_dttm
	 FROM tmp_sources src
	   JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name 
	                           AND dc.craftsman_email = src.craftsman_email 
	   JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name 
	                             AND dcust.customer_email = src.customer_email 
	   JOIN dwh.d_product dp ON dp.product_name = src.product_name 
	                         AND dp.product_description = src.product_description 
	                         AND dp.product_price = src.product_price;  
	                        
	--обновление данных в таблицы факта
	MERGE INTO dwh.f_order fo
	USING tmp_sources_fact sf
	   ON fo.product_id = sf.product_id 
	   AND fo.craftsman_id = sf.craftsman_id 
	   AND fo.customer_id = sf.customer_id 
	   AND fo.order_created_date = sf.order_created_date 
	WHEN MATCHED THEN
	  UPDATE SET order_completion_date= sf.order_completion_date, 
	order_status= sf.order_status, load_dttm = current_timestamp
	WHEN NOT MATCHED THEN
	  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	  VALUES (sf.product_id, sf.craftsman_id, sf.customer_id, order_created_date, order_completion_date, order_status, current_timestamp);

	SELECT count(order_id) cnt_order_update FROM dwh.f_order;


	--Шаг 4. Изучите потребности бизнеса в новой витрине
	--Шаг 5. Напишите DDL новой витрины
	
	
	DROP TABLE IF EXISTS dwh.customer_report_datamart;
	
	CREATE TABLE dwh.customer_report_datamart (
		id int8 GENERATED ALWAYS AS IDENTITY NOT NULL ,-- идентификатор записи
		customer_id int8 NOT NULL,
		customer_name varchar NOT NULL,
		customer_address varchar NOT NULL,
		customer_birthday date NOT NULL,
		customer_email varchar NOT NULL,
		customer_money numeric(15, 2) NOT NULL, -- сумма, которую потратил заказчик
		platform_money numeric(15, 2) NOT NULL, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик)
		count_order int8 NOT NULL, -- количество заказов у заказчика за месяц;
		avg_price_order numeric(10, 2) NOT NULL, -- средняя стоимость одного заказа у заказчика за месяц
		median_time_order_completed numeric(10, 1) NULL, --медианное время в днях от момента создания заказа до его завершения за месяц
		top_product_category varchar NOT NULL, -- самая популярная категория товаров у этого заказчика за месяц
		top_craftsman_id int8 NOT NULL, -- идентификатор самого популярного мастера ручной работы у заказчика.
		count_order_created int8 NOT NULL,
		count_order_in_progress int8 NOT NULL,
		count_order_delivery int8 NOT NULL,
		count_order_done int8 NOT NULL,
		count_order_not_done int8 NOT NULL,
		report_period varchar NOT NULL,
		CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
	);
	
	-- DDL таблицы инкрементальных загрузок
	DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;
	
	CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
	    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	    load_dttm DATE NOT NULL,
	    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
	);
	
	
	
	--Шаг 6. Напишите скрипт для инкрементального обновления витрины
	SAVEPOINT create_test;
	
	WITH
	dwh_delta AS ( -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений
	    SELECT     
	            dc.customer_id AS customer_id,
	            dc.customer_name AS customer_name,
	            dc.customer_address AS customer_address,
	            dc.customer_birthday AS customer_birthday,
	            dc.customer_email AS customer_email,
	            fo.order_id AS order_id,
	            dp.product_id AS product_id,
	            dp.product_price AS product_price,
	            dp.product_type AS product_type,
	            dcs.craftsman_id AS craftsman_id,
	            --DATE_PART('year', AGE(dcs.customer_birthday)) AS customer_age,
	            fo.order_completion_date - fo.order_created_date AS diff_order_date, 
	            fo.order_status AS order_status,
	            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
	            crd.customer_id AS exist_customer_id,
	            dc.load_dttm AS customer_load_dttm,
	            dcs.load_dttm AS craftsman_load_dttm,
	            dp.load_dttm AS products_load_dttm
	            FROM dwh.f_order fo 
	                INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id 
	                INNER JOIN dwh.d_craftsman dcs ON fo.craftsman_id = dcs.craftsman_id 
	                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
	                LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id
	                    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                            (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                            (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                            (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
	),                   
	                            
	                            
	dwh_update_delta AS ( -- делаем выборку мастеров ручной работы, по которым были изменения в DWH. По этим мастерам данные в витрине нужно будет обновить
	    SELECT     
	            dd.exist_customer_id AS customer_id
	            FROM dwh_delta dd 
	                WHERE dd.exist_customer_id IS NOT NULL        
	),
	
	dwh_delta_insert_result AS (                             
	    SELECT 
		        T4.customer_id AS customer_id,
		        T4.customer_name AS customer_name,
		        T4.customer_address AS customer_address,
		        T4.customer_birthday AS customer_birthday,
		        T4.customer_email AS customer_email,
		        T4.customer_money AS customer_money,
		        T4.platform_money AS platform_money,
		        T4.count_order AS count_order,
		        T4.avg_price_order AS avg_price_order,
		        T4.median_time_order_completed AS median_time_order_completed,
		        T4.product_type AS top_product_category,
		        T4.craftsman_id AS top_craftsman_id,
		        T4.count_order_created AS count_order_created,
		        T4.count_order_in_progress AS count_order_in_progress,
		        T4.count_order_delivery AS count_order_delivery,
		        T4.count_order_done AS count_order_done,
		        T4.count_order_not_done AS count_order_not_done,
		        T4.report_period AS report_period 
	       		FROM (
	                  SELECT     -- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и применяем оконную функцию для определения самой популярной категории товаров
	                        *
	                        ,ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS row_number_count_product  
	                        FROM ( 
	                            SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
	                                T1.customer_id AS customer_id,
	                                T1.customer_name AS customer_name,
	                                T1.customer_address AS customer_address,
	                                T1.customer_birthday AS customer_birthday,
	                                T1.customer_email AS customer_email,
	                                --T1.craftsman_id AS craftsman_id,
	                                SUM(T1.product_price) AS customer_money,
	                                SUM(T1.product_price) * 0.1 AS platform_money,
	                                COUNT(order_id) AS count_order,
	                                AVG(T1.product_price) AS avg_price_order,
	                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
	                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
	                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
	                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
	                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
	                                T1.report_period AS report_period
	                                FROM dwh_delta AS T1
	                                    WHERE T1.exist_customer_id IS NULL
	                                        GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
	                            ) AS T2 
	                                INNER JOIN (
	                                    SELECT     -- Эта выборка поможет определить самый популярный товар у мастера ручной работы. Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. Для данных этой выборки можно применить оконную функцию, которая и покажет самую популярную категорию товаров у мастера
	                                            dd.customer_id AS customer_id_for_product_type, 
	                                            dd.product_type, 
	                                            COUNT(dd.product_id) AS count_product
	                                            FROM dwh_delta AS dd
	                                                GROUP BY dd.customer_id, dd.product_type
	                                                    ORDER BY count_product DESC
	                                ) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
	                                
	                                INNER JOIN (
	                                SELECT 
	                            			customer_id_for_craftsman,
											craftsman_id
											FROM (
												SELECT *,
													   ROW_NUMBER() OVER (PARTITION BY customer_id_for_craftsman ORDER BY popular_craftsman DESC) row_number_craftsamn
												  FROM 
													(
													 SELECT     -- Эта выборка поможет определить самый популярный товар у мастера ручной работы. Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. Для данных этой выборки можно применить оконную функцию, которая и покажет самую популярную категорию товаров у мастера
													    dd.customer_id AS customer_id_for_craftsman, 
													    dd.craftsman_id,    
													    count(craftsman_id) AS popular_craftsman
													    FROM dwh_delta AS dd
													        GROUP BY dd.customer_id, dd.craftsman_id
													            ORDER BY 1, 3 DESC
													) t
											) t2
											WHERE row_number_craftsamn = 1
	                                ) AS T5 ON T2.customer_id = T5.customer_id_for_craftsman 
	     
	            ) AS T4              	                    
				WHERE row_number_count_product = 1                                
				ORDER BY report_period 
	),
	dwh_delta_update_result AS ( -- делаем перерасчёт для существующих записей витринs, так как данные обновились за отчётные периоды. Логика похожа на insert, но нужно достать конкретные данные из DWH
	      SELECT 
		        T4.customer_id AS customer_id,
		        T4.customer_name AS customer_name,
		        T4.customer_address AS customer_address,
		        T4.customer_birthday AS customer_birthday,
		        T4.customer_email AS customer_email,
		        T4.customer_money AS customer_money,
		        T4.platform_money AS platform_money,
		        T4.count_order AS count_order,
		        T4.avg_price_order AS avg_price_order,
		        T4.median_time_order_completed AS median_time_order_completed,
		        T4.product_type AS top_product_category,
		        T4.craftsman_id AS top_craftsman_id,
		        T4.count_order_created AS count_order_created,
		        T4.count_order_in_progress AS count_order_in_progress,
		        T4.count_order_delivery AS count_order_delivery,
		        T4.count_order_done AS count_order_done,
		        T4.count_order_not_done AS count_order_not_done,
		        T4.report_period AS report_period 
	       		FROM (
	                  SELECT     -- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и применяем оконную функцию для определения самой популярной категории товаров
	                        *
	                        ,ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS row_number_count_product  
	                        FROM ( 
	                            SELECT 
	                                T1.customer_id AS customer_id,
	                                T1.customer_name AS customer_name,
	                                T1.customer_address AS customer_address,
	                                T1.customer_birthday AS customer_birthday,
	                                T1.customer_email AS customer_email,
	                                SUM(T1.product_price) AS customer_money,
	                                SUM(T1.product_price) * 0.1 AS platform_money,
	                                COUNT(order_id) AS count_order,
	                                AVG(T1.product_price) AS avg_price_order,
	                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
	                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
	                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
	                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
	                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
	                                T1.report_period AS report_period
	                                FROM dwh_delta AS T1
	                                GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
	                            ) AS T2 
	                                INNER JOIN (
	                                    SELECT    
	                                            dd.customer_id AS customer_id_for_product_type, 
	                                            dd.product_type, 
	                                            COUNT(dd.product_id) AS count_product
	                                            FROM dwh_delta AS dd
	                                                GROUP BY dd.customer_id, dd.product_type
	                                                    ORDER BY count_product DESC
	                                ) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
	                                
	                                INNER JOIN (
	                                SELECT 
	                            			customer_id_for_craftsman,
											craftsman_id
											FROM
											(
												SELECT *,
													   ROW_NUMBER() OVER (PARTITION BY customer_id_for_craftsman ORDER BY popular_craftsman DESC) row_number_craftsamn
												  FROM 
													(
													 SELECT
													    dd.customer_id AS customer_id_for_craftsman, 
													    dd.craftsman_id,    
													    count(craftsman_id) AS popular_craftsman
													    FROM dwh_delta AS dd
													        GROUP BY dd.customer_id, dd.craftsman_id
													            ORDER BY 1, 3 DESC
													) t
											) t2
											WHERE row_number_craftsamn = 1
	                                ) AS T5 ON T2.customer_id = T5.customer_id_for_craftsman 
	     
	            ) AS T4              	                    
				WHERE row_number_count_product = 1                                
				ORDER BY report_period 
	),
	insert_delta AS ( -- выполняем insert новых расчитанных данных для витрины 
	    INSERT INTO dwh.customer_report_datamart (
	        customer_id,
	        customer_name,
	        customer_address,
	        customer_birthday, 
	        customer_email, 
	        customer_money, 
	        platform_money, 
	        count_order, 
	        avg_price_order, 
	        median_time_order_completed,
	        top_product_category, 
	        top_craftsman_id,
	        count_order_created, 
	        count_order_in_progress, 
	        count_order_delivery, 
	        count_order_done, 
	        count_order_not_done, 
	        report_period
	    ) SELECT 
	            customer_id,
	            customer_name,
	            customer_address,
	            customer_birthday,
	            customer_email,
	            customer_money,
	            platform_money,
	            count_order,
	            avg_price_order,
	            median_time_order_completed,
	            top_product_category,
				top_craftsman_id,
	            count_order_created, 
	            count_order_in_progress,
	            count_order_delivery, 
	            count_order_done, 
	            count_order_not_done,
	            report_period 
	            FROM dwh_delta_insert_result
	),
	update_delta AS ( -- выполняем обновление показателей в отчёте по уже существующим мастерам
	    UPDATE dwh.customer_report_datamart SET
	        customer_name = updates.customer_name, 
	        customer_address = updates.customer_address, 
	        customer_birthday = updates.customer_birthday, 
	        customer_email = updates.customer_email, 
	        customer_money = updates.customer_money, 
	        platform_money = updates.platform_money, 
	        count_order = updates.count_order, 
	        avg_price_order = updates.avg_price_order, 
	        median_time_order_completed = updates.median_time_order_completed, 
	        top_product_category = updates.top_product_category, 
			top_craftsman_id = updates.top_craftsman_id,
	        count_order_created = updates.count_order_created, 
	        count_order_in_progress = updates.count_order_in_progress, 
	        count_order_delivery = updates.count_order_delivery, 
	        count_order_done = updates.count_order_done,
	        count_order_not_done = updates.count_order_not_done, 
	        report_period = updates.report_period
	    FROM (
	        SELECT 
	            customer_id,
	            customer_name,
	            customer_address,
	            customer_birthday,
	            customer_email,
	            customer_money,
	            platform_money,
	            count_order,
	            avg_price_order,
	            median_time_order_completed,
	            top_product_category,
	            top_craftsman_id,
	            count_order_created,
	            count_order_in_progress,
	            count_order_delivery,
	            count_order_done,
	            count_order_not_done,
	            report_period 
	            FROM dwh_delta_update_result) AS updates
	    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
	),
	insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
	    INSERT INTO dwh.load_dates_customer_report_datamart (
	        load_dttm
	    )
	    SELECT GREATEST(COALESCE(MAX(customer_load_dttm), NOW()), 
	                    COALESCE(MAX(craftsman_load_dttm), NOW()), 
	                    COALESCE(MAX(products_load_dttm), NOW())) 
	        FROM dwh_delta
	)
	SELECT 'increment datamart'; -- инициализируем запрос CTE 
	
	
	SELECT *
	  FROM dwh.customer_report_datamart;
	
	
	
	ROLLBACK TO SAVEPOINT create_test;


ROLLBACK;
COMMIT;


/*

SELECT * FROM dwh.f_order fo;



SELECT count(1) FROM dwh.d_craftsman_test; --3996
SELECT * FROM external_source.craft_products_orders;
SELECT * FROM external_source.customers c;

SELECT public.get_all_attributes('external_source.customers');

SELECT * FROM dwh.d_craftsman_test --WHERE craftsman_name ILIKE 'Selena Wannop';

SELECT *, count(1) OVER() FROM dwh.d_craftsman_test WHERE craftsman_id IS NULL;

*/

