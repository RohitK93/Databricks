-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Higher Order Functions
-- MAGIC > - Higher Order Functions are functions that operate on complex data types such as arrays and maps.
-- MAGIC > - They allow you to pass functions as arguments (such as lambda expressions), apply transformations, and return arrays or maps.
-- MAGIC > - They are extremely useful for manipulating arrays without exploding them.
-- MAGIC
-- MAGIC
-- MAGIC ##### Commonly Used Higher Order Array Functions:
-- MAGIC > - TRANSFORM
-- MAGIC > - FILTER
-- MAGIC > - EXISTS
-- MAGIC > - AGGREGATE
-- MAGIC
-- MAGIC
-- MAGIC ##### Syntax
-- MAGIC `<function_name> (array_column, lambda_expression)`
-- MAGIC
-- MAGIC lambda_expression: `element -> expression`
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items AS
SELECT * FROM
VALUES
(1, array('smartphone', 'laptop', 'monitor')),
(2, array('tablet', 'headphones', 'smartwatch')),
(3, array('keyboard', 'mouse'))
AS orders(order_id, items);


-- COMMAND ----------

SELECT * FROM order_items;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Convert all the item names to be UPPERCASE (TRANSFORM Function)

-- COMMAND ----------

SELECT order_id,
        TRANSFORM(items, x -> UPPER(x)) as upper_items
  FROM order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Filter only items that contain the string 'smart' (FILTER Function)

-- COMMAND ----------

SELECT order_id,items,
        FILTER(items, x -> x LIKE '%smart%') as smart_items
  FROM order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Check to see whether the order includes any 'monitor' (EXISTS Function)

-- COMMAND ----------

SELECT order_id, items,
        EXISTS(items, x -> x = 'monitor') as has_monitor
  FROM order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### array with multiple objects

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT * FROM VALUES
  (1, array(
    named_struct('name', 'smartphone', 'price', 699),
    named_struct('name', 'laptop', 'price', 1199),
    named_struct('name', 'monitor', 'price', 399)
  )),
  (2, array(
    named_struct('name', 'tablet', 'price', 599),
    named_struct('name', 'headphones', 'price', 199),
    named_struct('name', 'smartwatch', 'price', 299)
  )),
  (3, array(
    named_struct('name', 'keyboard', 'price', 89),
    named_struct('name', 'mouse', 'price', 59)
  ))
AS orders(order_id, items);


-- COMMAND ----------

select * from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Convert all the item names to be UPPERCASE and add 10% TAX to each item (TRANSFORM)

-- COMMAND ----------

select order_id,items,
       transform(items, x -> named_struct(
                                          'name', UPPER(x.name),
                                          'price', ROUND(x.price * 1.10, 2)  
                                        )) as items_with_tax  
from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Calculate the total order amount for each order (AGGREGATE)

-- COMMAND ----------

select order_id,
       AGGREGATE(items, 0, (acc, x) -> acc + x.price) AS total_order_price
from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Map Functions
-- MAGIC
-- MAGIC #### Commonly Used Higher Order Map Functions:
-- MAGIC - TRANSFORM_VALUES: Applies a transformation to the values in the map.
-- MAGIC - TRANSFORM_KEYS: Applies a transformation to the keys in the map.
-- MAGIC - MAP_FILTER: Filters key-value pairs from the map based on a condition.
-- MAGIC
-- MAGIC ##### Syntax:
-- MAGIC `<function_name> (map_column, lambda_expression)`
-- MAGIC
-- MAGIC lambda expression: `(key, value) -> expression`

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW order_item_prices AS
SELECT * FROM VALUES
  (1, map('smartphone', 699, 'laptop', 1199, 'monitor', 399)),
  (2, map('tablet', 599, 'headphones', 199, 'smartwatch', 299)),
  (3, map('keyboard', 89, 'mouse', 59))
AS orders(order_id, item_prices);


-- COMMAND ----------

select * from order_item_prices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. convert all item names to uppercase using the TRANSFORM_KEYS function.
-- MAGIC

-- COMMAND ----------

SELECT order_id,
       TRANSFORM_KEYS(item_prices, (item, price) -> UPPER(item)) AS items_upper_case
FROM order_item_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Apply 10% TAX to each item price TRANSFORM_VALUES

-- COMMAND ----------

SELECT order_id,
       TRANSFORM_VALUES(item_prices, (item, price) -> ROUND(price * 1.10, 2)) AS price_with_tax
FROM order_item_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Filter only items with price above $500 (MAP_FILTER function)

-- COMMAND ----------

SELECT order_id,item_prices,
       MAP_FILTER(item_prices, (item, price) -> price > 500) AS premium_itmes
FROM order_item_prices;
