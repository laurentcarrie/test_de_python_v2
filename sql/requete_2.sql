CREATE EXTENSION tablefunc;

drop table if exists client_interest ;

CREATE TABLE client_interest (
  client_id int,
  ventes_meuble int,
   ventes_deco int
) ;

drop table xxx ;
CREATE TABLE xxx (
  client_id int,
  product_type text,
  order_count int
) ;





with tmp as (
        select * from transaction
        join nomenclature on transaction.prod_id = nomenclature.product_id
)
    insert into xxx (client_id,product_type,order_count) (select client_id,product_type,count(order_id) from tmp
    group by (client_id,product_type)) ;



insert into client_interest
SELECT client_id,ventes_meuble,ventes_deco
FROM crosstab(
  'select client_id, product_type, order_count
   from xxx
   where product_type = ''MEUBLE'' or product_type = ''DECO''
   order by 1,2',
  $$VALUES ('MEUBLE'), ('DECO')$$
)
AS (client_id int, ventes_meuble int,ventes_deco int);

\copy client_interest (client_id,ventes_meuble,ventes_deco) to result_2.csv  delimiter ';' ;
