CREATE EXTENSION tablefunc;


drop table client_to_vente ;
CREATE TABLE client_to_vente (
  client_id int,
  product_type text,
  order_count int
) ;

with xxx as (
    with tmp as (
        select * from transaction
        join nomenclature on transaction.prod_id = nomenclature.product_id
    )
    select client_id,product_type,count(order_id) as order_count from tmp
    group by (client_id,product_type)
    )
select * from xxx ;


with tmp as (
SELECT *
FROM crosstab(
  'select client_id, product_type, order_count
   from xxx
   where product_type = ''MEUBLE'' or product_type = ''DECO''
   order by 1,2')
AS xxx(client_id int, vente_meuble int,vente_deco int)
) insert into client_to_vente select client_id,vente_meuble,vente_deco from tmp ;

-- select * from client_to_vente ;

-- \copy client_to_vente (client_id,vente_meuble,vente_deco) to result_2.csv  delimiter ';' ;
