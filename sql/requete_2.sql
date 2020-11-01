CREATE EXTENSION tablefunc;

drop table if exists client_interest ;

CREATE TABLE client_interest (
  client_id date,
  ventes_meubles int,
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



CREATE TABLE ct(id SERIAL, rowid TEXT, attribute TEXT, value TEXT);
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att1','val1');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att2','val2');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att3','val3');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att4','val4');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att1','val5');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att2','val6');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att3','val7');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att4','val8');

select * from ct limit 10 ;

SELECT *
FROM crosstab(
  'select rowid, attribute, value
   from ct
   where attribute = ''att2'' or attribute = ''att3''
   order by 1,2')
AS ct(row_name text, blah_blah text, category_2 text, category_3 text);


SELECT *
FROM crosstab(
  'select client_id, product_type, order_count
   from xxx
   where product_type = ''MEUBLE'' or product_type = ''DECO''
   order by 1,2')
AS xxx(client_id int, vente_meuble int,vente_deco int);
