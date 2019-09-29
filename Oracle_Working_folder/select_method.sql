with my_table as
(

SELECT 
ml.datacollector as "METHOD_NAME", ml.MESSAGE,
extract(hour from ml.TIMESTAMP) "HOUR"
--count(extract(hour from ml.TIMESTAMP)) "CNT_FOR_HOUR"
  FROM 
  CommonLogs ml 
WHERE 
    ml.TIMESTAMP BETWEEN 
    TO_DATE('5.02.2014 09:00', 'dd.mm.yyyy hh24:mi') 
    AND TO_DATE('5.02.2014 22:00', 'dd.mm.yyyy hh24:mi')
    and regexp_like(ml.datacollector, 'ENTRY|EXIT|FAILURE|CUSTOM','i' )
    and regexp_like(ml.datacollector,  'exch_module.Clncmpintf.clientSearch','i' )
    
)

select mt4.method_name,mt4.msg2, mt4.hour,  count(*)
          from 
              (
                select mt1.method_name, mt1.hour, 
                      (
                      case
                      
                      when regexp_count(mt1.MESSAGE,  'Не найдено записей соответствующих условиям поиска',1,'i') >= 1 
                      then 'Не найдено записей соответствующих условиям поиска'
                                                                
                                                             
                      
                      when regexp_count(mt1.MESSAGE,  '<mprID>',1,'i') >= 1 then 'MPR'
                                                          
                      when regexp_count(mt1.MESSAGE,  '<lastName>',1,'i') >= 1 then 'lastName'
                      
                      when regexp_count(mt1.MESSAGE,  '<mobilePhone>',1,'i')> = 1 then 'mobilePhone'
                                                            
                      when regexp_count(mt1.MESSAGE,  '<faults>',1,'i') >= 1 then 'faults'
                      
                      when regexp_count(mt1.MESSAGE,  '<docType>',1,'i') >= 1 then 'docType'
                      
                      when regexp_count(mt1.MESSAGE,  '<id_in_id>',1,'i') >= 1 then 'id_in_id'
                      
                                            
                      --else
                      --'other_method' 
                      end 
                      ) as msg2
                     
                     --mt1.MESSAGE 
                
                from my_table mt1
              )mt4
              
--where mt4.msg2 = 'other_method';
                    
group by                 
mt4.method_name,
mt4.msg2, mt4.hour

order by mt4.hour asc , mt4.method_name asc, count(*) desc, mt4.msg2 asc;



    
/****************************************************************************************************************************/


/*****************************************************************/
SELECT
   datacollector as "METHOD_NAME",
   EXTRACT(hour from TIMESTAMP) "HOUR",
   EXTRACT(minute from TIMESTAMP) "MINUTE",
   ROUND(EXTRACT(second from TIMESTAMP)) "SECOND",
   COUNT(extract(second from TIMESTAMP)) "CNT_FOR_SECOND",
   KEY3
FROM
   CommonLogs 
WHERE
   TIMESTAMP BETWEEN TO_DATE('05.02.2014 13:00', 'dd.mm.yyyy hh24:mi') AND TO_DATE('05.02.2014 13:15', 'dd.mm.yyyy hh24:mi')
   AND regexp_like(datacollector, 'ENTRY','i' )
   AND regexp_like(datacollector,  'exch_module.Clncmpintf.clientSearch','i' )
   AND message like '%<lastName>%'
    
group by
   datacollector,
   KEY3,
   EXTRACT(hour from TIMESTAMP),
   EXTRACT(minute from TIMESTAMP),
   EXTRACT(second from TIMESTAMP)
order by
   "HOUR",
   "MINUTE",
   "METHOD_NAME",
   "CNT_FOR_SECOND" ;





