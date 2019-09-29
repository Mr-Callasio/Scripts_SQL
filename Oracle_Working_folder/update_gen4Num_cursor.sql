-- ver 0.1

-- Обновляет поле gen4Num_port в таблице extern_file

SET SERVEROUTPUT ON 

declare
		
		start_date					date := TO_DATE('10.02.2015 00:00', 'dd.mm.yyyy hh24:mi');
		end_date					date := TO_DATE('10.05.2015 00:00', 'dd.mm.yyyy hh24:mi');
		fetched_id					varchar2(256) :='';
		fetched_gen4Num_port		varchar2(256) :='';
		content						clob :='';
		direction					varchar2(256) :='';
		cur_gen4Num_port			varchar2(20) :='';
		Update_count				number(20,0):=0;
		Update_count_fr_selct		number(20,0):=0;
		
		


cursor Mycursor_find_account
       is	

with my_table as 
(
select  
        to_char (
          regexp_replace  (
                            regexp_replace  (
                                                        replace(content, chr(10),null),
                                            N'^.*<Название="',null
                                            ),
                                  
                              N'" Дата.*',null
                            ) 
                ) 
               
        
			"gen4Num_port_ord",  
			id, gen4Num_port,content
	
			from  extern_file nfs
			where creation_date between  start_date and end_date
			and direction = 'TO_external'
		
			 
)

select 	id, 		"gen4Num_port_ord", 		gen4Num_port, content
into 	fetched_id, fetched_gen4Num_port, 	cur_gen4Num_port, content
from 	my_table;


begin

	 open Mycursor_find_account;    
			fetch Mycursor_find_account into fetched_id,  fetched_gen4Num_port, cur_gen4Num_port, content;
				
				while Mycursor_find_account%found
					loop
					
						if 	fetched_id is not null 											-- не пустой id
							and
							cur_gen4Num_port is null  								 -- не заполненное поле cur_gen4Num_port
							
						then 
							begin
								
										update 	extern_FILE nff 
										set 	nff.gen4Num_port =  fetched_gen4Num_port
										where   nff.id = fetched_id;

										Update_count :=	Update_count + 1 ;	
										
								
								--dbms_output.put_line('fetched_id = ' || fetched_id || ', fetched_gen4Num_port =' || fetched_gen4Num_port || ' ,gen4Num_port = ' || cur_gen4Num_port );
							end;
							
							else 
								begin
									dbms_output.put_line('fetched_id = ' || fetched_id || ', fetched_gen4Num_port =' || fetched_gen4Num_port || ' ,gen4Num_port = ' || cur_gen4Num_port );
									dbms_output.put_line ('cur_gen4Num_port is not null or is incorrect');
								end;
						
						end if;
						
						fetch Mycursor_find_account into fetched_id,  fetched_gen4Num_port, cur_gen4Num_port, content;
					
					end loop;
	 close Mycursor_find_account;
	 
	 	 
	
	select 	count(*)
	into 	Update_count_fr_selct
	from 	extern_file nff
	where 	gen4Num_port is not null;


	 

	 
	 
	 --rollback; 
	 --rollback;
	 
	 
	 
	 

		dbms_output.put_line('----------------------Report has been generated----------------------------------------------------');
		dbms_output.put_line('start_date = ' || TO_DATE(start_date, 'dd.mm.yyyy hh24:mi')  ||  '  ,end_date = ' || TO_DATE(end_date, 'dd.mm.yyyy hh24:mi')); 
		dbms_output.put_line('Update_count = ' || Update_count);
		dbms_output.put_line('--------------------   Manual rollback!!!!   ----------------------------------------------------');	
		dbms_output.put_line('Update_count_fr_selct = ' || Update_count_fr_selct );
		dbms_output.put_line('--------------------   Manual commit!!!!   ----------------------------------------------------');	
		/**********-- manual  --commit;*******/
		
	 
end;