
  
    
        create or replace table `dbt_svaghasiya`.`my_second_dbt_model`
      
      
    using delta
      
      
      
      
      
      
      as
      -- Use the `ref` function to select from other models

select *
from `dbt_svaghasiya`.`my_first_dbt_model`
where id = 1
  