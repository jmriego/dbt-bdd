Feature: showing off behave

  Scenario: run a simple test
     Given calendar is loaded with this data
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |
      # When we run the load for fom
      When we compile the query
        """
        select * from {{ref('calendar')}}
        """
      Then dbt didn't fail
      And the compiled query is
        """
        select * from `eda-dev-coresvcs-5517`.`jvalenzuela_userdata`.`3699043a_calendar`
        """
