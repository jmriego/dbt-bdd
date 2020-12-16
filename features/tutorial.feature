Feature: showing off behave

  Scenario: run a compilation test
     Given calendar is loaded with this data
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |
      When we compile the query
        """
        select * from {{ref('calendar')}}
        """
      Then dbt didn't fail
      And the compiled query contains
        """
        `d9cfa8e7_calendar`
        """


  Scenario: run a seed data test
     Given calendar is loaded with this data
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |
      When we run the query
        """
        select * from {{ref('calendar')}}
        """
      Then the results of the query are
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |


  Scenario: run a sample unit test
     Given calendar is loaded with this data
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |
      When we run the load for fom
      Then the results of the model are
         | first_day_of_month | count_days |
         | 2020-01-01         | 2          |
         | 2020-02-01         | 1          |
