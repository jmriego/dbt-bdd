Feature: showing off behave

  Scenario: run a simple test
     Given calendar is loaded with this data
         | dt         | first_day_of_month |
         | 2020-01-01 | 2020-01-01         |
         | 2020-01-02 | 2020-01-01         |
         | 2020-02-02 | 2020-02-01         |
      When we run the load for fom
      Then dbt didn't fail