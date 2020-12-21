Feature: testing parent assignments

  Background:
     Given stg_bloodmoon__users_accounts with some data would have
        | user_id | account_id | relationship   | date_assignment_starts | date_assignment_ends | date_added          | comment                                                                                                     |
        | 1       | 1001       | SALES_REP      | 2019-05-01 22:00:00    | 2019-05-02 00:00:00  | 2019-05-09 00:00:00 | will show up on the 1st as it was active by the end of that day                                             |
        | 2       | 1002       | SERVICE_REP    | 2019-05-01 00:00:00    | 2019-05-01 13:00:00  | 2019-05-09 00:00:00 | wont be used as its not active by the end of the day                                                        |
        | 3       | 1003       | STRATEGIC_REP  | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  | 2019-05-09 00:00:00 | active for the 1st and 2nd                                                                                  |
        | 41      | 1004       | AGENCY_REP     | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  | 2019-05-09 00:00:00 |                                                                                                             |
        | 42      | 1004       | AGENCY_REP     | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  | 2019-05-10 00:00:00 | this one has priority over the previous one as it has a later date_added                                    |
        | 51      | 1005       | HIRE_RECRUITER | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  |                     |                                                                                                             |
        | 52      | 1005       | HIRE_RECRUITER | 2019-05-01 01:00:00    | 2019-05-03 00:00:00  |                     | this one has priority over the previous one as it has a later date_assignment_starts and date_added is null |
        | 61      | 1006       | SALES_REP      | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  | 2019-05-09 00:00:00 |                                                                                                             |
        | 62      | 1006       | SALES_REP      | 2019-05-02 00:00:00    | 2019-05-03 00:00:00  | 2019-05-10 00:00:00 | this one has priority over the previous one during its effective period as it has a later date_added        |
        | 71      | 1007       | SERVICE_REP    | 2019-05-01 00:00:00    | 2019-05-03 00:00:00  | 2019-05-09 00:00:00 |                                                                                                             |
        | 72      | 1007       | AGENCY_REP     | 2019-05-02 00:00:00    | 2019-05-03 00:00:00  | 2019-05-10 00:00:00 | this one doesnt overwrite the previous one as its a different relationship                                  |
        | 8       | 1008       | SALES_REP      | 2019-05-01 22:00:00    | 2019-05-02 00:00:00  | 2019-05-09 00:00:00 | wont be used as its not in dim_advertiser_current                                                           |
    And dim_advertiser_current with a few advertisers would have
        | account_id | advertiser_id | parent_company_id | comment                             |
        | 1001       | 9001          | 1                 | test case for adv rep assignment    |
        | 1002       | 9002          | 2                 | test case for adv rep assignment    |
        | 1003       | 9003          | 3                 | test case for adv rep assignment    |
        | 1004       | 9004          | 4                 | test case for adv rep assignment    |
        | 1005       | 9005          | 5                 | test case for adv rep assignment    |
        | 1006       | 9006          | 6                 | test case for adv rep assignment    |
        | 1007       | 9007          | 7                 | test case for adv rep assignment    |
        | 1101       | 9101          | 101               | test case for parent rep assignment |
        | 1102       | 9102          | 101               | test case for parent rep assignment |
        | 1103       | 9103          | 101               | test case for parent rep assignment |
        | 1104       | 9104          | 101               | test case for parent rep assignment |
        | 1105       | 9105          | 101               | test case for parent rep assignment |
        | 1106       | 9106          | 102               | test case for parent rep assignment |
        | 1107       | 9107          | 102               | test case for parent rep assignment |
        | 1108       | 9108          | 102               | test case for parent rep assignment |
        | 1109       | 9109          | 102               | test case for parent rep assignment |
        | 1110       | 9110          | 102               | test case for parent rep assignment |
    When we run the load for calendar

  Scenario: run the test for advertiser_rep_assignment_daily
     Given stg_bloodmoon__users_accounts is loaded with some data
       And dim_advertiser_current is loaded with a few advertisers
      When we query advertiser_rep_assignment_daily
      Then the results of the query ignoring other columns are
         | account_id | relationship   | dt         | user_id | advertiser_id | parent_company_id |
         | 1007       | AGENCY_REP     | 2019-05-02 | 72      | 9007          | 7                 |
         | 1004       | AGENCY_REP     | 2019-05-01 | 42      | 9004          | 4                 |
         | 1005       | HIRE_RECRUITER | 2019-05-01 | 52      | 9005          | 5                 |
         | 1005       | HIRE_RECRUITER | 2019-05-02 | 52      | 9005          | 5                 |
         | 1004       | AGENCY_REP     | 2019-05-02 | 42      | 9004          | 4                 |
         | 1001       | SALES_REP      | 2019-05-01 | 1       | 9001          | 1                 |
         | 1007       | SERVICE_REP    | 2019-05-02 | 71      | 9007          | 7                 |
         | 1006       | SALES_REP      | 2019-05-02 | 62      | 9006          | 6                 |
         | 1003       | STRATEGIC_REP  | 2019-05-01 | 3       | 9003          | 3                 |
         | 1003       | STRATEGIC_REP  | 2019-05-02 | 3       | 9003          | 3                 |
         | 1007       | SERVICE_REP    | 2019-05-01 | 71      | 9007          | 7                 |
         | 1006       | SALES_REP      | 2019-05-01 | 61      | 9006          | 6                 |
