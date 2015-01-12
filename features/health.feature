# Waiting on http://jira.blinkbox.local/jira/PT-220
Feature: Health endpoints

As a maintainer of this component
I want there to be health endpoints
In order to ascertain if the service is alive for loadbalancing/monitoring

  Scenario: Ping endpoint
    When I request the ping health endpoint
    Then the request is successful
    And I get a valid ping health response returned

  Scenario: Report endpoint
    When I request the report health endpoint
    Then the request is successful
    And I get a valid report health response returned 

  Scenario: Threads endpoint
    When I request the threads health endpoint
    Then the request is successful
