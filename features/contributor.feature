Feature: Contributor endpoints

As a content manager
I want to be able to view details about a contributor
In order to assess it's suitability for sale

  Scenario: Looking up a specific contributor's details
    Given IMS has information about an existing contributor
    When I request the contributor details
    Then the request is successful
    And the correct contributor is returned

  Scenario: Looking up a contributor with an invalid id (non-GUID)
    Given I have information about an invalid contributor
    When I request the contributor details
    Then the request fails because it was invalid
    And I get back a properly formatted InvalidUUID error response

  Scenario: Looking up a non-existant contributor's details
    Given IMS does not have information about a specific contributor
    When I request the contributor details 
    Then the request fails because the contributor was not found
    And I get back a properly formatted NotFound error response

  Scenario: Re-indexing details for an existing contributor
    Given IMS has information about an existing contributor
    When I request the contributor is re-indexed
    Then the request is successful

  Scenario: Re-indexing details for a contributor with a nonsense ID
    Given I have information about an invalid contributor
    When I request the contributor is re-indexed
    Then the request fails because it was invalid
    And I get back a properly formatted InvalidUUID error response

  Scenario: Re-indexing details for a non-existant contributor
    Given IMS does not have information about a specific contributor
    When I request the contributor is re-indexed 
    Then the request fails because the contributor was not found
    And I get back a properly formatted NotFound error response
