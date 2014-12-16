Feature: Book endpoints

As a content manager
I want to be able to view details about a book
In order to assess it's suitability for sale

  Scenario: Looking up a specific book's details
    Given IMS has information about an existing book
    When I request the book details
    Then the request is successful
    And the correct book is returned

  Scenario: Looking up a book with an invalid id (non-GUID)
    Given I have information about an invalid book
    When I request the book details
    Then the request fails because it was invalid
    And I get back a properly formatted InvalidUUID error response

  Scenario: Looking up a non-existant book's details
    Given IMS does not have information about a specific book
    When I request the book details 
    Then the request fails because the book was not found
    And I get back a properly formatted NotFound error response

  Scenario: Re-indexing details for an existing book
    Given IMS has information about an existing book
    When I request the book is re-indexed
    Then the request is successful

  Scenario: Re-indexing details for a book with a nonsense ID
    Given I have information about an invalid book
    When I request the book is re-indexed
    Then the request fails because it was invalid
    And I get back a properly formatted InvalidUUID error response

  Scenario: Re-indexing details for a non-existant book
    Given IMS does not have information about a specific book
    When I request the book is re-indexed 
    Then the request fails because the book was not found
    And I get back a properly formatted NotFound error response
