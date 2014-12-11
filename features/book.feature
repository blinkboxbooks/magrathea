Feature: Book endpoints

As a content manager
I want to be able to view details about a book
In order to assess it's suitability for sale

Scenario: Looking up a specific book's details
  Given IMS has information about an existing book
  When I request the book details
  Then the request is successful
  And the correct book is returned

Scenario: Looking up a non-existant book's details
  Given IMS does not have information about a specific book
  When I request the book details 
  Then the request fails because the book was not found

Scenario: Re-indexing details for an existing book
  Given IMS has information about an existing book
  When I request the book is re-indexed
  Then the request is successful

Scenario: Re-indexing details for a non-existant book
  Given IMS does not have information about a specific book
  When I request the book is re-indexed 
  Then the request fails because the book was not found
