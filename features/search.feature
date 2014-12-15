Feature: Search endpoints

As a content manager
I want to be able to search for books and contributors
In order to find what info we have on something

Scenario: Searching for a specific book's details
  Given IMS has information about an existing book
  When I search for the book's title
  Then the request is successful
  And the book is included in the search results

Scenario: Search for a specific contributor's details
  Given IMS has information about an existing contributor
  When I search for the contributor's display name
  Then the request is successful
  And the contributor is included in the search results

# Waiting on http://jira.blinkbox.local/jira/browse/PT-201
Scenario: Searching for an empty search term
  When I search for ""
  Then the request fails because it was invalid

# Waiting on http://jira.blinkbox.local/jira/browse/PT-201
Scenario: Searching without a "q" parameter
  When I search without a 'q' parameter
  Then the request fails because it was invalid

Scenario Outline: Searching with a count limit parameter
  When I search for "zaphod" with a count of "<count>"
  Then the request is successful
  And I get no more than <count> results

  Examples:
    | count |
    | 1     |
    | 2     |

# Waiting on http://jira.blinkbox.local/jira/browse/PT-202
Scenario Outline: Searching with an invalid count limit parameter
  When I search for "zaphod" with a count of "<count>"
  Then the request fails because it was invalid
  And I get back a properly formatted BadRequest error response

  Examples:
    | count       | description                   |
    | 2147483648  | Larger than a 32-bit integer  |
    | 0           | Count must be non-zero        |
    | -1          | Count must be greater than 0  |
    |             | Blank string                  |
    | string      | A non-numeric value           |

Scenario Outline: Searching with an offset parameter
  When I search for "zaphod" with a offset of "<offset>"
  Then the request is successful

  Examples:
    | offset  |
    | 1       |
    | 100     |

# Waiting on http://jira.blinkbox.local/jira/browse/PT-202
Scenario Outline: Searching with an invalid offset limit parameter
  When I search for "zaphod" with a offset of "<offset>"
  Then the request fails because it was invalid
  And I get back a properly formatted BadRequest error response

  Examples:
    | offset      | description                   |
    | 2147483648  | Larger than a 32-bit integer  |
    | -1          | Offset must be greater than 0 |
    |             | Blank string                  |
    | string      | A non-numeric value           |
