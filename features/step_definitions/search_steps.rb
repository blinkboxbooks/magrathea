When(/^I search for the book's (\w+)$/) do |property|
  search_for_term(query: @book[property])
end

When(/^I search for the contributor's display name$/) do
  search_for_term(query: @contributor['names']['display'])
end

When(/^I search without a 'q' parameter$/) do
  search_for_term(query: nil)
end

When(/^I search for "([^"]*)"$/) do |term|
  search_for_term(query: term)
end

When(/^I search for "([^"]*)" with a (\w+) of "([^"]*)"$/) do |term, filter, value|
  search_for_term(query: term, filter: { filter => value })
end

Then(/^the book is included in the search results$/) do
  matched_results = @response_data['items'].any? { |i| i['title'] == @book['title'] }
  expect(matched_results).to be_truthy
end

Then(/^the contributor is included in the search results$/) do
  matched_results = @response_data['items'].any? { |i| i['names']['display'] == @contributor['names']['display'] }
  expect(matched_results).to be_truthy
end

Then(/^I get no more than (\d+) results$/) do |count|
  expect(@response_data['items'].size).to_not be > count.to_i
end
