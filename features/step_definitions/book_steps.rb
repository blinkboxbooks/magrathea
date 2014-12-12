Given(/^(?:\w+) ha(?:s|ve) information about an? (.+) book$/) do |condition|
  @book = data_for_a(:book, which: condition)
end

Given(/^IMS does not have information about a specific book$/) do
  @book = { 'id' => "16df6d65-8b87-46b2-9fe5-27e7934c77e2", 'title' => "This doesn't really exist" }
end

When(/^I request the book details$/) do
  get_book_details(@book['id'])
end

When(/^I request the book is re-indexed$/) do
  submit_book_for_reindexing(@book['id'])
end

Then(/^the correct book is returned$/) do
  expect(@response_data['title']['value']).to eq(@book['title'])
end
