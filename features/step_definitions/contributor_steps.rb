Given(/^(?:\w+) ha(?:s|ve) information about an? (.+) contributor$/) do |condition|
  @contributor = data_for_a(:contributor, which: condition)
end

Given(/^IMS does not have information about a specific contributor$/) do
  @contributor = { 'id' => "16df6d65-8b87-46b2-9fe5-27e7934c77e2", 'title' => "This doesn't really exist" }
end

When(/^I request the contributor details$/) do
  get_contributor_details(@contributor['id'])
end

When(/^I request the contributor is re-indexed$/) do
  submit_contributor_for_reindexing(@contributor['id'])
end

Then(/^the correct contributor is returned$/) do
  expect(@response_data['names']['display']['value']).to eq(@contributor['names']['display'])
end
