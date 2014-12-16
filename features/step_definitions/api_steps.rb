Then(/^I get back a properly formatted (\w+) error response$/) do |error_code|
  expect(@response_data['code']).to eq(error_code)
end
