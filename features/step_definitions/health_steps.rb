When(/^I request the (\w+) health endpoint$/) do |endpoint|
  send("get_#{endpoint}_endpoint".to_sym)
end

Then(/^I get a valid ping health response returned$/) do
  expect(@response_data).to eq("pong")
end

Then(/^I get a valid report health response returned$/) do
  @response_data.each_key { |k|
    expect(@response_data[k]['healthy']).to_not be_nil
  }
end
