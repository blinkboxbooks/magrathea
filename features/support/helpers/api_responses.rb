module CanParseAPIResponses
  def parse_last_api_response
    JSON.load(HttpCapture::RESPONSES.last.body)
  rescue JSON::ParserError
    HttpCapture::RESPONSES.last.body
  end
end

World(CanParseAPIResponses)
