module CanParseAPIResponses
  def parse_last_api_response
    begin
      JSON.load(HttpCapture::RESPONSES.last.body)
    rescue JSON::ParserError
      HttpCapture::RESPONSES.last.body
    end
  end
end

World(CanParseAPIResponses)
