module KnowsAboutHealthEndpoints
  def get_ping_endpoint
    http_get :health, "ping", "Accept" => "text/plain"
    @response_data = parse_last_api_response
  end

  def get_report_endpoint
    http_get :health, "report", "Accept" => "application/json"
    @response_data = parse_last_api_response
  end

  def get_threads_endpoint
    http_get :health, "threads", "Accept" => "text/plain"
    @response_data = parse_last_api_response
  end
end

World(KnowsAboutHealthEndpoints)
