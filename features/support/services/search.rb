module KnowsAboutSearchApi
  def search_for_term(query: nil, filter: {})
    search_url = query.nil? ? "search" : "search?q=#{query.url_encode}"
    filter.each { |k, v| search_url += "&#{k.url_encode}=#{v.url_encode}" }
    http_get :api, search_url, "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end

  def reindex_current_documents
    http_put :api, "search/reindex/current", nil, "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end

  def reindex_history_documents
    http_put :api, "search/reindex/history", nil, "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end
end

World(KnowsAboutSearchApi)
