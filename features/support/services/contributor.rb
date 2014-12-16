module KnowsAboutContributorApi
  def get_contributor_details(contributor_id)
    http_get :api, "contributors/#{contributor_id.url_encode}", "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end

  def submit_contributor_for_reindexing(contributor_id)
    http_put :api, "contributors/#{contributor_id.url_encode}/reindex", nil, "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end
end

World(KnowsAboutContributorApi)
