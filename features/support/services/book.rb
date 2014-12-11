module KnowsAboutBookApi
  def get_book_details(book_id)
    http_get :api, "books/#{book_id.url_encode}", "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end

  def submit_book_for_reindexing(book_id)
    http_put :api, "books/#{book_id.url_encode}/reindex", nil, "Accept" => "application/vnd.blinkbox.books.v2+json"
    @response_data = parse_last_api_response
  end
end

World(KnowsAboutBookApi)
