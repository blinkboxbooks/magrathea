require "httpclient"
require "httpclient/capture"
require "active_support/core_ext"
require "cucumber/rest"

TEST_CONFIG = {}
TEST_CONFIG["server"] = ENV["SERVER"] || "DEV_INT"
TEST_CONFIG["proxy"] = ENV["PROXY_SERVER"]
TEST_CONFIG["debug"] = !!(ENV["DEBUG"] =~ /^on|true$/i)
TEST_CONFIG["fail_fast"] = !!(ENV["FAIL_FAST"] =~ /^on|true$/i)

puts "TEST_CONFIG: #{TEST_CONFIG}" if TEST_CONFIG["debug"]

require "cucumber/blinkbox"

class Object
  def url_encode
    URI.encode_www_form_component("#{self}")
  end
end

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end
