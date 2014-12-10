$LOAD_PATH << "lib" if Dir.pwd == __dir__

task :default => :test

desc "Runs all tests"
task :test do
  Rake::Task[:features].invoke
end

desc "Runs all feature tests"
begin
  require "cucumber"
  require "cucumber/rake/task"
  Cucumber::Rake::Task.new(:features) do |t|
    t.cucumber_opts = "--profile #{ENV["PROFILE"]}" if ENV["PROFILE"]
  end
rescue LoadError
  task :features do
    $stderr.puts "Please install cucumber: `gem install cucumber`"
  end
end
