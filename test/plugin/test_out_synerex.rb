require "helper"
require "fluent/plugin/out_synerex.rb"

class SynerexOutputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::SynerexOutput).configure(conf)
  end
end
