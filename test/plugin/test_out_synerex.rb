require "helper"
require "fluent/plugin/out_synerex.rb"

class SynerexOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers


  setup do
    Fluent::Test.setup
  end


  private

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::SynerexOutput).configure(conf)
  end

  sub_test_case 'path' do
    test 'normal' do
      d = create_driver('...')
      time = Fluent::Engine.now
      d.run(default_tag:'test') do
        d.feed(time,{'target' => '200'})
      end
      events = d.events

    end
  end
end
