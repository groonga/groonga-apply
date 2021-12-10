# Copyright (C) 2021  Sutou Kouhei <kou@clear-code.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require "yaml"

module GroongaSync
  class Config
    def initialize(path)
      if File.exist?(path)
        @data = YAML.load(File.read(path))
      else
        @data = {}
      end
    end

    def delta_dir
      # TODO: Improve error handling
      @data["delta_dir"] or raise "No delta_dir"
    end

    def groonga
      Groonga.new(@data["groonga"] || {})
    end

    class Groonga
      def initialize(data)
        @data = data
      end

      def url
        @data["url"] || "http://127.0.0.1:10041"
      end

      def read_timeout
        if @data.key?("read_timeout")
          ::Groonga::Client::Default::READ_TIMEOUT
        else
          @data["read_timeout"]
        end
      end
    end
  end
end
