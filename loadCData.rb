require 'pacer'
require 'pacer-neo4j'
require 'csv'

class Life

	attr_accessor :g

	def initialize(graph = nil)
		self.g = graph
		self.g.allow_auto_tx = false
	end

	def init_graph
		start_time = Time.now
		doCSV('births.csv')
		doCSV('deaths.csv')
		puts "Loaded CSVs in #{Time.now - start_time} seconds."
	end

	def cities
		g.v(:type => 'city')
	end 

	def cities_with_higher_birth_rate_than_death_rate_in_the_same_year
		cities.filter{ |v| v['births'].to_i > v['deaths'].to_i }
	end

	def cities_in_a_year(year)
		# not sure why this doesn't work; lookahead?
		cities.filter{ |v| v.in_e(:city).out_v.in_e(:month).out_v[:name] == year }
	end

	def cities_from_a_year(year)
		g.v(:type => 'year', :name => year).out_e(:month).in_v.out_e(:city).in_v
	end

	def cities_with_growth_trend_in_birth_rates_for_the_year(year)
		
	end

	def total_deaths_for_the_year(year)
		total = 0

		cities_from_a_year(year) do |v|
			total = total + v[:deaths].to_i
		end

                total
	end

	def doCSV(csvName)

		i = 0

		g.transaction do |commit, rollback|

		CSV.foreach(csvName) do |row|
			yearValue = row[0]
			monthValue = row[1]
			cityValue = row[2]
			numValue = row[3]

			yearVert = g.v(:name => yearValue).first

			monthVert = yearVert.out_e(:month).in_v(:name => monthValue).first if !yearVert.nil?
			
			cityVert = monthVert.out_e(:city).in_v(:name => cityValue).first if !monthVert.nil?

			if yearVert.nil? then
				yearVert = g.create_vertex :name => yearValue, :type => 'year'
			end

			if monthVert.nil? then
				monthVert = g.create_vertex :name => monthValue, :type => 'month'
				g.create_edge nil, yearVert, monthVert, :month
			end

			if csvName == 'births.csv' then
				if cityVert.nil? then
					cityVert = g.create_vertex :name => cityValue, :type => 'city', :births => numValue
				else
					cityVert[:births] = numValue
				end
			else
				if cityVert.nil? then
					cityVert = g.create_vertex :name => cityValue, :type => 'city', :deaths => numValue
				else
					cityVert[:deaths] = numValue
				end
			end 

			g.create_edge nil, monthVert, cityVert, :city

		        i = i + 1

			commit.call if i % 5000 == 4999
		end

		end
	end
end

