#=
  Meng Zhou
  CS696 - Intro to Big Data
  Assignment #3
  Part 1 - Election Data
  Date: 10/20/2016
=#

# Useful info:
# http://dataframesjl.readthedocs.io/en/latest/io.html
# http://stackoverflow.com/questions/21559683/how-do-you-change-multiple-column-names-in-a-julia-version-0-3-dataframe
# https://en.wikibooks.org/wiki/Introducing_Julia/DataFrames#Loading_data_into_dataframes


# using DataFrames
# using DataArrays
# using Gadfly
# using RDatasets
using DataFrames, RDatasets, Gadfly, StatsBase

# Change the line below to match your file path
# file_location = "C:\\Users\\meng_\\Google Drive\\Fall 2016\\BigData696\\Programs\\Assignments\\Assignment#3\\EelctionData\\EelctionData\\"
file_location = "C:/Users/meng_/Google Drive/Fall 2016/BigData696/Programs/Assignments/Assignment#3/EelctionData/EelctionData/"
# Change the above line to match your file path

# Having some fun...
countryname = Dict("UK" => ["UK", "Britain", "GB", "not a member of the EU"],
                    "RU" => ["RU", "Rus", "Russia", "CCCP", "USSR", "Росси́я"])

#################################################
# 1. Data Cleaning
function load_data(country::AbstractString)
    local election_data::DataFrames.DataFrame
    if country in countryname["UK"]
        election_data = load_uk_data()
        # deleterows!(election_data, size(election_data)[1])
    elseif country in countryname["RU"]
        election_data = load_ru_data()
    end
    return election_data
end


function load_uk_data()
    file_name = "UK2010.csv"
    fullpath = file_location * file_name
    uk_data = readtable(fullpath, separator=',')
    # I spent a long time debugging this just because I misspelled "separator"
    # as "seperator"! I'll never forget about this...
    # For UK data, the last row needs to be deleted
    deleterows!(uk_data, size(uk_data)[1])
    return uk_data
end


function load_ru_data()
    subdir = "Russia2011/"
    file1 = "Russia2011_1of2.csv"
    file2 = "Russia2011_2of 2.csv"  # There is a space between "of" and the 2nd "2"
    ru_part1 = file_location * subdir * file1
    ru_part2 = file_location * subdir * file2

    ru_data1 = readtable(ru_part1)
    ru_data2 = readtable(ru_part2)

    # Name part 2 with the same column names in part 1 (they ARE the same)
    # In order to make the append happen
    names!(ru_data2, [Symbol(col) for col in names(ru_data1)])
    append!(ru_data1, ru_data2)

    return ru_data1
end


# function clean_data(election_data::DataFrames.DataFrame, column::Int64)
#     deleterows!(election_data, find(isna(election_data[:,column])))
function clean_data(election_data::DataFrames.DataFrame, column::Symbol)
    # deleterows!(election_data, find(isna(election_data[:,column])))
    # TODO(MZ): Can't just delete the row, because other columns could have useful values
    #           in the same row. Instead, just zero them out
    toclean = find(isna(election_data[:,column]))
    election_data[toclean,column] = 0
end


# If the column contains Noninteger values, deletion is sufficient
function delete_data(election_data::DataFrames.DataFrame, column::Symbol)
    deleterows!(election_data, find(isna(election_data[:,column])))
end


#################################################
#################################################
# 2. Simple queries
# a. How many voters are in each country?
function count_voters(country::AbstractString, election_data::DataFrames.DataFrame)
    local voters_count::Int64 # The number of rows in Russian data exceeds typemax(Int16)
    if country in countryname["UK"]
        # clean_data(election_data, 5)
        # voters_count = sum(election_data[:,5])
        # MEMO: Using magic numbers is easier but they are still magic numbers
        col = Symbol("Electorate")
        clean_data(election_data, col)
        voters_count = sum(election_data[:,col])
    elseif country in countryname["RU"]
        # clean_data(election_data, 4)
        # voters_count = sum(election_data[:,4])
        col = Symbol("Number_of_voters_included_in_voters_list")
        clean_data(election_data, col)
        voters_count = sum(election_data[:,col])
    end
    return voters_count
end


# b. How many votes were cast in each country?
function count_votes(country::AbstractString, election_data::DataFrames.DataFrame)
    local votes_cast::Int64
    if country in countryname["UK"]
        col = Symbol("Votes")
        clean_data(election_data, col)
        votes_cast = sum(election_data[:,col])
    elseif country in countryname["RU"]
        col = Symbol("Number_of_valid_ballots")
        clean_data(election_data, col)
        votes_cast = sum(election_data[:,col])
    end
    return votes_cast
end


# c. Which party received the most votes in each country?
# Useful info:
# https://www.reddit.com/r/Julia/comments/56kkly/how_to_get_column_name_from_column_index/
function most_party(country::AbstractString, election_data::DataFrames.DataFrame)
    local maxvote::Int64 = 0
    local maxcol::Symbol
    if country in countryname["UK"]
        for col in names(election_data)[7:end]
            clean_data(election_data, col)
            votes = sum(election_data[col])
            # votes > maxvote ? maxvote, maxcol = votes, col : nothing
            if votes > maxvote
                maxvote = votes
                maxcol = col
            end
        end
    elseif country in countryname["RU"]
        for col in names(election_data)[22:end]
            clean_data(election_data, col)
            votes = sum(election_data[col])
            # votes > maxvote ? maxvote, maxcol = votes, col : nothing
            if votes > maxvote
                maxvote = votes
                maxcol = col
            end
        end
    end
    # return convert(AbstractString, maxcol)
    return maxcol
    # TODO(MZ): Gotta figure out how to convert symbols to strings
end


# d. What is the mean and standard deviation of the number of voters in each
#    district in each country?
function mean_voters(country::AbstractString, election_data::DataFrames.DataFrame)
    result = 0
    if country in countryname["UK"]
        col = Symbol("Constituency_Name")
        delete_data(election_data, col)
        result = mean(election_data[:,:Electorate])
    elseif country in countryname["RU"]
        col = Symbol("Name_of_district")
        delete_data(election_data, col)
        result = mean(election_data[:,:Number_of_voters_included_in_voters_list])
    end
    return result
end


function std_voters(country::AbstractString, election_data::DataFrames.DataFrame)
    result = 0
    if country in countryname["UK"]
        col = Symbol("Constituency_Name")
        delete_data(election_data, col)
        result = std(election_data[:,:Electorate])
    elseif country in countryname["RU"]
        col = Symbol("Name_of_district")
        delete_data(election_data, col)
        result = std(election_data[:,:Number_of_voters_included_in_voters_list])
    end
    return result
end


# e. Using Gadfly produce a histogram of number of voters in each district for
#    each district for each country. What sort of differences or similarities
#    are there between the two countries in this regard?
# Useful info:
# http://gadflyjl.org/stable/lib/geoms/geom_histogram.html
function voters_hist(country::AbstractString, election_data::DataFrames.DataFrame)
    if country in countryname["UK"]
        plot(election_data, x="Electorate", Geom.histogram,
            Guide.title("Number of Voters per District - UK"))
    elseif country in countryname["RU"]
        plot(election_data, x="Number_of_voters_included_in_voters_list",
            Geom.histogram, Guide.title("Number of Voters per District - Russia"))
    end
end


# f. What is the mean and standard deviation of the number of votes cast in each
#    district in each country
function mean_votes(country::AbstractString, election_data::DataFrames.DataFrame)
    result = 0
    if country in countryname["UK"]
        delete_data(election_data, :Constituency_Name)
        result = mean(election_data[:,:Votes])
    elseif country in countryname["RU"]
        delete_data(election_data, :Name_of_district)
        result = mean(election_data[:,:Number_of_valid_ballots])
    end
    return result
end


function std_votes(country::AbstractString, election_data::DataFrames.DataFrame)
    result = 0
    if country in countryname["UK"]
        delete_data(election_data, :Constituency_Name)
        result = std(election_data[:,:Votes])
    elseif country in countryname["RU"]
        delete_data(election_data, :Name_of_district)
        result = std(election_data[:,:Number_of_valid_ballots])
    end
    return result
end


#################################################
#################################################
# 3. Sanity checks
function count_error_district(country::AbstractString, election_data::DataFrames.DataFrame)
    local error_count::Int64 = 0
    if country in countryname["UK"]
        # Clean all the columns of votes for each party
        for col in names(election_data)[7:end]
            clean_data(election_data, col)
        end
        col_votes = :Votes
        for i in range(1,length(election_data[:,col_votes]))
            party_votes = 0
            # for j in range(22,length(election_data))
            for j = 7:length(election_data)
                # println(length(election_data))
                party_votes += election_data[i,j]
            end
            if party_votes != election_data[i,col_votes]
                error_count += 1
            end
        end

    elseif country in countryname["RU"]
        for col in names(election_data)[22:end]
            clean_data(election_data, col)
        end
        col_votes = :Number_of_valid_ballots
        for i in range(1, length(election_data[:,col_votes]))
        # size(election_data)[1] also works
            party_votes = 0
            # for j in range(22,length(election_data))
            for j = 22:length(election_data)
                # println(length(election_data))
                party_votes += election_data[i,j]
            end
            if party_votes != election_data[i,col_votes]
                error_count += 1
            end
        end
    end
    return error_count
end


#################################################
#################################################
# 4. Investigating the election results
# a. For each district compute the turnout rate. Produce histograms of the
#    turnout rate in each country per district. Does the result in each country
#    approximate a normal distribution. A high turnout rate could mean high
#    interest in particular districts or it could represent ballot stuffing.
function turnout_rate(country::AbstractString, election_data::DataFrames.DataFrame)
    turnout = Array{Float64,1}(size(election_data)[1])
    if country in countryname["UK"]
        col_voters = :Electorate
        col_votes = :Votes
        clean_data(election_data, col_voters)
        clean_data(election_data, col_votes)
        for i in range(1, size(election_data)[1])
            num_voters = election_data[i, col_voters]
            num_votes = election_data[i, col_votes]
            turnout[i] = num_votes / num_voters
        end
    elseif country in countryname["RU"]
        col_voters = :Number_of_voters_included_in_voters_list
        col_votes = :Number_of_valid_ballots
        clean_data(election_data, col_voters)
        clean_data(election_data, col_votes)
        for i in range(1, size(election_data)[1])
            num_voters = election_data[i, col_voters]
            num_votes = election_data[i, col_votes]
            turnout[i] = num_votes / num_voters
        end
    end
    # Add a column named "Turnout" to the data table
    election_data[:Turnout] = turnout
    plot(election_data, x="Turnout", Geom.histogram,
        Guide.title("Turnout Rate per District - $country"))
end


# b. Compute the total number of votes in each district won by the coalition of
#    the Tories and Lib Dems
function coalition_votes(election_data::DataFrames.DataFrame)
    coalition = Array{Int64,1}(size(election_data)[1])
    col_Con = :Con
    col_LD = :LD
    clean_data(election_data, col_Con)
    clean_data(election_data, col_LD)
    for i in range(1, size(election_data)[1])
        coalition[i] = election_data[i, col_Con] + election_data[i, col_LD]
    end
    # Add a column named "Coalition" to the UK data table
    election_data[:Coalition] = coalition
end


# c. For each country produce a scatterplot of votes botained by the winners in
#    each district and the turnout rate in each district.
function winners(country::AbstractString, election_data::DataFrames.DataFrame)
    winner_turnout = Array{Float64,1}(size(election_data)[1])
    winner_votes = Array{Int64,1}(size(election_data)[1])
    if country in countryname["UK"]
        # Clean all the columns of votes for each party
        for col in names(election_data)[7:end]
            clean_data(election_data, col)
        end
        # Compute the winner data in each district
        for i in range(1, size(election_data)[1])
            win_vote = 0
            for j = 7:length(election_data)
                party_votes = election_data[i,j]
                party_votes > win_vote ? win_vote = party_votes : nothing
            end
            total_votes = election_data[i, :Votes]
            winner_votes[i] = win_vote
            winner_turnout[i] = win_vote / total_votes
        end
    elseif country in countryname["RU"]
        for col in names(election_data)[22:end]
            clean_data(election_data, col)
        end
        # Compute the winner data in each district
        for i in range(1, size(election_data)[1])
            win_vote = 0
            for j = 22:length(election_data)
                party_votes = election_data[i,j]
                party_votes > win_vote ? win_vote = party_votes : nothing
            end
            total_votes = election_data[i, :Number_of_valid_ballots]
            winner_votes[i] = win_vote
            winner_turnout[i] = win_vote / total_votes
        end
    end
    # Add columns named "Winner_Votes" and "Winner_Turnout" to the data table
    election_data[:Winner_Votes] = winner_votes
    election_data[:Winner_Turnout] = winner_turnout
    plot(election_data, x="Winner_Votes", y="Winner_Turnout",
        Geom.point, Geom.smooth(method=:lm),
        Guide.title("Winner Turnout per District - $country"))
end


# d. Plot 4c for Russia with random samples.
function got_russians(election_data::DataFrames.DataFrame, sample_size::Int64)
    # sample_size = 10000
    sample_rows = sample(1:size(election_data)[1], sample_size, replace=false)
    sample_votes = election_data[sample_rows, :Winner_Votes]
    sample_turnout = election_data[sample_rows, :Winner_Turnout]

    russia_sample = DataFrame(Sample_Winner_Votes = sample_votes,
                            Sample_Winner_Turnout = sample_turnout)
    plot(russia_sample, x="Sample_Winner_Votes", y="Sample_Winner_Turnout",
        Geom.point, Geom.smooth(method=:lm),
        Guide.title("Winner Turnout per District in Sample District - Russia"))
end

#################################################
# End of Part 1
