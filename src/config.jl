const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"

function configuration()
    @assert isfile(CONFIG_FILE)
    retval = Dict{Symbol, Union{String, Nothing}}()
    source = JSON.parsefile(CONFIG_FILE)
    # flatten
    if haskey(source, "project")
        merge!(source, source["project"])
        delete!(source, "project")
    end
    if haskey(source, "case")
        merge!(source, source["case"])
        delete!(source, "case")
    end
    for (k,v) in source
        if v == "None" || v == "" || ismissing(v)
            source[k] = nothing
        end
    end
    for (from, to) in ("project_id" => :project_slug,
                       "project_name" => :project_code,
                       "project_title" => :project_title,
                       "irb_id" => :irb_code,
                       "irb_start_date" => :irb_start_date,
                       "irb_end_date" => :irb_end_date,
                       "pi_display_name" => :pi_name,
                       "case_id" => :case_slug,
                       "case_number" => :case_code,
                       "subject" => :case_title)
        retval[to] = get(source, from, nothing)
    end
    retval[:irb_code] = something(retval[:irb_code], DISCOVERY_IRB)
    project_code = retval[:project_code]
    if !isnothing(project_code)
        @assert startswith(project_code, "P-") && length(project_code) == 8
        retval[:project_code] = project_code[3:end]
    end
    case_code = retval[:case_code]
    if !isnothing(case_code)
        @assert length(case_code) == 8
    end
    return retval
end

function get_config_item(item)
    item = get(configuration(), item, nothing)
    @assert !isnothing(item) "$item not configured; see TRDW.json file"
    return item
end

get_case_code() = get_config_item(:case_code)
funsql_get_case_code = get_case_code

get_irb_code() = get_config_item(:irb_code)
funsql_get_irb_code = get_irb_code

funsql_get_irb_start_date() = Date(get_config_item(:irb_start_date))
funsql_get_irb_end_date() = Date(get_config_item(:irb_end_date))

@funsql is_during_irb_window() = between(datetime, get_irb_start_date(), get_irb_end_date())

is_discovery() = string(get_irb_code()) == DISCOVERY_IRB
funsql_is_discovery = is_discovery
