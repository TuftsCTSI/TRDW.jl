const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"

function configuration()
    source = isfile(CONFIG_FILE) ? JSON.parsefile(CONFIG_FILE) : Dict()
    retval = Dict{Symbol, Union{String, Vector, Nothing}}()
    # normalize none/missing to nothing
    for (k,v) in source
        if v == "None" || v == "" || ismissing(v)
            source[k] = nothing
        end
    end
    # pull out values
    for (to, from) in (
            :project_slug => "project_id",
            :project_code => "project_name",
            :project_title => "project_title",
            :irb_code => "irb_id",
            :irb_start_date => "irb_start_date",
            :irb_end_date => "irb_end_date",
            :pi_name => "pi_display_name",
            :case => "case",
            :project_stem => "project_stem")
        retval[to] = get(source, from, nothing)
    end
    # quality checking and defaults
    retval[:irb_code] = something(retval[:irb_code], DISCOVERY_IRB)
    if DISCOVERY_IRB == retval[:irb_code]
        retval[:irb_start_date] = something(retval[:irb_start_date], "2010-01-01")
        retval[:irb_end_date] = something(retval[:irb_end_date], string(Dates.now())[1:10])
    end
    project_code = retval[:project_code]
    if isnothing(project_code)
        retval[:project_code] = "005547"
    else
        @assert startswith(project_code, "P-") && length(project_code) == 8
        retval[:project_code] = project_code[3:end]
    end
    case = retval[:case]
    if isnothing(case)
        retval[:case] = Any[Dict("case_number" => "01000526")]
    else
        @assert length(retval[:case][1]["case_number"]) == 8
    end
    return retval
end

function get_config_item(item)
    result = get(configuration(), item, nothing)
    @assert !isnothing(result) "$item not configured; see TRDW.json file"
    return result
end

funsql_get_case_code() = get_config_item(:case_code)
funsql_get_project_code() = get_config_item(:project_code)
funsql_get_irb_code() = get_config_item(:irb_code)
funsql_get_irb_start_date() = Date(get_config_item(:irb_start_date))
funsql_get_irb_end_date() = Date(get_config_item(:irb_end_date))

is_discovery() = funsql_get_irb_code() == DISCOVERY_IRB
funsql_is_discovery = is_discovery

@funsql is_during_irb_window() = between(datetime, get_irb_start_date(), get_irb_end_date())
