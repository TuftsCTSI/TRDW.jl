const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"

function configuration()
    source = isfile(CONFIG_FILE) ? JSON.parsefile(CONFIG_FILE) : Dict()
    retval = Dict{Symbol, Union{String, Nothing}}()
    # flatten
    if haskey(source, "project")
        merge!(source, source["project"])
        delete!(source, "project")
    end
    if haskey(source, "case")
        merge!(source, source["case"])
        delete!(source, "case")
    end
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
            :case_slug => "case_id",
            :case_code => "case_number",
            :case_title => "subject")
        retval[to] = get(source, from, nothing)
    end
    # quality checking and defaults
    retval[:irb_code] = something(retval[:irb_code], DISCOVERY_IRB)
    project_code = retval[:project_code]
    if isnothing(project_code)
        retval[:project_code] = "005547"
    else
        @assert startswith(project_code, "P-") && length(project_code) == 8
        retval[:project_code] = project_code[3:end]
    end
    case_code = retval[:case_code]
    if isnothing(case_code)
        retval[:case_code] = "01000526"
    else
        @assert length(case_code) == 8
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
