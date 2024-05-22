const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"

is_discovery(irb) = isnothing(irb) || string(irb) == DISCOVERY_IRB

function configuration(; kwargs...)
    retval = Dict{Symbol, Union{String, Nothing}}()
    source = isfile(CONFIG_FILE) ? JSON.parsefile(CONFIG_FILE) : Dict{String, Any}()
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
    for (to, from) in (:project_slug => "project_id",
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
    for (k,v) in kwargs
        if !isnothing(v)
            retval[k] = v
        end
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

get_case_code(case_code=nothing; config=nothing)::String =
    something(config, configuration(; case_code))[:case_code]
funsql_get_case_code = get_case_code

get_irb_code(irb_code=nothing; config=nothing)::String =
    something(config, configuration(; irb_code))[:irb_code]
funsql_get_irb_code = get_irb_code

is_discovery() = is_discovery(get_irb_code())
funsql_is_discovery = is_discovery
