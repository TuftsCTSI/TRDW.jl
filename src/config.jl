struct Project
    project_id::String
    project_name::String
    project_title::String
    irb_id::Union{String, Nothing}
    irb_start_date::Union{Dates.Date, Nothing}
    irb_end_date::Union{Dates.Date, Nothing}

    Project(; project_id, project_name, project_title, irb_id = nothing, irb_start_date = nothing, irb_end_date = nothing) =
        new(project_id, project_name, project_title, irb_id, irb_start_date, irb_end_date)
end

const DEFAULT_PROJECT_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"

function load_project(filename = DEFAULT_PROJECT_FILE)
    json = JSON.parsefile(filename)
    project_id = json["project_id"]::String
    project_name = json["project_name"]::String
    project_title = json["project_title"]::String
    irb_id = get(json, "irb_id", nothing)::Union{String, Nothing}
    irb_start_date =
        let s = get(json, "irb_start_date", nothing)::Union{String, Nothing}
            s !== nothing ? Dates.Date(s) : nothing
        end
    irb_end_date =
        let s = get(json, "irb_end_date", nothing)::Union{String, Nothing}
            s !== nothing ? Dates.Date(s) : nothing
        end
    Project(; project_id, project_name, project_title, irb_id, irb_start_date, irb_end_date)
end

function FunSQL.Chain(prj::Project, attr::Symbol)
    val = getproperty(prj, attr)
    val !== nothing || return missing
    val
end

function funsql_get_project(attr = nothing)
    function custom_resolve(n, ctx)
        m = get_metadata(ctx.catalog)
        prj = m.project
        prj !== nothing || return missing
        attr !== nothing || return prj
        val = getproperty(prj, attr)
        val !== nothing || return missing
        val
    end
    CustomResolve(resolve_scalar = custom_resolve, terminal = true)
end

@funsql begin

get_project_id() =
    get_project(project_id)

get_project_name() =
    get_project(project_name)

get_project_title() =
    get_project(project_title)

get_irb_id() =
    get_project(irb_id)

get_irb_start_date() =
    get_project(irb_start_date)

get_irb_end_date() =
    get_project(irb_end_date)

is_discovery() =
    get_irb_id() == $DISCOVERY_IRB

is_during_irb_window() =
    between(datetime, get_irb_start_date(), get_irb_end_date())
end
