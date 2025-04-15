const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"
const DISCOVERY_PROJECT = "P-005547"
const DISCOVERY_CASE = "01000526"
const DISCOVERY_START = "2010-01-01"
const DISCOVERY_SLUG = "005547_Harvey_TRDW_Guides"
const DISCOVERY_ID = "a0n8Y00000Z6YtIQAV"
const DISCOVERY_END = "2029-12-31"
const QUALITY_PROJECT = "P-005627"
const QUALITY_ID = "a0nan000001gvPVAAY"

function config_file()
    source = isfile(CONFIG_FILE) ? JSON.parsefile(CONFIG_FILE) : Dict()
    retval = OrderedDict{Symbol, Union{String, Vector, Nothing}}()
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
            :irb_code => "irb_code",
            :irb_start_date => "irb_start_date",
            :irb_end_date => "irb_end_date",
            :pi_name => "pi_display_name",
            :project_slug => "project_slug",
            :project_id => "project_id",
            :description => "description")
        retval[to] = get(source, from, nothing)
    end

    # provide defaults for cohort discoveries
    if retval[:irb_code] == DISCOVERY_IRB || retval[:project_code] == DISCOVERY_PROJECT
        retval[:irb_code] = something(retval[:irb_code], DISCOVERY_IRB)
        retval[:project_id] = something(retval[:project_id], DISCOVERY_ID)
        retval[:project_slug] = something(retval[:project_slug], DISCOVERY_SLUG)
        retval[:project_code] = something(retval[:project_code], DISCOVERY_PROJECT)
        retval[:irb_start_date] = something(retval[:irb_start_date], DISCOVERY_START)
        retval[:irb_end_date] = something(retval[:irb_end_date], DISCOVERY_END)
    end

    # give quality projects a broad date range
    if isnothing(retval[:irb_code]) || retval[:project_code] == QUALITY_PROJECT
        retval[:irb_code] = nothing
        retval[:project_id] = something(retval[:project_id], QUALITY_ID)
        retval[:project_code] = something(retval[:project_code], QUALITY_PROJECT)
        retval[:irb_start_date] = something(retval[:irb_start_date], DISCOVERY_START)
        retval[:irb_end_date] = something(retval[:irb_end_date], DISCOVERY_END)
    end

    # always have an IRB end date if there is a start date
    retval[:irb_end_date] =
        isnothing(retval[:irb_end_date]) ? retval[:irb_start_date] : retval[:irb_end_date]

    if !isnothing(retval[:project_code])
        # remove the "P-" prefix, if it was provided
        project_code = retval[:project_code]
        @assert startswith(project_code, "P-") && length(project_code) == 8
        retval[:project_code] = project_code[3:end]
    end

    return retval
end

is_discovery() = config_file()[:irb_code] == DISCOVERY_IRB
is_quality() = isnothing(config_file()[:irb_code])
funsql_config_file() = DataFrame(config_file())

# We are only permitted to consider data expressly permitted by the
# date window of the IRB approval.
function irb_date_range()
    config = config_file()
    return (config[:irb_start_date], config[:irb_end_date])
end

function funsql_irb_date_range()
    (irb_start_date, irb_end_date) = irb_date_range()
    @funsql select(:irb_start_date => $irb_start_date, :irb_end_date => $irb_end_date)
end

function funsql_is_during_irb_date_range(datetime=:datetime)
    (irb_start_date, irb_end_date) = irb_date_range()
    return @funsql begin
        :is_during_irb_date_range =>
            between($datetime, $irb_start_date, $irb_end_date)
    end
end
