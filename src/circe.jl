module Circe

using CSV
using DataFrames
using Markdown
using Pkg.Artifacts
using StringEncodings

const java_initialized = Ref(false)

function initialize_java()
    if !java_initialized[]
        @eval using JavaCall
        Base.invokelatest() do
            JavaCall.addClassPath(joinpath(artifact"CirceR", "CirceR-1.3.2/inst/java/*"))
            JavaCall.addClassPath(joinpath(artifact"SqlRender", "SqlRender-1.16.1/inst/java/*"))
            JavaCall.init(["-Xmx128M"])
        end
        java_initialized[] = true
    end
end

function describe_cohort(str)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest describe_cohort(str)
    end
    MarkdownRender = JavaObject{Symbol("org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender")}
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderCohort",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function describe_concept_set_list(str)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest describe_concept_set_list(str)
    end
    MarkdownRender = JavaObject{Symbol("org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender")}
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderConceptSetList",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function describe_concept_set(str)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest describe_concept_set(str)
    end
    MarkdownRender = JavaObject{Symbol("org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender")}
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderConceptSet",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function build_cohort_query(str)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest build_cohort_query(str)
    end
    CohortExpressionQueryBuilder = JavaObject{Symbol("org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder")}
    builder = CohortExpressionQueryBuilder(())
    BuildExpressionQueryOptions =
        JavaObject{Symbol("org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder\$BuildExpressionQueryOptions")}
    jcall(
        builder,
        "buildExpressionQuery",
        JString,
        (JString, BuildExpressionQueryOptions),
        str,
        nothing)
end

function render_sql(template, params = (;))
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest render_sql(template, params)
    end
    SqlRender = JavaObject{Symbol("org.ohdsi.sql.SqlRender")}
    jcall(
        SqlRender,
        "renderSql",
        JString,
        (JString, Vector{JString}, Vector{JString}),
        template,
        collect(String, string.(keys(params))),
        collect(String, string.(values(params))))
end

function translate_sql(sql; dialect = "spark", session_id = nothing, temp_emulation_schema = nothing)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest translate_sql(sql, dialect = dialect, session_id = session_id, temp_emulation_schema = nothing)
    end
    SqlTranslate = JavaObject{Symbol("org.ohdsi.sql.SqlTranslate")}
    jcall(
        SqlTranslate,
        "translateSql",
        JString,
        (JString, JString, JString, JString),
        sql,
        dialect,
        session_id !== nothing ? string(session_id) : nothing,
        temp_emulation_schema !== nothing ? string(temp_emulation_schema) : nothing)
end

function split_sql(sql)
    if !@isdefined JavaCall
        initialize_java()
        return @invokelatest split_sql(sql)
    end
    SqlSplit = JavaObject{Symbol("org.ohdsi.sql.SqlSplit")}
    v = jcall(
        SqlSplit,
        "splitSql",
        Vector{JString},
        (JString,),
        sql)
    map(JavaCall.unsafe_string, v)
end

function cohort_to_sql(
    str;
    target_cohort_id,
    cdm_database_schema,
    session_id = nothing,
    vocabulary_database_schema = cdm_database_schema,
    results_database_schema = cdm_database_schema,
    target_database_schema = cdm_database_schema,
    temp_emulation_schema = cdm_database_schema,
    target_cohort_table = :cohort,
    generate_stats = false,
)
    tmpl = build_cohort_query(str)
    sql = render_sql(
        tmpl,
        (;
         target_cohort_id,
         cdm_database_schema,
         vocabulary_database_schema,
         results_database_schema,
         target_database_schema,
         target_cohort_table,
         generateStats = Int(generate_stats)))
    tr = translate_sql(sql, temp_emulation_schema = temp_emulation_schema, session_id = session_id)
    tr′ = replace(tr, "\r\n" => '\n')
    String[stmt for stmt in split_sql(tr′) if !startswith(stmt, "TRUNCATE TABLE ")]
end

function phenotype_library()
    csv = CSV.File(
        joinpath(artifact"PhenotypeLibrary", "PhenotypeLibrary-3.32.0/inst/Cohorts.csv"),
        select = [:cohortId, :cohortName, :logicDescription],
        types = Dict(:cohortId => Int, :cohortName => String, :logicDescription => String))
    ids = csv[:cohortId]
    names = csv[:cohortName]
    descriptions = csv[:logicDescription]
    definitions = String[decode(read(joinpath(artifact"PhenotypeLibrary", "PhenotypeLibrary-3.32.0/inst/cohorts/$id.json")), "latin1") for id in ids]
    DataFrame(id = ids, name = names, description = descriptions, definition = definitions)
end

function phenotype_library_v001()
    all_ids = Int[]
    all_names = String[]
    all_descriptions = Union{String, Missing}[]
    all_definitions = String[]
    for dir in readdir(joinpath(artifact"PhenotypeLibrary-v0.0.1", "PhenotypeLibrary-0.0.1/inst"), join = true)
        isdir(dir) || continue
        csv = CSV.File(
            joinpath(dir, "cohortDescription.csv"),
            select = [:cohortId, :webApiCohortId, :cohortName, :logicDescription],
            types = Dict(:cohortId => Int, :webApiCohortId => Int, :cohortName => String, :logicDescription => String))
        basenames = csv[:cohortId]
        ids = csv[:webApiCohortId]
        names = csv[:cohortName]
        descriptions = csv[:logicDescription]
        definitions = String[decode(read(joinpath(dir, "$basename.json")), "latin1") for basename in basenames]
        append!(all_ids, ids)
        append!(all_names, names)
        append!(all_descriptions, descriptions)
        append!(all_definitions, definitions)
    end
    df = DataFrame(id = all_ids, name = all_names, description = all_descriptions, definition = all_definitions)
    sort!(df)
end

end # module Circe
