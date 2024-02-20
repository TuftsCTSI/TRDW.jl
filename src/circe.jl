module Circe

using Markdown
using Pkg.Artifacts

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
    tr = translate_sql(sql, temp_emulation_schema = temp_emulation_schema)
    tr′ = replace(tr, "\r\n" => '\n')
    split_sql(tr′)
end

end # module Circe
